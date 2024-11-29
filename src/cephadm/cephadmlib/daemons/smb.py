import dataclasses
import enum
import json
import logging
import pathlib
import re
import socket

from typing import List, Dict, Tuple, Optional, Any, NamedTuple

from .. import context_getters
from .. import daemon_form
from .. import data_utils
from .. import deployment_utils
from .. import file_utils
from ..call_wrappers import call, CallVerbosity
from ceph.cephadm.images import DEFAULT_SAMBA_IMAGE
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_engines import Podman
from ..container_types import (
    CephContainer,
    InitContainer,
    Namespace,
    SidecarContainer,
    enable_shared_namespaces,
)
from ..context import CephadmContext
from ..daemon_identity import DaemonIdentity, DaemonSubIdentity
from ..deploy import DeploymentType
from ..exceptions import Error
from ..host_facts import list_networks
from ..net_utils import EndPoint


logger = logging.getLogger()

# sambacc provided commands we will need (when clustered)
_SCC = '/usr/bin/samba-container'
_NODES_SUBCMD = [_SCC, 'ctdb-list-nodes']
_MUTEX_SUBCMD = [_SCC, 'ctdb-rados-mutex']  # requires rados uri


class Features(enum.Enum):
    DOMAIN = 'domain'
    CLUSTERED = 'clustered'

    @classmethod
    def valid(cls, value: str) -> bool:
        # workaround for older python versions
        try:
            cls(value)
            return True
        except ValueError:
            return False


class ClusterPublicIP(NamedTuple):
    address: str
    destinations: List[str]

    @classmethod
    def convert(cls, item: Dict[str, Any]) -> 'ClusterPublicIP':
        assert isinstance(item, dict)
        address = item['address']
        assert isinstance(address, str)
        destinations = item['destinations']
        assert isinstance(destinations, list)
        return cls(address, destinations)


@dataclasses.dataclass(frozen=True)
class Config:
    identity: DaemonIdentity
    instance_id: str
    source_config: str
    domain_member: bool
    clustered: bool
    samba_debug_level: int = 0
    ctdb_log_level: str = ''
    debug_delay: int = 0
    join_sources: List[str] = dataclasses.field(default_factory=list)
    user_sources: List[str] = dataclasses.field(default_factory=list)
    custom_dns: List[str] = dataclasses.field(default_factory=list)
    smb_port: int = 0
    ceph_config_entity: str = 'client.admin'
    vhostname: str = ''
    metrics_image: str = ''
    metrics_port: int = 0
    # clustering related values
    rank: int = -1
    rank_generation: int = -1
    cluster_meta_uri: str = ''
    cluster_lock_uri: str = ''
    cluster_public_addrs: List[ClusterPublicIP] = dataclasses.field(
        default_factory=list
    )

    def config_uris(self) -> List[str]:
        uris = [self.source_config]
        uris.extend(self.user_sources or [])
        if self.clustered:
            # When clustered, we inject certain clustering related config vars
            # via a config file generated by cephadm (elsewhere in this file)
            uris.append('/etc/samba/container/ctdb.json')
        return uris


def _container_dns_args(cfg: Config) -> List[str]:
    cargs = []
    for dns in cfg.custom_dns:
        cargs.append(f'--dns={dns}')
    if cfg.vhostname:
        cargs.append(f'--hostname={cfg.vhostname}')
    return cargs


class ContainerCommon:
    def __init__(self, cfg: Config, image: str = '') -> None:
        self.cfg = cfg
        self.image = image

    def name(self) -> str:
        raise NotImplementedError('container name')

    def envs(self) -> Dict[str, str]:
        return {}

    def envs_list(self) -> List[str]:
        return []

    def args(self) -> List[str]:
        return []

    def container_args(self) -> List[str]:
        return []

    def container_image(self) -> str:
        return self.image


class SambaContainerCommon(ContainerCommon):
    def __init__(self, cfg: Config, image: str = '') -> None:
        self.cfg = cfg
        self.image = image

    def envs(self) -> Dict[str, str]:
        environ = {
            'SAMBA_CONTAINER_ID': self.cfg.instance_id,
            'SAMBACC_CONFIG': json.dumps(self.cfg.config_uris()),
        }
        # The CTDB support in sambacc project is considered experimental
        # and it refuses to run without setting the following environment
        # variable. This can be dropped once sambacc no longer needs it,
        # possibly after the next sambacc release.
        environ['SAMBACC_CTDB'] = 'ctdb-is-experimental'
        if self.cfg.ceph_config_entity:
            environ['SAMBACC_CEPH_ID'] = f'name={self.cfg.ceph_config_entity}'
        if self.cfg.rank >= 0:
            # how the values are known to ceph (for debugging purposes...)
            environ['RANK'] = str(self.cfg.rank)
            environ['RANK_GENERATION'] = str(self.cfg.rank)
            # samba container specific variant
            environ['NODE_NUMBER'] = environ['RANK']
        return environ

    def envs_list(self) -> List[str]:
        return [f'{k}={v}' for (k, v) in self.envs().items()]

    def args(self) -> List[str]:
        args = []
        if self.cfg.samba_debug_level:
            args.append(f'--samba-debug-level={self.cfg.samba_debug_level}')
        if self.cfg.debug_delay:
            args.append(f'--debug-delay={self.cfg.debug_delay}')
        return args


class SambaNetworkedInitContainer(SambaContainerCommon):
    """SambaContainerCommon subclass that enables additional networking
    params for an init container by default.
    NB: By networked we mean needs to use public network resources outside
    the ceph cluster.
    """

    def container_args(self) -> List[str]:
        cargs = _container_dns_args(self.cfg)
        if self.cfg.clustered:
            cargs.append('--network=host')
        return cargs


class SMBDContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'smbd'

    def args(self) -> List[str]:
        args = super().args()
        args.append('run')
        if self.cfg.clustered:
            auth_kind = 'nsswitch' if self.cfg.domain_member else 'users'
            args.append(f'--setup={auth_kind}')
            args.append('--setup=smb_ctdb')
            args.append('--wait-for=ctdb')
        args.append('smbd')
        return args

    def container_args(self) -> List[str]:
        cargs = []
        if self.cfg.smb_port:
            cargs.append(f'--publish={self.cfg.smb_port}:{self.cfg.smb_port}')
        if self.cfg.metrics_port:
            metrics_port = self.cfg.metrics_port
            cargs.append(f'--publish={metrics_port}:{metrics_port}')
        cargs.extend(_container_dns_args(self.cfg))
        return cargs


class WinbindContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'winbindd'

    def args(self) -> List[str]:
        args = super().args()
        args.append('run')
        if self.cfg.clustered:
            args.append('--setup=smb_ctdb')
            args.append('--wait-for=ctdb')
        args.append('winbindd')
        return args


class ConfigInitContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'config'

    def args(self) -> List[str]:
        return super().args() + ['init']


class MustJoinContainer(SambaNetworkedInitContainer):
    def name(self) -> str:
        return 'mustjoin'

    def args(self) -> List[str]:
        args = super().args()
        if self.cfg.clustered:
            # TODO: not only do we want to only do this on node 0, we only
            # want to do it exactly ONCE per cluster even on pnn 0. This needs
            # additional work to get that right.
            args.append('--skip-if=env:NODE_NUMBER!=0')
        args.append('must-join')
        for join_src in self.cfg.join_sources:
            args.append(f'-j{join_src}')
        return args


class ConfigWatchContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'configwatch'

    def args(self) -> List[str]:
        return super().args() + ['update-config', '--watch']


class SMBMetricsContainer(ContainerCommon):
    def name(self) -> str:
        return 'smbmetrics'

    def args(self) -> List[str]:
        args = []
        if self.cfg.metrics_port > 0:
            args.append(f'--port={self.cfg.metrics_port}')
        return args


class CTDBMigrateInitContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'ctdbMigrate'

    def args(self) -> List[str]:
        # TODO: not only do we want to only do this on node 0, we only
        # want to do it exactly ONCE per cluster even on pnn 0. This needs
        # additional work to get that right.
        return super().args() + [
            '--skip-if=env:NODE_NUMBER!=0',
            'ctdb-migrate',
            '--dest-dir=/var/lib/ctdb/persistent',
            '--archive=/var/lib/samba/.migrated',
        ]


class CTDBMustHaveNodeInitContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'ctdbMustHaveNode'

    def args(self) -> List[str]:
        args = super().args()
        unique_name = self.cfg.identity.daemon_name
        args += [
            'ctdb-must-have-node',
            # hostname is a misnomer (todo: fix in sambacc)
            f'--hostname={unique_name}',
            '--take-node-number-from-env',
        ]
        return args


class CTDBDaemonContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'ctdbd'

    def args(self) -> List[str]:
        return super().args() + [
            'run',
            'ctdbd',
            '--setup=smb_ctdb',
            '--setup=ctdb_config',
            '--setup=ctdb_etc',
        ]

    def container_args(self) -> List[str]:
        cargs = super().container_args()
        # make conditional?
        # CAP_NET_ADMIN is needed for event script to add public ips to iface
        cargs.append('--cap-add=NET_ADMIN')
        # CAP_NET_RAW allows to send gratuitous ARPs/tickle ACKs via raw sockets
        cargs.append('--cap-add=NET_RAW')
        return cargs


class CTDBNodeMonitorContainer(SambaContainerCommon):
    def name(self) -> str:
        return 'ctdbNodes'

    def args(self) -> List[str]:
        args = super().args()
        unique_name = self.cfg.identity.daemon_name
        args += [
            '--debug',
            'ctdb-monitor-nodes',
            # hostname is a misnomer (todo: fix in sambacc)
            f'--hostname={unique_name}',
            '--take-node-number-from-env',
            '--reload=all',
        ]
        return args


class ContainerLayout:
    init_containers: List[SambaContainerCommon]
    primary: SambaContainerCommon
    supplemental: List[ContainerCommon]

    def __init__(
        self,
        init_containers: List[SambaContainerCommon],
        primary: SambaContainerCommon,
        supplemental: List[ContainerCommon],
    ) -> None:
        self.init_containers = init_containers
        self.primary = primary
        self.supplemental = supplemental


@daemon_form.register
class SMB(ContainerDaemonForm):
    """Provides a form for SMB containers."""

    daemon_type = 'smb'
    daemon_base = '/usr/sbin/smbd'
    default_image = DEFAULT_SAMBA_IMAGE

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(self, ctx: CephadmContext, ident: DaemonIdentity):
        assert ident.daemon_type == self.daemon_type
        self._identity = ident
        self._instance_cfg: Optional[Config] = None
        self._files: Dict[str, str] = {}
        self._raw_configs: Dict[str, Any] = context_getters.fetch_configs(ctx)
        self._config_keyring = context_getters.get_config_and_keyring(ctx)
        self._cached_layout: Optional[ContainerLayout] = None
        self._rank_info = context_getters.fetch_rank_info(ctx) or (-1, -1)
        self.smb_port = 445
        self.metrics_port = 9922
        self._network_mapper = _NetworkMapper(ctx)
        logger.debug('Created SMB ContainerDaemonForm instance')

    @staticmethod
    def get_version(ctx: CephadmContext, container_id: str) -> Optional[str]:
        version = None
        out, _, ret = call(
            ctx,
            [
                ctx.container_engine.path,
                'exec',
                container_id,
                SMB.daemon_base,
                '-V',
            ],
            verbosity=CallVerbosity.QUIET,
        )

        if ret == 0:
            match = re.search(r'Version\s*([\d.]+)', out)
            if match:
                version = match.group(1)
        return version

    def validate(self) -> None:
        if self._instance_cfg is not None:
            return

        configs = self._raw_configs
        instance_id = configs.get('cluster_id', '')
        source_config = configs.get('config_uri', '')
        join_sources = configs.get('join_sources', [])
        user_sources = configs.get('user_sources', [])
        custom_dns = configs.get('custom_dns', [])
        instance_features = configs.get('features', [])
        files = data_utils.dict_get(configs, 'files', {})
        ceph_config_entity = configs.get('config_auth_entity', '')
        vhostname = configs.get('virtual_hostname', '')
        metrics_image = configs.get('metrics_image', '')
        metrics_port = int(configs.get('metrics_port', '0'))
        cluster_meta_uri = configs.get('cluster_meta_uri', '')
        cluster_lock_uri = configs.get('cluster_lock_uri', '')
        cluster_public_addrs = configs.get('cluster_public_addrs', [])

        if not instance_id:
            raise Error('invalid instance (cluster) id')
        if not source_config:
            raise Error('invalid configuration source uri')
        invalid_features = {
            f for f in instance_features if not Features.valid(f)
        }
        if invalid_features:
            raise Error(
                f'invalid instance features: {", ".join(invalid_features)}'
            )
        if not vhostname:
            # if a virtual hostname is not provided, generate one by prefixing
            # the cluster/instanced id to the system hostname
            hname = socket.getfqdn()
            vhostname = f'{instance_id}-{hname}'
        _public_addrs = [
            ClusterPublicIP.convert(v) for v in cluster_public_addrs
        ]
        if _public_addrs:
            # cache the cephadm networks->devices mapping for later
            self._network_mapper.load()

        rank, rank_gen = self._rank_info
        self._instance_cfg = Config(
            identity=self._identity,
            instance_id=instance_id,
            source_config=source_config,
            join_sources=join_sources,
            user_sources=user_sources,
            custom_dns=custom_dns,
            domain_member=Features.DOMAIN.value in instance_features,
            clustered=Features.CLUSTERED.value in instance_features,
            smb_port=self.smb_port,
            ceph_config_entity=ceph_config_entity,
            vhostname=vhostname,
            metrics_image=metrics_image,
            metrics_port=metrics_port,
            rank=rank,
            rank_generation=rank_gen,
            cluster_meta_uri=cluster_meta_uri,
            cluster_lock_uri=cluster_lock_uri,
            cluster_public_addrs=_public_addrs,
        )
        self._files = files
        logger.debug('SMB Instance Config: %s', self._instance_cfg)
        logger.debug('Configured files: %s', self._files)

    @property
    def _cfg(self) -> Config:
        self.validate()
        assert self._instance_cfg
        return self._instance_cfg

    @property
    def instance_id(self) -> str:
        return self._cfg.instance_id

    @property
    def source_config(self) -> str:
        return self._cfg.source_config

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'SMB':
        return cls(ctx, ident)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return 0, 0

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return self._config_keyring

    def _layout(self) -> ContainerLayout:
        if self._cached_layout:
            return self._cached_layout
        init_ctrs: List[SambaContainerCommon] = []
        ctrs: List[ContainerCommon] = []

        init_ctrs.append(ConfigInitContainer(self._cfg))
        ctrs.append(ConfigWatchContainer(self._cfg))

        if self._cfg.domain_member:
            init_ctrs.append(MustJoinContainer(self._cfg))
            ctrs.append(WinbindContainer(self._cfg))

        metrics_image = self._cfg.metrics_image.strip()
        metrics_port = self._cfg.metrics_port
        if metrics_image and metrics_port > 0:
            ctrs.append(SMBMetricsContainer(self._cfg, metrics_image))

        if self._cfg.clustered:
            init_ctrs += [
                CTDBMigrateInitContainer(self._cfg),
                CTDBMustHaveNodeInitContainer(self._cfg),
            ]
            ctrs += [
                CTDBDaemonContainer(self._cfg),
                CTDBNodeMonitorContainer(self._cfg),
            ]

        smbd = SMBDContainer(self._cfg)
        self._cached_layout = ContainerLayout(init_ctrs, smbd, ctrs)
        return self._cached_layout

    def _to_init_container(
        self, ctx: CephadmContext, smb_ctr: SambaContainerCommon
    ) -> InitContainer:
        volume_mounts: Dict[str, str] = {}
        container_args: List[str] = smb_ctr.container_args()
        self.customize_container_mounts(ctx, volume_mounts)
        # XXX: is this needed? if so, can this be simplified
        if isinstance(ctx.container_engine, Podman):
            ctx.container_engine.update_mounts(ctx, volume_mounts)
        identity = DaemonSubIdentity.from_parent(
            self.identity, smb_ctr.name()
        )
        return InitContainer(
            ctx,
            entrypoint='',
            image=ctx.image or self.default_image,
            identity=identity,
            args=smb_ctr.args(),
            container_args=container_args,
            envs=smb_ctr.envs_list(),
            volume_mounts=volume_mounts,
        )

    def _to_sidecar_container(
        self, ctx: CephadmContext, smb_ctr: ContainerCommon
    ) -> SidecarContainer:
        volume_mounts: Dict[str, str] = {}
        container_args: List[str] = smb_ctr.container_args()
        self.customize_container_mounts(ctx, volume_mounts)
        shared_ns = {
            Namespace.ipc,
            Namespace.network,
            Namespace.pid,
        }
        if isinstance(ctx.container_engine, Podman):
            # XXX: is this needed? if so, can this be simplified
            ctx.container_engine.update_mounts(ctx, volume_mounts)
            # docker doesn't support sharing the uts namespace with other
            # containers. It may not be entirely needed on podman but it gives
            # me warm fuzzies to make sure it gets shared.
            shared_ns.add(Namespace.uts)
        enable_shared_namespaces(
            container_args, self.identity.container_name, shared_ns
        )
        identity = DaemonSubIdentity.from_parent(
            self.identity, smb_ctr.name()
        )
        img = smb_ctr.container_image() or ctx.image or self.default_image
        return SidecarContainer(
            ctx,
            entrypoint='',
            image=img,
            identity=identity,
            container_args=container_args,
            args=smb_ctr.args(),
            envs=smb_ctr.envs_list(),
            volume_mounts=volume_mounts,
            init=False,
            remove=True,
        )

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self, host_network=self._cfg.clustered)
        # We want to share the IPC ns between the samba containers for one
        # instance.  Cephadm's default, host ipc, is not what we want.
        # Unsetting it works fine for podman but docker (on ubuntu 22.04) needs
        # to be expliclty told that ipc of the primary container must be
        # shareable.
        ctr.ipc = 'shareable'
        return deployment_utils.to_deployment_container(ctx, ctr)

    def init_containers(self, ctx: CephadmContext) -> List[InitContainer]:
        return [
            self._to_init_container(ctx, smb_ctr)
            for smb_ctr in self._layout().init_containers
        ]

    def sidecar_containers(
        self, ctx: CephadmContext
    ) -> List[SidecarContainer]:
        return [
            self._to_sidecar_container(ctx, smb_ctr)
            for smb_ctr in self._layout().supplemental
        ]

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        clayout = self._layout()
        envs.extend(clayout.primary.envs_list())

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        clayout = self._layout()
        args.extend(clayout.primary.args())

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self._layout().primary.container_args())

    def customize_container_mounts(
        self,
        ctx: CephadmContext,
        mounts: Dict[str, str],
    ) -> None:
        self.validate()
        data_dir = pathlib.Path(self.identity.data_dir(ctx.data_dir))
        etc_samba_ctr = str(data_dir / 'etc-samba-container')
        lib_samba = str(data_dir / 'lib-samba')
        run_samba = str(data_dir / 'run')
        config = str(data_dir / 'config')
        keyring = str(data_dir / 'keyring')
        mounts[etc_samba_ctr] = '/etc/samba/container:z'
        mounts[lib_samba] = '/var/lib/samba:z'
        mounts[run_samba] = '/run:z'  # TODO: make this a shared tmpfs
        mounts[config] = '/etc/ceph/ceph.conf:z'
        mounts[keyring] = '/etc/ceph/keyring:z'
        if self._cfg.clustered:
            ctdb_persistent = str(data_dir / 'ctdb/persistent')
            ctdb_run = str(data_dir / 'ctdb/run')  # TODO: tmpfs too!
            ctdb_volatile = str(data_dir / 'ctdb/volatile')
            ctdb_etc = str(data_dir / 'ctdb/etc')
            mounts[ctdb_persistent] = '/var/lib/ctdb/persistent:z'
            mounts[ctdb_run] = '/var/run/ctdb:z'
            mounts[ctdb_volatile] = '/var/lib/ctdb/volatile:z'
            mounts[ctdb_etc] = '/etc/ctdb:z'
            # create a shared smb.conf file for our clustered instances.
            # This is a HACK that substitutes for a bunch of architectural
            # changes to sambacc *and* smbmetrics (container). In short,
            # sambacc can set up the correct cluster enabled conf file for
            # samba daemons (smbd, winbindd, etc) but not it's own long running
            # tasks.  Similarly, the smbmetrics container always uses the
            # registry conf (non-clustered). Having cephadm create a stub
            # config that will share the file across all containers is a
            # stopgap that resolves the problem for now, but should eventually
            # be replaced by a less "leaky" approach in the managed containers.
            ctdb_smb_conf = str(data_dir / 'ctdb/smb.conf')
            mounts[ctdb_smb_conf] = '/etc/samba/smb.conf:z'

    def customize_container_endpoints(
        self, endpoints: List[EndPoint], deployment_type: DeploymentType
    ) -> None:
        if not any(ep.port == self.smb_port for ep in endpoints):
            endpoints.append(EndPoint('0.0.0.0', self.smb_port))
        if self.metrics_port > 0:
            if not any(ep.port == self.metrics_port for ep in endpoints):
                endpoints.append(EndPoint('0.0.0.0', self.metrics_port))

    def prepare_data_dir(self, data_dir: str, uid: int, gid: int) -> None:
        self.validate()
        ddir = pathlib.Path(data_dir)
        etc_samba_ctr = ddir / 'etc-samba-container'
        file_utils.makedirs(etc_samba_ctr, uid, gid, 0o770)
        file_utils.makedirs(ddir / 'lib-samba', uid, gid, 0o770)
        file_utils.makedirs(ddir / 'run', uid, gid, 0o770)
        if self._files:
            file_utils.populate_files(data_dir, self._files, uid, gid)
        if self._cfg.clustered:
            file_utils.makedirs(ddir / 'ctdb/persistent', uid, gid, 0o770)
            file_utils.makedirs(ddir / 'ctdb/run', uid, gid, 0o770)
            file_utils.makedirs(ddir / 'ctdb/volatile', uid, gid, 0o770)
            file_utils.makedirs(ddir / 'ctdb/etc', uid, gid, 0o770)
            self._write_ctdb_stub_config(etc_samba_ctr / 'ctdb.json')
            self._write_smb_conf_stub(ddir / 'ctdb/smb.conf')

    def _write_ctdb_stub_config(self, path: pathlib.Path) -> None:
        reclock_cmd = ' '.join(_MUTEX_SUBCMD + [self._cfg.cluster_lock_uri])
        nodes_cmd = ' '.join(_NODES_SUBCMD)
        stub_config: Dict[str, Any] = {
            'samba-container-config': 'v0',
            'ctdb': {
                # recovery_lock is passed directly to ctdb: needs '!' prefix
                'recovery_lock': f'!{reclock_cmd}',
                'cluster_meta_uri': self._cfg.cluster_meta_uri,
                'nodes_cmd': nodes_cmd,
                'public_addresses': self._network_mapper.for_sambacc(
                    self._cfg
                ),
            },
        }
        if self._cfg.ctdb_log_level:
            stub_config['ctdb']['log_level'] = self._cfg.ctdb_log_level
        with file_utils.write_new(path) as fh:
            json.dump(stub_config, fh)

    def _write_smb_conf_stub(self, path: pathlib.Path) -> None:
        """Initialize a stub smb conf that will be shared by the primary
        and sidecar containers. This is expected to be overwritten by
        sambacc.
        """
        _lines = [
            '[global]',
            'config backend = registry',
        ]
        with file_utils.write_new(path) as fh:
            for line in _lines:
                fh.write(f'{line}\n')


class _NetworkMapper:
    """Helper class that maps between cephadm-friendly address-networks
    groupings to ctdb-friendly address-device groupings.
    """

    def __init__(self, ctx: CephadmContext):
        self._ctx = ctx
        self._networks: Dict = {}

    def load(self) -> None:
        logger.debug('fetching networks')
        self._networks = list_networks(self._ctx)

    def _convert(self, addr: ClusterPublicIP) -> ClusterPublicIP:
        devs = []
        for net in addr.destinations:
            if net not in self._networks:
                # ignore mappings that cant exist on this host
                logger.warning(
                    'destination network %r not found in %r',
                    net,
                    self._networks.keys(),
                )
                continue
            for dev in self._networks[net]:
                logger.debug(
                    'adding device %s from network %r for public ip %s',
                    dev,
                    net,
                    addr.address,
                )
                devs.append(dev)
        return ClusterPublicIP(addr.address, devs)

    def for_sambacc(self, cfg: Config) -> List[Dict[str, Any]]:
        if not cfg.cluster_public_addrs:
            return []
        addrs = (self._convert(a) for a in (cfg.cluster_public_addrs or []))
        return [
            {'address': a.address, 'interfaces': a.destinations}
            for a in addrs
        ]