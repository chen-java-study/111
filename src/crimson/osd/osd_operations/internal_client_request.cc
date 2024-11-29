// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <seastar/core/future.hh>

#include "crimson/osd/osd_operations/internal_client_request.h"
#include "osd/object_state_fmt.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::InternalClientRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

SET_SUBSYS(osd);

namespace crimson::osd {

InternalClientRequest::InternalClientRequest(Ref<PG> pg)
  : pg(pg), start_epoch(pg->get_osdmap_epoch())
{
  assert(bool(this->pg));
  assert(this->pg->is_primary());
}

InternalClientRequest::~InternalClientRequest()
{
  LOG_PREFIX(InternalClientRequest::~InternalClientRequest);
  DEBUGI("{}: destroying", *this);
}

void InternalClientRequest::print(std::ostream &) const
{
}

void InternalClientRequest::dump_detail(Formatter *f) const
{
}

CommonPGPipeline& InternalClientRequest::client_pp()
{
  return pg->request_pg_pipeline;
}

InternalClientRequest::interruptible_future<>
InternalClientRequest::do_process(
  crimson::osd::ObjectContextRef obc,
  std::vector<OSDOp> &osd_ops)
{
  LOG_PREFIX(InternalClientRequest::do_process);
  auto params = get_do_osd_ops_params();
  OpsExecuter ox(
    pg, obc, op_info, params, params.get_connection(), SnapContext{});
  co_await pg->run_executer(
    ox, obc, op_info, osd_ops
  ).handle_error_interruptible(
    crimson::ct_error::all_same_way(
      [this, FNAME](auto e) {
	ERRORDPPI("{}: got unexpected error {}", *pg, *this, e);
	ceph_assert(0 == "should not return an error");
	return interruptor::now();
      })
  );

  auto [submitted, completed] = co_await pg->submit_executer(
    std::move(ox), osd_ops);

  co_await std::move(submitted);
  co_await std::move(completed);
}

InternalClientRequest::interruptible_future<>
InternalClientRequest::with_interruption()
{
  LOG_PREFIX(InternalClientRequest::with_interruption);
  assert(pg->is_active());

  co_await enter_stage<interruptor>(client_pp().recover_missing);

  bool unfound = co_await do_recover_missing(
    pg, get_target_oid(), osd_reqid_t());

  if (unfound) {
    throw std::system_error(
      std::make_error_code(std::errc::operation_canceled),
      fmt::format("{} is unfound, drop it!", get_target_oid()));
  }
  co_await enter_stage<interruptor>(
    client_pp().check_already_complete_get_obc);

  DEBUGI("{}: getting obc lock", *this);

  auto osd_ops = create_osd_ops();

  DEBUGI("InternalClientRequest: got {} OSDOps to execute",
	 std::size(osd_ops));
  [[maybe_unused]] const int ret = op_info.set_from_op(
    std::as_const(osd_ops), pg->get_pgid().pgid, *pg->get_osdmap());
  assert(ret == 0);

  auto obc_manager = pg->obc_loader.get_obc_manager(get_target_oid());

  // initiate load_and_lock in order, but wait concurrently
  enter_stage_sync(client_pp().lock_obc);

  co_await pg->obc_loader.load_and_lock(
    obc_manager, pg->get_lock_type(op_info)
  ).handle_error_interruptible(
    crimson::ct_error::assert_all("unexpected error")
  );

  DEBUGDPP("{}: got obc {}, entering process stage",
	   *pg, *this, obc_manager.get_obc()->obs);
  co_await enter_stage<interruptor>(client_pp().process);

  DEBUGDPP("{}: in process stage, calling do_process",
	   *pg, *this);
  co_await do_process(obc_manager.get_obc(), osd_ops);

  logger().debug("{}: complete", *this);
  co_await interruptor::make_interruptible(handle.complete());
  co_return;
}

seastar::future<> InternalClientRequest::start()
{
  track_event<StartEvent>();
  LOG_PREFIX(InternalClientRequest::start);
  DEBUGI("{}: in repeat", *this);

  return interruptor::with_interruption([this]() mutable {
    return with_interruption();
  }, [](std::exception_ptr eptr) {
    return seastar::now();
  }, pg, start_epoch).then([this] {
    track_event<CompletionEvent>();
  }).handle_exception_type([](std::system_error &error) {
    logger().debug("error {}, message: {}", error.code(), error.what());
    return seastar::now();
  }).finally([this] {
    logger().debug("{}: exit", *this);
    handle.exit();
  });
}

} // namespace crimson::osd
