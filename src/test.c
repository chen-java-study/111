#include <rados/librados.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define POOL_NAME "testpool"
#define OBJECT_NAME "testobject"
#define TEST_CONTENT "Hello, Ceph!"

int main() {
    rados_t cluster;
    rados_ioctx_t io_ctx;
    int ret;

    // 1. 初始化 RADOS 集群句柄
    ret = rados_create(&cluster, NULL);
    if (ret < 0) {
        fprintf(stderr, "无法创建 RADOS 句柄: %s\n", strerror(-ret));
        return EXIT_FAILURE;
    }

    printf("成功创建 RADOS 集群句柄。\n");

    // 2. 读取配置文件并初始化集群
    ret = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
    if (ret < 0) {
        fprintf(stderr, "无法加载配置文件: %s\n", strerror(-ret));
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功加载配置文件。\n");

    // 3. 连接到集群
    ret = rados_connect(cluster);
    if (ret < 0) {
        fprintf(stderr, "无法连接到集群: %s\n", strerror(-ret));
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功连接到集群。\n");

    // 4. 检查并创建存储池
    ret = rados_pool_lookup(cluster, POOL_NAME);
    if (ret < 0) {
        printf("存储池 '%s' 不存在，尝试创建...\n", POOL_NAME);
        ret = rados_pool_create(cluster, POOL_NAME);
        if (ret < 0) {
            fprintf(stderr, "无法创建存储池: %s\n", strerror(-ret));
            rados_shutdown(cluster);
            return EXIT_FAILURE;
        }
        printf("成功创建存储池 '%s'。\n", POOL_NAME);
    } else {
        printf("存储池 '%s' 已存在。\n", POOL_NAME);
    }

    // 5. 打开存储池
    ret = rados_ioctx_create(cluster, POOL_NAME, &io_ctx);
    if (ret < 0) {
        fprintf(stderr, "无法打开存储池: %s\n", strerror(-ret));
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功打开存储池 '%s'。\n", POOL_NAME);

    // 6. 写入对象
    const char *content = TEST_CONTENT;
    ret = rados_write(io_ctx, OBJECT_NAME, content, strlen(content), 0);
    if (ret < 0) {
        fprintf(stderr, "无法写入对象: %s\n", strerror(-ret));
        rados_ioctx_destroy(io_ctx);
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功写入对象 '%s'，内容: '%s'。\n", OBJECT_NAME, content);

    // 7. 读取对象
    char read_buf[128];
    memset(read_buf, 0, sizeof(read_buf));
    ret = rados_read(io_ctx, OBJECT_NAME, read_buf, sizeof(read_buf), 0);
    if (ret < 0) {
        fprintf(stderr, "无法读取对象: %s\n", strerror(-ret));
        rados_ioctx_destroy(io_ctx);
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功读取对象 '%s'，内容: '%s'。\n", OBJECT_NAME, read_buf);

    // 8. 删除对象
    ret = rados_remove(io_ctx, OBJECT_NAME);
    if (ret < 0) {
        fprintf(stderr, "无法删除对象: %s\n", strerror(-ret));
        rados_ioctx_destroy(io_ctx);
        rados_shutdown(cluster);
        return EXIT_FAILURE;
    }

    printf("成功删除对象 '%s'。\n", OBJECT_NAME);

    // 9. 销毁 I/O 上下文和集群句柄
    rados_ioctx_destroy(io_ctx);
    rados_shutdown(cluster);

    printf("测试完成，所有资源已释放。\n");
    return EXIT_SUCCESS;
}
