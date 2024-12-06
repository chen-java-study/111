#include <rados/librados.hpp>
#include <iostream>
#include <vector>
#include <string>

constexpr size_t OBJECT_SIZE = 4096;  // 每个对象的大小 (4KB)
const std::string POOL_NAME = "rbd";
const std::string OBJECT_NAME = "test_object";

int main() {
    try {
        // 初始化集群
        librados::Rados cluster;
        cluster.init2("admin", "ceph", 0);
        cluster.conf_read_file("/etc/ceph/ceph.conf");
        cluster.connect();

        // 打开池
        librados::IoCtx io_ctx;
        cluster.ioctx_create(POOL_NAME.c_str(), io_ctx);

        // 写对象
        std::vector<char> write_buffer(OBJECT_SIZE, 'A'); // 填充 'A'
        io_ctx.write(OBJECT_NAME, write_buffer.data(), write_buffer.size(), 0);

        std::cout << "Data written to object: " << OBJECT_NAME << std::endl;

        // 读对象
        std::vector<char> read_buffer(OBJECT_SIZE);
        io_ctx.read(OBJECT_NAME, read_buffer.size(), 0, read_buffer.data());
        std::cout << "Data read from object: " << std::string(read_buffer.begin(), read_buffer.end()) << std::endl;

        // 清理对象
        io_ctx.remove(OBJECT_NAME);
        std::cout << "Object removed: " << OBJECT_NAME << std::endl;

        // 关闭池和集群
        io_ctx.close();
        cluster.shutdown();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
