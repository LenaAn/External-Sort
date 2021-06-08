#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <iostream>
#include <stdexcept>
#include <string>
namespace ss = seastar;

constexpr size_t aligned_size = 4096;


std::string str(aligned_size, 'x');
ss::sstring fname = "/root/seastar-starter/simple_output.txt";
const char * to_write = str.c_str();
ss::temporary_buffer<char> buf = ss::temporary_buffer<char>::aligned(aligned_size, aligned_size);


ss::future<> write_to_file(ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [](ss::file f) mutable {
            std::cout << "opened file\n";
            return f.dma_write<char>(0, to_write, aligned_size).then([](size_t unused){
                std::cout << "I wrote\n" << std::flush;
            });
    });
}

ss::future<> read_from_file(const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [](ss::file& f) mutable {
            std::cout << "opened file to read\n";
            return f.dma_read<char>(0, buf.get_write(), aligned_size).then([](size_t unused){
                std::cout << "I read\n";
                for (int i = 0; i < aligned_size; ++i) {
                    std::cout << buf[i];
                }
                std::cout << "\n";
            });
        });
}

int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            return read_from_file(fname);
        });
    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}