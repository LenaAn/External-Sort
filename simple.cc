#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <iostream>
#include <stdexcept>
#include <string>
namespace ss = seastar;

constexpr size_t aligned_size = 4096;


ss::sstring fname_output = "/root/seastar-starter/simple_output.txt";
ss::sstring fname_input = "/root/seastar-starter/simple_input.txt";

ss::future<> write_to_file(ss::temporary_buffer<char>& buf, ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&buf](ss::file f) mutable {
            std::cout << "opened file\n";
            return f.dma_write<char>(0, buf.get(), aligned_size).then([](size_t unused){
                std::cout << "I wrote\n" << std::flush;
            });
    });
}

ss::future<> read_from_file(ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&buf](ss::file& f) mutable {
            std::cout << "opened file to read\n";
            return f.dma_read<char>(0, buf.get_write(), aligned_size).then([&buf](size_t unused){
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
            return ss::do_with(ss::temporary_buffer<char>::aligned(aligned_size, aligned_size),
                [](auto& buf) {
                    return read_from_file(buf, fname_input).then([&buf]{
                        return write_to_file(buf, fname_output);
                });
            });
        });
    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}