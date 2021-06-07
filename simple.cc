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


ss::future<> write_to_file(ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [](ss::file f) mutable {
            std::cout << "opened file\n";
            return f.dma_write<char>(0, to_write, aligned_size).then([](size_t unused){
                std::cout << "I wrote\n" << std::flush;
            });
    });
}


int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            return write_to_file(fname);
        });
    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}