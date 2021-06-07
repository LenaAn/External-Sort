#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <iostream>
#include <stdexcept>
#include <string>
namespace ss = seastar;

std::string str(4096, 'x');
const char * to_write = str.c_str();

ss::future<> write_to_file(ss::file f) {
	return f.dma_write<char>(0, to_write, 4096).then([](size_t unused){
			
    std::cout << "I wrote\n" << std::flush;
    });
}


ss::future<> open_file_and_write() {
    ss::sstring fname = "/root/seastar-starter/simple_output.txt";
    return ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create).then([](ss::file f) mutable {
		    std::cout << "opened file\n"; 
		    return write_to_file(f);
    });
}


int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, open_file_and_write);
    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}
