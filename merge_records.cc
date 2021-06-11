#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/iostream.hh>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
namespace ss = seastar;

constexpr size_t aligned_size = 4096;
uint64_t RAM_AVAILABLE = 32284672;
size_t chunks_in_record = RAM_AVAILABLE / aligned_size;
size_t record_size = chunks_in_record*aligned_size;

ss::sstring fname_records = "/root/seastar-starter/output.txt";


ss::future<size_t> read_chunk(size_t& record_pos, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            std::cout << "opened file, gonna read on pos: " << record_size*record_pos << "\n";
            return f.dma_read<char>(record_size*record_pos, buf.get_write(), aligned_size).then([&](size_t count){
                std::cout << "I've read " << count << "\n";
                return ss::make_ready_future<size_t>(count);
            });
        });
}



int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            std::cout << "Hi there! I will try to merge records\n";

            auto buffers = std::vector<ss::temporary_buffer<char>>();
            for (int i = 0; i < 3; ++i){
                buffers.emplace_back(ss::temporary_buffer<char>::aligned(aligned_size, aligned_size));
            }

            return ss::do_with(
                std::move(buffers),
                size_t(0),
                [](auto& buffers, auto& i){
                    return ss::repeat([&](){
                        if (i == 3) {
                            return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                        }
                        return read_chunk(i, buffers[i], fname_records).then([&](size_t count_read){
                            std::cout << "my chunk: " << buffers[i].get() << "\n"; // prints more than one chunk, but that's ok
                            ++i;
                            return ss::stop_iteration::no;
                        });
                    });
                }
            );
        });

    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}