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
uint64_t RAM_AVAILABLE = ss::memory::stats().total_memory();
ss::sstring fname_records = "/root/seastar-starter/sorted_output.txt";


ss::future<size_t> read_chunk(size_t& record_pos, ss::temporary_buffer<char>& buf){
    return with_file(ss::open_file_dma(fname_records, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            std::cout << "opened file, gonna read on pos: " << record_pos << "\n";
            return f.dma_read<char>(record_pos, buf.get_write(), aligned_size).then([&](size_t count){
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
            return ss::do_with(
                ss::temporary_buffer<char>::aligned(aligned_size, aligned_size),
                std::string(),
                std::string(),
                size_t(0),
                [](auto& buf, auto& curr_string, auto& next_string, auto& pos){
                    return read_chunk(pos, buf).then([&](size_t read_count){
                        pos += read_count;
                        next_string = std::string(buf.get(), aligned_size);
                        return ss::repeat(
                            [&](){
                                return read_chunk(pos, buf).then([&](size_t read_count){
                                    if (read_count == 0){
                                        return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                                    } else {
                                        pos += read_count;
                                        curr_string = next_string;
                                        next_string = std::string(buf.get(), aligned_size);
                                        if (curr_string.compare(next_string) > 0){
                                            std::cout << "curr_string: " << curr_string << "\n";
                                            std::cout << "next_string: " << next_string << "\n";
                                        }
                                        assert(curr_string.compare(next_string) < 0);
                                        return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
                                    }
                                });
                            }
                        );
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