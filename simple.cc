#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/internal/api-level.hh>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
namespace ss = seastar;

constexpr size_t aligned_size = 4096;
// todo: I believe I can actually use less without overhelming system
uint64_t RAM_AVAILABLE = ss::memory::stats().free_memory();

size_t chunks_in_record = RAM_AVAILABLE / aligned_size;
size_t record_size = chunks_in_record*aligned_size;
constexpr bool debug = false;


ss::sstring fname_output = "/root/seastar-starter/simple_output.txt";
ss::sstring fname_input = "/root/seastar-starter/simple_input.txt";

ss::future<> write_chunk(int& record_pos, const int& i, std::string& chunk, ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&](ss::file f) mutable {
            return f.dma_write<char>(record_pos + i*aligned_size, chunk.c_str(), aligned_size).then([&](size_t unused){
                if (debug){
                    std::cout << "I wrote\n" << std::flush;
                }
            });
    });
}

// todo: move vector of strings??
ss::future<> write_record(int& record_pos, std::vector<std::string>& chunks, ss::sstring fname) {
    return ss::do_with(
        int(0),
        [&](auto& i){
            return ss::do_until(
                [&]{
                    // todo: store chunks.size() in a variable;
                    return i == chunks.size();
                },
                [&]{
                    return write_chunk(record_pos, i, chunks[i], fname_output).then([&]{
                        ++i;
                    });
                }
            ).then([]{
                std::cout << "I wrote a record\n";
            });
        }
    );
}


// todo: move record?
std::vector<std::string> sort_chunks(size_t& count_read, ss::temporary_buffer<char>& record) {
    std::vector<std::string> chunks;
    for (int offset =0; offset + aligned_size <= count_read; offset+=aligned_size ){
        chunks.emplace_back(std::string(record.get() + offset, aligned_size));
    }
    // todo: make sure it's us-ascii order
    std::sort(chunks.begin(), chunks.end());
    std::cout << "I have " << chunks.size() << " chunks\n";
    if (debug) {
        std::cout << "sorted chunks:\n";
        for (auto& chunk : chunks) {
            std::cout << chunk << "\n";
        }
    }
    return chunks;

}

ss::future<size_t> read_record(int& record_pos, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            std::cout << "opened file to read\n";
            return f.dma_read<char>(record_pos, buf.get_write(), record_size).then([](size_t count){
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
            std::cout << "I have " << RAM_AVAILABLE << " RAM\n";
            return ss::do_with(
                ss::temporary_buffer<char>::aligned(aligned_size, record_size),
                int(0),
                [](auto& buf, auto& record_pos){
                    return ss::repeat([&](){
                        return read_record(record_pos, buf, fname_input).then([&](auto count_read){
                            if (count_read == 0) {
                                return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                            } else {
                                return ss::do_with(
                                    sort_chunks(count_read, buf),
                                    [&](auto& chunks){
                                        return write_record(record_pos, chunks, fname_output).then([&]{
                                            record_pos += count_read;
                                        });
                                    }
                                ).then([]{
                                    return ss::stop_iteration::no;
                                });
                            }
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