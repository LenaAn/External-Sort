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
// todo: recalculate based on RAM available
constexpr size_t chunks_in_record = 4;
constexpr size_t record_size = chunks_in_record*aligned_size;
constexpr bool debug = false;


ss::sstring fname_output = "/root/seastar-starter/simple_output.txt";
ss::sstring fname_input = "/root/seastar-starter/simple_input.txt";

ss::future<> write_chunk(int& record_pos, const int& i, std::string& chunk, ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&](ss::file f) mutable {
            return f.dma_write<char>(record_pos*record_size + i*aligned_size, chunk.c_str(), aligned_size).then([&](size_t unused){
                std::cout << "I wrote\n" << std::flush;
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
                    return i == chunks_in_record;
                },
                [&]{
                    return write_chunk(record_pos, i, chunks[i], fname_output).then([&]{
                        ++i;
                    });
                }
            );
        }
    );
}


// todo: move record?
std::vector<std::string> sort_chunks(ss::temporary_buffer<char>& record) {
    std::vector<std::string> chunks;
    for (int offset =0; offset + aligned_size <= record.size(); offset+=aligned_size ){
        chunks.emplace_back(std::string(record.get() + offset, aligned_size));
    }
    // todo: make sure it's us-ascii order
    std::sort(chunks.begin(), chunks.end());
    std::cout << "sorted chunks:\n";
    for (auto& chunk : chunks) {
        std::cout << chunk << "\n";
    }
    return chunks;

}

ss::future<> read_record(int& i_rec, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            std::cout << "opened file to read\n";
            return f.dma_read<char>(i_rec*record_size, buf.get_write(), record_size).discard_result();
        });
}

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            return ss::do_with(
                ss::temporary_buffer<char>::aligned(aligned_size, record_size),
                std::vector<int>{0, 1},
                [](auto& buf, auto & range){
                    return ss::do_for_each(
                        range,
                        [&buf](int& record_pos) {
                            std::cout << "record_pos: " << record_pos << "\n";
                            return read_record(record_pos, buf, fname_input).then([&] {
                                return ss::do_with(
                                    sort_chunks(buf),
                                    [&](auto& chunks){
                                        // todo: account for record pos
                                        return write_record(record_pos, chunks, fname_output);
                                    }
                                );
                            });
                        }
                    );
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