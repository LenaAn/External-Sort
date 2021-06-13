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
uint64_t RAM_AVAILABLE = ss::memory::stats().free_memory();

size_t chunks_in_record = RAM_AVAILABLE / aligned_size;
size_t record_size = chunks_in_record*aligned_size;
constexpr bool debug = false;

ss::future<> write_chunk(size_t& record_pos, const int& i, std::string& chunk, const ss::sstring& fname) {
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
ss::future<> write_record(size_t& record_pos, std::vector<std::string>& chunks, const ss::sstring& tmp_file) {
    return ss::do_with(
        int(0),
        [&](auto& i){
            return ss::do_until(
                [&]{
                    return i == chunks.size();
                },
                [&]{
                    return write_chunk(record_pos, i, chunks[i], tmp_file).then([&]{
                        ++i;
                    });
                }
            );
        }
    );
}


// todo: move record?
std::vector<std::string> sort_chunks(size_t& count_read, ss::temporary_buffer<char>& record) {
    std::vector<std::string> chunks;
    for (int offset =0; offset + aligned_size <= count_read; offset+=aligned_size ){
        chunks.emplace_back(std::string(record.get() + offset, aligned_size));
    }
    std::sort(chunks.begin(), chunks.end());
    if (debug) {
        std::cout << "sorted chunks:\n";
        for (auto& chunk : chunks) {
            std::cout << chunk << "\n";
        }
    }
    return chunks;

}

ss::future<size_t> read_record(size_t& record_pos, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            return f.dma_read<char>(record_pos, buf.get_write(), record_size).then([](size_t count){
                return ss::make_ready_future<size_t>(count);
            });
        });
}

ss::future<> sort_chunks_inside_records(const ss::sstring& fname_input, const ss::sstring& tmp_file){
    std::cout << "I have " << RAM_AVAILABLE << " RAM\n";
    std::cout << "fname_input: " << fname_input << "\n";
    return ss::do_with(
        ss::temporary_buffer<char>::aligned(aligned_size, record_size),
        size_t(0),
        [&](auto& buf, auto& record_pos){
            return ss::repeat([&](){
                return read_record(record_pos, buf, fname_input).then([&](auto count_read_ext){
                    return ss::do_with(
                        size_t(count_read_ext),
                        [&](auto& count_read){
                            if (count_read == 0) {
                                return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                            } else {
                                return ss::do_with(
                                    sort_chunks(count_read, buf),
                                    [&](auto& chunks){
                                        return write_record(record_pos, chunks, tmp_file).then([&]{
                                            record_pos += count_read;
                                        });
                                    }
                                ).then([]{
                                    return ss::stop_iteration::no;
                                });
                            }
                        }
                    );
                });
            });
        }
    );
}

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    namespace bpo = boost::program_options;

    seastar::app_template app;
    app.add_positional_options({
       { "filename", bpo::value<std::vector<seastar::sstring>>()->default_value({}),
         "sstable files to verify", -1}
    });

    try {
        app.run(argc, argv, [&app] {
            auto& args = app.configuration();
            auto& filenames = args["filename"].as<std::vector<seastar::sstring>>();
            if (filenames.size() < 1){
                throw std::runtime_error("Input file name not provided.");
            }


            return ss::do_with(
                ss::sstring(filenames[0]),
                // todo: generate tmp file name
                ss::sstring("/mnt/volume_ams3_04/simple_output_small_record.txt"),
                [](auto& fname_input, auto& tmp_file){
                    return sort_chunks_inside_records(fname_input, tmp_file);
                });
        });
    } catch(...) {
        std::cerr << "Failed to start application: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}