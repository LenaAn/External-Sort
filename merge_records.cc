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
ss::sstring fname_sorted = "/root/seastar-starter/sorted_output.txt";

constexpr bool debug = false;

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

ss::future<> write_chunk(int& iter_count, std::string& chunk, ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&](ss::file f) mutable {
            std::cout << "Opened file. Gonna write the chunk:\n";
            std::cout << chunk << "\n";
            return f.dma_write<char>(iter_count*aligned_size, chunk.c_str(), aligned_size).then([&](size_t unused){
                std::cout << "I wrote\n" << std::flush;
            });
    });
}

std::vector<std::string> convert_to_string(std::vector<ss::temporary_buffer<char>>& buffers){
    std::vector<std::string> chunks;
    for (int i = 0; i < 3; ++i) {
        chunks.emplace_back(std::string(buffers[i].get(), aligned_size));
    }

    if (debug){
        std::cout << "I have chunks:\n";
        for (auto& chunk : chunks) {
            std::cout << chunk << "\n";
        }
        std::cout << "\n";
    }
    return chunks;
}

ss::future<> upload_new_value(std::vector<std::string>& chunks, size_t& i_record_to_update){
    std::cout << "i_record_to_update: " << i_record_to_update << "\n";
    return ss::make_ready_future();
}

ss::future<> write_min(size_t& i_record_to_update, int& iter_count, std::string& min_chunk, std::vector<std::string>& chunks, const ss::sstring& output_fname){
    i_record_to_update = 0;
    min_chunk = chunks[i_record_to_update];

    for (size_t i = 0; i < chunks.size(); ++i){
        if (min_chunk.compare(chunks[i]) > 0){
            i_record_to_update = i;
            min_chunk = chunks[i];
        }
    }
    // std::cout << "min_chunk: " << min_chunk << "\n";
    return write_chunk(iter_count, min_chunk, output_fname);
}

ss::future<> upload_first_chunks_of_records(std::vector<ss::temporary_buffer<char>>& buffers) {
    return ss::do_with(
        size_t(0),
        [&](auto& i){
            return ss::repeat([&](){
                if (i == 3) {
                    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                }
                return read_chunk(i, buffers[i], fname_records).then([&](size_t count_read){
                    ++i;
                    return ss::stop_iteration::no;
                });
            });
        }
    );
}

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            auto buffers = std::vector<ss::temporary_buffer<char>>();
            for (int i = 0; i < 3; ++i){
                buffers.emplace_back(ss::temporary_buffer<char>::aligned(aligned_size, aligned_size));
            }

            return ss::do_with(
                std::move(buffers),
                [](auto& buffers){
                    return upload_first_chunks_of_records(buffers).then([&](){
                        return ss::do_with(
                            size_t(0),
                            int(0),
                            std::string(),
                            convert_to_string(buffers),
                            [&](auto& i_record_to_update, auto& iter_count, auto& min_chunk, auto& chunks){
                                return ss::repeat([&](){
                                    if (iter_count == 3) {
                                        return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                                    } else {
                                        std::cout << "gonna merge 'em all!\n";
                                        return write_min(i_record_to_update, iter_count, min_chunk, chunks, fname_sorted).then([&]{
                                            return upload_new_value(chunks, i_record_to_update).then([&]{
                                                ++iter_count;
                                                return ss::stop_iteration::no;
                                            });
                                        });
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