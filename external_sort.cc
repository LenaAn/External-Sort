#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/tmp_file.hh>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <queue>
namespace ss = seastar;
namespace fs = std::filesystem;

constexpr size_t aligned_size = 4096;
uint64_t RAM_AVAILABLE = ss::memory::stats().free_memory();

size_t chunks_in_record = RAM_AVAILABLE / aligned_size;
size_t record_size = chunks_in_record*aligned_size;
size_t number_of_records_to_merge = chunks_in_record;

struct enumeratedChunk {
    size_t number;
    std::string chunk;
};

inline bool operator> (const enumeratedChunk& lhs, const enumeratedChunk& rhs){ return lhs.chunk > rhs.chunk; }

ss::future<> write_chunk(const size_t& record_pos, const int& i, const std::string& chunk, const ss::sstring& fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&](ss::file f) mutable {
            return f.dma_write<char>(record_pos + i*aligned_size, chunk.c_str(), aligned_size).discard_result();
    });
}

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


std::vector<std::string> sort_chunks(size_t& count_read, ss::temporary_buffer<char>& record) {
    std::vector<std::string> chunks;
    for (int offset =0; offset + aligned_size <= count_read; offset+=aligned_size ){
        chunks.emplace_back(std::string(record.get() + offset, aligned_size));
    }
    std::sort(chunks.begin(), chunks.end());
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

// ==============Watershed==============

ss::future<size_t> read_chunk(size_t& offset, size_t& record_pos, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&](ss::file& f) mutable {
            return f.dma_read<char>(offset + record_size*record_pos, buf.get_write(), aligned_size).then([&](size_t count){
                return ss::make_ready_future<size_t>(count);
            });
        });
}

std::vector<std::string> convert_to_string(std::vector<ss::temporary_buffer<char>>& buffers){
    std::vector<std::string> chunks;
    for (int i = 0; i < buffers.size(); ++i) {
        chunks.emplace_back(std::string(buffers[i].get(), aligned_size));
    }
    return chunks;
}

ss::future<> upload_first_chunks_of_records(bool& chunks_are_full, size_t& offset, std::vector<ss::temporary_buffer<char>>& buffers, std::vector<bool>& pos_is_valid, const ss::sstring& tmp_file) {
    return ss::do_with(
        size_t(0),
        [&](auto& i){
            return ss::repeat([&](){
                if (i == number_of_records_to_merge) {
                    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                }
                return read_chunk(offset, i, buffers[i], tmp_file).then([&](size_t count_read){
                    if (count_read == 0) {
                        pos_is_valid[i] = false;
                        chunks_are_full = false;
                    }
                    ++i;
                    return ss::stop_iteration::no;
                });
            });
        }
    );
}

ss::future<bool> write_min(size_t& offset, std::vector<bool>& pos_is_valid, size_t& i_record_to_update, int& iter_count, std::string& min_chunk, std::vector<std::string>& chunks, auto& queue, const ss::sstring& tmp_output){
    if (queue.empty()){
        return ss::make_ready_future<bool>(false);
    } else {
        min_chunk = queue.top().chunk;
        i_record_to_update = queue.top().number;
        queue.pop();
        return write_chunk(offset, iter_count, min_chunk, tmp_output).then([]{
            return ss::make_ready_future<bool>(true);
        });
    }
}

ss::future<> upload_new_value(size_t& offset, std::vector<bool>& pos_is_valid, std::vector<size_t>& positions, std::vector<std::string>& chunks, auto& queue, size_t& i_record_to_update, const ss::sstring& tmp_file){
    if (!pos_is_valid[i_record_to_update]){
        return ss::make_ready_future();
    }
    return ss::do_with(
        ss::temporary_buffer<char>::aligned(aligned_size, aligned_size),
        [&](auto& buf){
            return with_file(ss::open_file_dma(tmp_file, ss::open_flags::ro),
                [&](ss::file& f) mutable {
                    return f.dma_read<char>(offset + i_record_to_update * record_size + positions[i_record_to_update] * aligned_size, buf.get_write(), aligned_size).then([&](size_t count){
                        if (count == 0){
                            pos_is_valid[i_record_to_update] = false;
                        } else {
                            chunks[i_record_to_update] = std::string(buf.get(), aligned_size);
                            queue.push({i_record_to_update, chunks[i_record_to_update]});
                        }
                    });
                }
            );
        }
    );
}

ss::future<> sort_records(size_t& offset, std::vector<std::string>& chunks, std::vector<bool>& pos_is_valid, const ss::sstring& tmp_file, const ss::sstring& tmp_output){
    std::vector<size_t> positions(number_of_records_to_merge, 0);
    std::priority_queue<enumeratedChunk, std::vector<enumeratedChunk>, std::greater<enumeratedChunk>> queue;
    for (size_t i = 0; i < chunks.size(); ++i){
        if (pos_is_valid[i]){
            queue.push({i, chunks[i]});
        }
    }

    return ss::do_with(
        std::move(positions),
        std::move(queue),
        size_t(0),
        int(0),
        std::string(),
        [&](auto& positions, auto& queue, auto& i_record_to_update, auto& iter_count, auto& min_chunk){
            return ss::repeat([&](){
                return write_min(offset, pos_is_valid, i_record_to_update, iter_count, min_chunk, chunks, queue, tmp_output).then([&](bool can_continue){
                    if (!can_continue){
                        std::cout << "all positions are invalid, returning. iter_count: " << iter_count << "\n";
                        return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                    } else {
                        ++positions[i_record_to_update];
                        // the last record may be shorter, but upload_new_value will handle pos_is_valid for that
                        if (positions[i_record_to_update] >= chunks_in_record) {
                            pos_is_valid[i_record_to_update] = false;
                        }
                        if (iter_count % 1000 == 0){
                            std::cout << "positions: " << positions[0] << ", " << positions[1] << ", " << positions[2] << "\n";
                        }
                        return upload_new_value(offset, pos_is_valid, positions, chunks, queue, i_record_to_update, tmp_file).then([&]{
                            ++iter_count;
                            return ss::stop_iteration::no;
                        });
                    }
                });
            });
        }
    );
}

ss::future<bool> merge_k_records(size_t& offset, const ss::sstring& tmp_file, const ss::sstring& tmp_output){
    auto buffers = std::vector<ss::temporary_buffer<char>>();
    for (int i = 0; i < number_of_records_to_merge; ++i){
        buffers.emplace_back(ss::temporary_buffer<char>::aligned(aligned_size, aligned_size));
    }
    std::vector<bool> pos_is_valid(number_of_records_to_merge, true);

    return ss::do_with(
        std::move(buffers),
        std::move(pos_is_valid),
        bool(true),
        [&](auto& buffers, auto& pos_is_valid, auto& chunks_are_full){
            return upload_first_chunks_of_records(chunks_are_full, offset, buffers, pos_is_valid, tmp_file).then([&](){
                return ss::do_with(
                    convert_to_string(buffers),
                    [&](auto& chunks){
                        return sort_records(offset, chunks, pos_is_valid, tmp_file, tmp_output).then([&]{
                            return chunks_are_full;
                        });
                    }
                );
            });
        }
    );
}

ss::future<> merge_all_records_fixed_size(bool& should_increase_record, const ss::sstring& tmp_file, const ss::sstring& tmp_output){
    return ss::do_with(
        size_t(0),
        [&](auto& offset){ // offset in bytes
            return ss::repeat(
                [&]{
                    return merge_k_records(offset, tmp_file, tmp_output).then([&](auto can_continue){
                        if (can_continue) {
                            should_increase_record = true;
                            offset += number_of_records_to_merge * chunks_in_record * aligned_size;
                            std::cout << "gonna continue with new offset:" <<  offset << "\n";
                            return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
                        } else {
                            std::cout << "not gonna continue\n";
                            return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                        }
                    });
                }
            );
        }
    );
}

ss::future<> merge_records(ss::sstring& tmp_file, ss::sstring& tmp_output, const ss::sstring& fname_output){
    std::cout << "tmp_file: " << tmp_file << "\n";
    std::cout << "tmp_output: " << tmp_output << "\n";
    return ss::do_with(
        bool(false),
        [&](auto& should_increase_record){
            return ss::repeat(
                [&]{
                    return merge_all_records_fixed_size(should_increase_record, tmp_file, tmp_output).then([&]{
                        if (should_increase_record) {
                            should_increase_record = false;
                            chunks_in_record *= number_of_records_to_merge;
                            record_size = chunks_in_record*aligned_size;
                            std::cout << "Increasing record size to: " << record_size << "\n";
                            std::swap(tmp_file, tmp_output);
                            std::cout << "gonna write output to: " << tmp_output << "\n";
                            return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
                        } else {
                            std::cout << "Not gonna increase record size\n";

                            fs::copy(tmp_output.c_str(), fname_output.c_str());
                            return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::yes);
                        }
                    });
                }
            );
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
            if (filenames.size() == 0){
                throw std::runtime_error("Input file name not provided.");
            } else if (filenames.size() == 1) {
                throw std::runtime_error("Ouput file name not provided.");
            }

            return ss::tmp_dir::do_with(
                [&](auto& t) {
                    return ss::do_with(
                        ss::sstring((t.get_path() / "tmp_file").native()),
                        ss::sstring((t.get_path() / "tmp_output").native()),
                        ss::sstring(filenames[0]),
                        ss::sstring(filenames[1]),
                        [&](auto& tmp_file, auto& tmp_output, auto& fname_input, auto& fname_output){
                            return sort_chunks_inside_records(fname_input, tmp_file).then([&]{
                                return merge_records(tmp_file, tmp_output, fname_output);
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