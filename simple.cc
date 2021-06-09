#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <boost/iterator/counting_iterator.hpp>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
namespace ss = seastar;

constexpr size_t aligned_size = 4096;
constexpr size_t record_size = 4*aligned_size;
constexpr bool debug = true;


ss::sstring fname_output = "/root/seastar-starter/simple_output.txt";
ss::sstring fname_input = "/root/seastar-starter/simple_input.txt";

ss::future<> write_to_file(int& pos, ss::temporary_buffer<char>& buf, ss::sstring fname) {
    return with_file(ss::open_file_dma(fname, ss::open_flags::wo | ss::open_flags::create),
        [&buf, &pos](ss::file f) mutable {
            std::cout << "opened file\n";
            return f.dma_write<char>(pos*aligned_size, buf.get(), record_size).then([&pos](size_t unused){
                std::cout << "I wrote\n" << std::flush;
            });
    });
}

std::vector<std::string> sort_record(ss::temporary_buffer<char>& record) {
    std::vector<std::string> chunks_by4k;
    if (debug) {
        std::cout << "record.size(): " << record.size() << "\n";
    }
    for (int offset =0; offset + aligned_size <= record.size(); offset+=aligned_size ){
        chunks_by4k.emplace_back(std::string(record.get() + offset, aligned_size));
    }

    for (auto &chunk: chunks_by4k) {
        std::cout << "chunk: " << chunk << "\n";
    }
    return chunks_by4k;

}

ss::future<> read_from_file(int& pos, ss::temporary_buffer<char>& buf, const ss::sstring& fname){
    return with_file(ss::open_file_dma(fname, ss::open_flags::ro),
        [&buf, &pos](ss::file& f) mutable {
            std::cout << "opened file to read\n";
            return f.dma_read<char>(pos*aligned_size, buf.get_write(), record_size).then([&buf, &pos](size_t unused){
                if (debug) {
                    std::cout << "I read\n";
                    for (int i = 0; i < record_size; ++i) {
                        std::cout << buf[i];
                    }
                    std::cout << "\n";
                }
            });
        });
}

int main(int argc, char** argv) {
    using namespace std::chrono_literals;
    seastar::app_template app;
    try {
        app.run(argc, argv, []{
            return ss::do_with(
                ss::temporary_buffer<char>::aligned(aligned_size, record_size),
                std::vector<int>{0},
                [](auto& buf, auto & range){
                    return ss::do_for_each(
                        range,
                        [&buf](int& pos) {
                            std::cout << "pos: " << pos << "\n";
                            return read_from_file(pos, buf, fname_input).then([&buf, &pos] {
                                auto chunks = sort_record(buf);
                                // todo: write to file by chunks
                                return write_to_file(pos, buf, fname_output);
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