/*
Copyright 2017 InitialDLab

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <memory>
//#include <unistd.h>
#include <cstdio>

#include <boost/geometry/geometry.hpp>

//#include <tpie/tpie.h>
//#include <tpie/file_stream.h>
//#include <tpie/sort.h>
//#include <tpie/progress_indicator_null.h>

template<typename Value>
struct RandomShuffle
{
    // should be consistent with rtree/io_layers.h for experiments
    struct Parameters {
        double fill_ratio = 0.7;
        size_t block_size = 8192;
    };

    using converter_t = std::function<Value(std::string const&)>;
    using value_with_random_id_t = std::pair<Value, int>;
    //using file_stream_t = tpie::file_stream<value_with_random_id_t>;
    using cache_t = std::vector<Value>;

    static void
    build(
        std::string const& in_filename, 
        std::string const& out_filename, 
        converter_t ReadConverter, 
        Parameters const& parameters,
        size_t tpie_allowed_memory
    );

    RandomShuffle(std::string const& filename, size_t cache_size = 0) 
        : filename(filename)
    { 
        cache.reserve(cache_size / sizeof(Value) + 1);
        std::ifstream fin(filename, std::ifstream::binary);
        fin.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
        fin.read(reinterpret_cast<char*>(&block_size), sizeof(block_size));

        int64_t entry_read = 0;
        size_t cache_used = 0;
       
        value_with_random_id_t v;
        entry_read = 0;
        block_read_for_cache = 0 ;
        while(entry_read < entry_count)
        {
            auto vec = read_block(fin, block_read_for_cache); 
            if(cache_used + sizeof(Value) * vec.size() > cache_size)
                break;

            ++block_read_for_cache;
            cache.insert(cache.end(), vec.begin(), vec.end());
            cache_used += sizeof(Value) * vec.size();
            entry_read += vec.size();
        }
    }

    std::vector<Value>
    read_block(std::ifstream & in, size_t block_id) const
    {
        std::vector<Value> v;
        size_t offset = sizeof(int64_t) + sizeof(size_t) + block_size * block_id;
        in.seekg(offset);
        int64_t count;
        in.read(reinterpret_cast<char*>(&count), sizeof(count));
        value_with_random_id_t e;
        for(int64_t i = 0; i < count; ++i)
        {
            in.read(reinterpret_cast<char*>(&e), sizeof(e));
            v.push_back(e.first);
        }
        return v;
    }

    template<typename Geometry>
    struct sample_query_cursor {
        sample_query_cursor(Geometry const& query, RandomShuffle const& rs)
            : query(query)
            , rs(rs)
            , cache(rs.cache)
            , pfs(new std::ifstream(rs.filename, std::ifstream::binary))
            , entry_read(rs.cache.size())
            , block_read(rs.block_read_for_cache)
            , entry_count(rs.entry_count)
            , block_size(rs.block_size)
        { }

        struct Stats {
            size_t cached_entries_read = 0;
            size_t disk_entries_read = 0;

            size_t blocks_read = 0;
            size_t accepted_results = 0;
            size_t rejected_results = 0;
        };

        Stats get_stats() const { return stats; }

        template<typename OutIter>
        void
        get_samples(size_t sample_size, OutIter out_iter) {
            while((sample_size > 0) && (cache_used < cache.size()))
            {
                Value const& v = cache[cache_used];
                ++cache_used;
                ++stats.cached_entries_read;
                if(boost::geometry::covered_by(v.get_point(), query)) {
                    *out_iter = v;
                    ++out_iter;
                    --sample_size;

                    stats.accepted_results++;
                }
                else {
                    stats.rejected_results++;
                }
            }
            while(sample_size > 0)
            {
                if(buffer.empty())
                {
                    if(entry_read >= entry_count) break;
                    buffer = rs.read_block(*pfs, block_read);
                    ++stats.blocks_read;
                    ++block_read;
                    stats.disk_entries_read += buffer.size();
                    entry_read += buffer.size();
                }
                Value v = buffer.back();
                buffer.pop_back();
                if(boost::geometry::covered_by(v.get_point(), query)) {
                    *out_iter = v;
                    ++out_iter;
                    --sample_size;
                    stats.accepted_results++;
                }
                else {
                    stats.rejected_results++;
                }
            }
        }

        Geometry query;
        RandomShuffle const& rs;
        cache_t const& cache;
        std::unique_ptr<std::ifstream> pfs;
        int64_t entry_count;
        size_t block_size;
        size_t cache_used = 0;
        size_t entry_read;
        size_t block_read; // blocks read for cache
        Stats stats;
        std::vector<Value> buffer;
    };

    template<typename Geometry>
    sample_query_cursor<Geometry>
    sample_query(Geometry const& query) {
        return sample_query_cursor<Geometry>(query, *this);
    }

    std::string filename;
    int64_t entry_count;
    size_t block_size;
    size_t block_read_for_cache;
    cache_t cache;
};

template<typename Value>
void RandomShuffle<Value>::build(
    std::string const& in_filename, 
    std::string const& out_filename, 
    converter_t ReadConverter, 
    Parameters const& parameters,
    size_t tpie_allowed_memory
)
{
    throw "convert to using newer template class";

    //tpie::tpie_init();
    //tpie::get_memory_manager().set_limit(tpie_allowed_memory);

    const char *tmp_filename = "./staging";
    int64_t entry_count = 0;
    // read plain text input file
    // assign random id and write to binary output file
    // see also IOLayers::convert_data_file
    {
        std::ifstream fin(in_filename);
        //file_stream_t tmp_fout;
        remove(tmp_filename); // make sure it is not there before trying to open the temp file.
        //tmp_fout.open(tmp_filename);

        std::default_random_engine rng(std::random_device{}());
        std::uniform_int_distribution<int> random_id(std::numeric_limits<int>::min(), std::numeric_limits<int>::max());
        std::string line;
        while(fin)
        {
            std::getline(fin, line);
            if(line.size() == 0) continue;
            //tmp_fout.write(std::make_pair(
            //            (Value)std::move(ReadConverter(line)),
            //            random_id(rng)
            //            ));
            ++entry_count;
        }
        fin.close();

        // sort based on random id
        throw "sort by random id using new interface";
        //tpie::progress_indicator_null pi;
        //tpie::sort(
        //        tmp_fout,
        //        [](value_with_random_id_t const& p1, value_with_random_id_t const& p2)->bool{
        //        return p1.second < p2.second;
        //        },
        //        pi
        //        );
    }

    // dump to our own format
    throw "update dumping of formating information";
    //{
    //    std::ofstream fout(out_filename, std::ofstream::binary);
    //    fout.write(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
    //    size_t block_size = parameters.block_size;
    //    fout.write(reinterpret_cast<char*>(&block_size), sizeof(block_size));

    //    size_t block_capacity = (block_size - sizeof(int64_t)) / sizeof(value_with_random_id_t) * parameters.fill_ratio;

    //    file_stream_t tmp_fin;
    //    tmp_fin.open(tmp_filename);

    //    size_t entries_in_buffer = 0;
    //    char * buffer = new char[block_size];

    //    while(tmp_fin.can_read())
    //    {
    //        if(entries_in_buffer == block_capacity)
    //        {
    //            *reinterpret_cast<int64_t*>(buffer) = (int64_t) entries_in_buffer;
    //            fout.write(buffer, block_size);
    //            entries_in_buffer = 0;
    //        }
    //        value_with_random_id_t const& v = tmp_fin.read();
    //        memcpy(buffer + sizeof(int64_t) + sizeof(value_with_random_id_t) * entries_in_buffer, &v, sizeof(v));
    //        ++entries_in_buffer;
    //    }

    //    if(entries_in_buffer > 0)
    //    {
    //        *reinterpret_cast<int64_t*>(buffer) = (int64_t) entries_in_buffer;
    //        fout.write(buffer, block_size);
    //        entries_in_buffer = 0;
    //    }

    //    delete [] buffer;

    //    ::unlink(tmp_filename);
    //}
}
