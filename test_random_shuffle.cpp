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
#include <vector>
#include <iostream>
#include <string>
#include <cstdlib>

#include <boost/timer/timer.hpp>

#include <rtree/rtree.h>
#include "random_shuffle.h"

#include "mongo_types.h"

//#define USE_OSM

std::string IN_FN = "/tmp/in.txt";
//std::string OUT_FN = "/tmp/out.bin";
#ifdef USE_OSM
std::string OUT_FN = "./built/osm_shuffle_1.shf";
#else
std::string OUT_FN = "./built/geo_shuffle.shf";
#endif

using point = mongo_types::point3d;
using box = mongo_types::box3d;
using entry = mongo_types::entry;

template <class T>
void do_query_for_rs(T & rs)
{
    boost::timer::cpu_timer timer;
    auto cursor = rs.sample_query(
#ifdef USE_OSM
            box(
                point(40.64967346, 16.55130386, 1249275136),
                point(46.23781204, 27.72757912, 1335750784)
                )
#else
            box(
                point(39.83063889,115.985733,287327584),
                point(39.96952438,116.7704773,288192224)
                )
#endif
            );
    std::cout << "after cursor " << (timer.elapsed().wall / 1000000.0) << std::endl;

#ifdef USE_OSM
    size_t K_piece = 20000;
    size_t K = 200000;
#else
    size_t K_piece = 50000;
    size_t K = 50000;
#endif
    std::vector<entry> buffer;
    {
        boost::timer::auto_cpu_timer _timer;
        _timer.stop();
        size_t samples_got = 0;
        while(samples_got < K)
        {
            buffer.clear();
            _timer.resume();
            cursor.get_samples(K_piece, std::back_inserter(buffer));
            _timer.stop();
            if(buffer.empty()) break;
            samples_got += buffer.size();
            //std::cout << samples_got << ' ' << (timer.elapsed().wall / 1000000.0) << std::endl;
        }
        std::cout << samples_got << ' ' << (timer.elapsed().wall / 1000000.0) << std::endl;
    }
    auto stats = cursor.get_stats();
    std::cerr << "cache_read " << stats.cached_entries_read << std::endl;
    std::cerr << "disk_read " << stats.disk_entries_read << std::endl;
    std::cerr << "disk_block_read " << stats.blocks_read << std::endl;
}

void clear_cache()
{
    int i = system("/uusoc/scratch/magpie18/robertc/sample/clearCache");
    if(i != 0)
        std::cerr << "WARNING: clear_cache() failed!" << std::endl;
}

void do_query()
{
#ifdef USE_OSM
    rtree::IOLayersParameters parameters;
    parameters.max_top_layer_io_node_count = 1024;
    auto memory_limit = rtree::rtree<entry>::estimate_memory_usage(2210518076ULL, parameters);
#else
    auto memory_limit = rtree::rtree<entry>::estimate_memory_usage(24876977);
#endif
    std::cerr << "memory_limit(G) " << ((double)memory_limit)/1024/1024/1024 << std::endl;
    //debug 
    memory_limit = 24876977*30;
    //RandomShuffle<entry> rs(OUT_FN, 1024*16*sizeof(entry));
    RandomShuffle<entry> rs(OUT_FN, memory_limit);
    std::cerr << "create rs " << std::endl;
    std::cerr << "entry_count " << rs.entry_count << std::endl;
    std::cerr << "block_size " << rs.block_size << std::endl;

    //clear_cache();
    do_query_for_rs(rs);

    std::cerr << "destroy rs" << std::endl;
}

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        std::cerr << "Need exactly 1 argument" << std::endl;
        return -1;
    }

    std::string cmd = argv[1];

    if(false) { }
#if 0
    else if(cmd == "build")
    {
        {
            std::ofstream fout(IN_FN);
            for(int i = 0; i < 16*4096; ++i)
                fout << i << std::endl;
        }

        RandomShuffle<entry>::build(IN_FN, OUT_FN,
                [](std::string const& line)->entry{ return entry(std::stoi(line)); },
                RandomShuffle<entry>::Parameters(),
                1024*1024*1024
                );
    }
#endif 
    else if (cmd == "query")
    {
        do_query();
    }
    else
    {
        std::cerr << "Invalid argument" << std::endl;
        return -1;
    }

    return 0;
}
