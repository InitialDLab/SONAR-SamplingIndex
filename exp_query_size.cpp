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
#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <cstdlib>
#include <sstream>

#include <boost/geometry/geometries/box.hpp>
#include <boost/timer/timer.hpp>
#include <boost/lexical_cast.hpp>


#include "rtree/rtree.h"
#include "rtree/tests/integrity_checker.h"

#include "level_sampling/level_sampling.h"

#include "mongo_types.h"
#include "hilbert/hilbert.h"

std::string const DATASET = "geo_with_alt";
int const LEVEL_SAMPLING_MIN_MEM_LEVEL = 4;
int const REPEAT = 100;
int const SAMPLE_SIZE = 10000;


struct DummyIterator
{
    DummyIterator& operator++() { return *this; }
    DummyIterator& operator++(int) { return *this; }
    DummyIterator& operator*() { return *this; }
    template<typename T>
    void operator= (T const& t) { }
};

struct entry_with_alt
    : mongo_types::entry
{
    entry_with_alt() { }
    entry_with_alt(float x, float y, int timestamp, int altitude, std::string const& id) 
        : mongo_types::entry(x, y, timestamp, id)
        , altitude(altitude)
    { }
    int altitude;
};

struct sample_entry_with_alt
    : mongo_types::sample_entry
{
    sample_entry_with_alt () { }
    sample_entry_with_alt (entry_with_alt const& e) 
        : mongo_types::sample_entry(e)
        , altitude(e.altitude)
    { }

    int altitude;
};

namespace std {
    template <>
    struct hash<sample_entry_with_alt>
    {
        std::size_t operator () (sample_entry_with_alt const& e) const {
            return std::hash<mongo_types::sample_entry>()(e);
        }
    };
}


/*
 * cache system disk cache
 */
const char * clear_cache_cmd = "/uusoc/scratch/magpie17/robertc/clearCache > /dev/null";
void clear_cache()
{
    auto unused = system(clear_cache_cmd);
    // TODO clear block manager cache when implemented
}

namespace bg = boost::geometry;
namespace bt = boost::timer;


struct Exp
{
    // how many sample queries to perform
    using entry = entry_with_alt;
    using sample_entry = sample_entry_with_alt;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    using rtree_t = rtree::rtree<
        entry, 
        sample_entry, 
        box,
        256, // node sample count
        16  // max fanout
    >;

    using ls_t = level_sampling::level_sampling<
        entry,
        sample_entry,
        box,
        16 // max fanout
    >;

    Exp(std::string const& rtree_name, std::string const& ls_name) 
        : rtree_name(rtree_name)
        , ls_name(ls_name)
    { }

    std::vector<entry_with_alt> raw_data;
    void build(std::string const& raw_data_filename) {
        load_raw_data(raw_data_filename);
        build_rtree();
        build_ls();
        std::cerr << "Done." << std::endl;
    }

    void load_raw_data(std::string const& filename) {
        std::cerr << "loading raw data" << std::endl;
        raw_data.clear();
        std::string line;
        std::ifstream inf(filename);
        while(std::getline(inf, line))
        {
            if(line.empty())
                return;

            float x_val;
            float y_val;
            int   time;
            int altitude;
            char OIDvalue[32];

            int ret = sscanf(line.c_str(), "%24s\t%f\t%f\t%d\t%d", OIDvalue,
                    &x_val, &y_val, &altitude, &time);


            raw_data.emplace_back(x_val, y_val, time, altitude, OIDvalue);
        }
    }

    void build_rtree(void) {
        std::cerr << "building rtree io layers" << std::endl;
        rtree_t::build_io_layers(raw_data.begin(), raw_data.end(), rtree_name);
        std::cerr << "generating rtree samples and saving rtree mem nodes" << std::endl;
        std::unique_ptr<rtree_t>(new rtree_t(rtree_name))->save_mem_nodes();
    }

    void build_ls(void) {
        std::cerr << "building level sampling" << std::endl;
        ls_t::build(raw_data.begin(), raw_data.end(), ls_name);
    }

    void load_rtree (bool load_mem_nodes = false) {
        bt::cpu_timer timer;
        rtree_ptr.reset(new rtree_t(rtree_name, false, load_mem_nodes));
        std::cout << "tree size: " << rtree_ptr->size() << std::endl;

        auto stats = rtree_ptr->get_stats();
        std::cout << "leaf node count: " << stats.leaf_nodes << std::endl;
        std::cout << "internal node count: " << stats.internal_nodes << std::endl;
        std::cout << "tree building time(ms): " << ' ' << timer.elapsed().wall/1000000.0 << std::endl;

        std::cout << "bbox: ";
        dump_box(std::cout, rtree_ptr->bbox());
        std::cout << std::endl;
    }

    void load_ls (void) {
        bt::cpu_timer timer;
        ls_ptr.reset(new ls_t(ls_name, LEVEL_SAMPLING_MIN_MEM_LEVEL));
        std::cout << "min mem level: " << ls_ptr->min_mem_level << std::endl;
        std::cout << "max level: " << ls_ptr->max_level << std::endl;
    }

    void run() {
        load_rtree(true);
        load_ls();

        std::vector<sample_entry> buffer;

        std::vector<box> all_queries;

        {
            std::string queries_string = \
"39.56232834 114.4697037 285657216 40.23783493 118.286499 289862560 2001713 "\
"38.9452095 110.4104156 295326784 41.03949356 122.2436752 308364608 3997459 "\
"39.02836609 108.5703583 275396864 41.66404724 123.4626312 291805120 8003465 "\
"37.34314728 101.044838 285514560 42.78077698 131.7688599 319366144 9998364 "\
"35.58291626 91.98937988 262880304 44.21461487 140.7607269 316616352 17987463"\
;
            std::istringstream sin(queries_string);
            float x1,y1,z1,x2,y2,z2,unused;
            while(sin >> x1 >> y1 >> z1 >> x2 >> y2 >> z2 >> unused)
            {
                all_queries.emplace_back(point(x1, y1, z1), point(x2, y2, z2));
            }
        }

        box query;


        if(rtree_ptr)
            rtree_ptr->get_block_manager().set_cache_capacity(0);

        for(auto query: all_queries) 
        {
            long long query_size;
            double count_query_time;
            { // baseline
                rtree_ptr->flush_cache();
                clear_cache();
                bt::cpu_timer timer;
                auto cursor = rtree_ptr->naive_sample_query(query);
                timer.stop();
                //test_cursor(cursor, timer);
                query_size = cursor.get_count();
                std::cout << "query size: " << query_size << std::endl;

                count_query_time = (timer.elapsed().wall / 1000000.0);
                std::cout << "count query time: " << count_query_time << std::endl;
            }

            { // rs-tree
                rtree_ptr->flush_cache();
                clear_cache();
                bt::cpu_timer timer;
                auto cursor = rtree_ptr->sample_query(query);
                timer.stop();

                DummyIterator dummy_iterator;
                std::cout << "Time(ms) IO ~size error sd ci95" << std::endl;
                std::ostringstream sout;
                for(int i = 0; i < REPEAT; ++i)
                {
                    timer.resume();
                    cursor.get_samples(SAMPLE_SIZE, dummy_iterator);
                    timer.stop();

                    double sd;
                    long long estimate_count = cursor.estimate_count(&sd);

                    auto stats = cursor.get_stats();

                    double time_elapsed = (timer.elapsed().wall / 1000000.0);
                    if(time_elapsed > count_query_time * 0.1)
                        break;

                    sout.str("");
                    sout << time_elapsed
                        << " " << cursor.get_io_cost()
                        << " " << estimate_count
                        << " " << (estimate_count - query_size)
                        << " " << sd
                        << " " << (1.96l * sd / std::sqrt(rtree_t::mem_node_sample_size));
                }
                std::cout << sout.str() << std::endl;
            }
        }
    }

    std::unique_ptr<rtree_t> rtree_ptr;
    std::unique_ptr<ls_t> ls_ptr;
    std::string rtree_name, ls_name;
    double exact_mean;
};

int main(int argc, char ** argv)
{
    if(argc != 2)
    {
        std::cerr << "argument: build | run" << std::endl;
        return -1;
    }
    std::string path = "/uusoc/scratch/magpie21/luwang/data/built/" + DATASET;
    Exp exp(path + "_tree", path + "_level");

    std::string cmd(argv[1]);
    if(cmd == "build")
        exp.build("/uusoc/scratch/magpie21/robertc/sample/raw/Geolife_with_alt.txt");
    else if (cmd == "run")
        exp.run();
    else
        std::cerr << "unknown cmd: " << cmd << std::endl;

    return 0;
}
