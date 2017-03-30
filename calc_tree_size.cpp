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
/*
 * Rtree Experiements 
 * 
 * by Lu Wang 2014
 */

#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <cstdlib>

#include <boost/geometry/geometries/box.hpp>
#include <boost/timer/timer.hpp>
#include <boost/lexical_cast.hpp>


#include "rtree/rtree.h"
#include "rtree/tests/integrity_checker.h"

#include "level_sampling/level_sampling.h"

#include "mongo_types.h"
#include "hilbert/hilbert.h"

//#define USE_OSM

#ifdef USE_OSM
// osm
std::string const DATASET = "osm";
int const LEVEL_SAMPLING_MIN_MEM_LEVEL = 12;
#else
// geo
std::string const DATASET = "geo";
int const LEVEL_SAMPLING_MIN_MEM_LEVEL = 4;
#endif


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
    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
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

    void build_rtree (bool load_mem_nodes = false) {
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

    void build_ls (void) {
        bt::cpu_timer timer;
        ls_ptr.reset(new ls_t(ls_name, LEVEL_SAMPLING_MIN_MEM_LEVEL));
        std::cout << "min mem level: " << ls_ptr->min_mem_level << std::endl;
        std::cout << "max level: " << ls_ptr->max_level << std::endl;
    }

    void run() {
        build_rtree(true);
        auto stats = rtree_ptr->count_nodes();
        std::cerr << "internal nodes: " << stats.internal_nodes << std::endl;
        std::cerr << "leaf nodes: " << stats.leaf_nodes << std::endl;
        std::cerr << "io internal nodes: " << stats.io_internal_nodes << std::endl;
        std::cerr << "io leaf nodes: " << stats.io_leaf_nodes << std::endl;
    }

    std::unique_ptr<rtree_t> rtree_ptr;
    std::unique_ptr<ls_t> ls_ptr;
    std::string rtree_name, ls_name;

    std::vector<box> queries;
};

int main(int argc, char ** argv)
{
    //std::string filename = argv[1];
    std::string path = "/uusoc/scratch/magpie21/luwang/data/built/" + DATASET;

    //Exp(filename+"-rtree", filename+"-ls").run();
    Exp(path + "_tree", path + "_level").run();

    return 0;
}
