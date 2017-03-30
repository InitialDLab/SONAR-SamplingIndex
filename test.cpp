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

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <boost/geometry/geometries/box.hpp>
#include <boost/timer/timer.hpp>
#include <boost/lexical_cast.hpp>

#include "rtree/rtree.h"
#include "rtree/tests/integrity_checker.h"

#include "level_sampling/level_sampling.h"

#include "mongo_types.h"
#include "hilbert/hilbert.h"

#define USE_OSM

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
//const char *CLEAR_CACHE_CMD = "/uusoc/scratch/magpie18/robertc/sample/clearCache";
const char *CLEAR_CACHE_CMD = "/bin/true";
void clear_cache()
{
#if 0
    int pid = fork();
    if(pid == 0) // child
    {
        int ret = execve(CLEAR_CACHE_CMD, nullptr, nullptr);
        if(ret != 0)
            std::cerr << "WARNING: clear_cache() failed!" << std::endl;
        _exit(1);
    }
    else
    {
        while (true) 
        {
            int status;
            pid_t done = ::wait(&status);
            if (done == -1) {
                if (errno == ECHILD) break; // no more child processes
            } else {
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                    std::cerr << "clear cached failed" << std::endl;
                    exit(1);
                }
            }
        }
    }
#else
    if(system(CLEAR_CACHE_CMD) != 0)
      std::cerr << "clear cached failed" << std::endl;
#endif
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
        box
    >;

    using ls_t = level_sampling::level_sampling<
        entry,
        sample_entry,
        box
    >;

    Exp(std::string const& rtree_name, std::string const& ls_name) 
        : rtree_name(rtree_name)
        , ls_name(ls_name)
    { }

    void build_rtree (bool load_mem_nodes = false) {
        bt::cpu_timer timer;
        rtree_ptr.reset(new rtree_t(
            rtree_name, 
            false, 
            load_mem_nodes, 
            ((size_t)4)*1024*1024*1024
        ));
        std::cout << "tree size: " << rtree_ptr->size() << std::endl;

        auto stats = rtree_ptr->get_stats();
        std::cout << "leaf node count: " << stats.leaf_nodes << std::endl;
        std::cout << "internal node count: " << stats.internal_nodes << std::endl;
        std::cout << "tree building time(ms): " << ' ' << timer.elapsed().wall/1000000.0 << std::endl;

        std::cout << "bbox: ";
        dump_box(std::cout, rtree_ptr->bbox());
        std::cout << std::endl;

        std::cout << "estimated_memory_usage: " << rtree_ptr->estimate_memory_usage(rtree_ptr->size()) << std::endl;
    }

    void build_ls (void) {
        bt::cpu_timer timer;
        ls_ptr.reset(new ls_t(ls_name, -1, 4UL*1024*1024*1024));
        std::cout << "min mem level: " << ls_ptr->min_mem_level << std::endl;
        std::cout << "max level: " << ls_ptr->max_level << std::endl;
    }

    void run() {
        build_rtree(true);
        //build_ls();

        std::vector<sample_entry> buffer;
        box query;

        //query = rtree_ptr->bbox();

        //q=249021
        box geo_query1(
            point(39.83063889,115.985733,287327584),
            point(39.96952438,116.7704773,288192224)
        );
        box geo_query2(
            point(1.044023991, -39.27259827, 155019328),
            point(39.85100937, 179.9969482, 396609536)
        );

        // q=9827868
        box osm_query1(
            point(40.64967346, 16.55130386, 1249275136),
            point(46.23781204, 27.72757912, 1335750784)
        );

#ifdef USE_OSM
        query = osm_query1;
#else
        query = geo_query1;
#endif

#ifdef USE_OSM
        const int REPEAT = 1;
        const int SAMPLE_SIZE = 200000;
#else
        const int REPEAT = 1;
        const int SAMPLE_SIZE = 50000;
#endif

        if(rtree_ptr)
            rtree_ptr->get_block_manager().set_cache_capacity(0);

        if(0)
        { // baseline
            rtree_ptr->flush_cache();
            clear_cache();

            bt::cpu_timer timer;
            auto cursor = rtree_ptr->naive_sample_query(query);

            {
                std::cout << "range count " << cursor.get_count() << std::endl;
                std::cout << "node count " << cursor.get_node_count() << std::endl;
                std::cout << "value count " << cursor.get_value_count() << std::endl;
            }

            std::cout << "No Time(ms) IO IO" << std::endl;
            for(int i = 0; i <= REPEAT; ++i)
            {
                auto stats = cursor.get_stats();
                std::cout << i 
                    << " " << (timer.elapsed().wall / 1000000.0)
                    << " " << (stats.io_internal_nodes + stats.io_leaf_nodes)
                    << " " << cursor.get_io_cost()
                    << std::endl;

                if(i==REPEAT) break;

                buffer.clear();
                cursor.get_samples(SAMPLE_SIZE, std::back_inserter(buffer));
            }
        }

        { // rs-tree
            if(0)
            {
                auto cursor = rtree_ptr->naive_sample_query(query);

                {
                    std::cout << "range count " << cursor.get_count() << std::endl;
                    std::cout << "node count " << cursor.get_node_count() << std::endl;
                    std::cout << "value count " << cursor.get_value_count() << std::endl;
                }
            }
            rtree_ptr->flush_cache();
            //clear_cache();

            bt::cpu_timer timer;
            auto cursor = rtree_ptr->sample_query(query);
            timer.stop();
            std::cout << "after cursor " << (timer.elapsed().wall / 1000000.0) << std::endl;

            size_t samples_got = 0;
            buffer.clear();
            std::cout << "No Time(ms) | IO IO |frontier| #internal #leaf | #io_internal #io_leaf" << std::endl;
            for(int i = 0; i <= REPEAT; ++i)
            {
                auto stats = cursor.get_stats();

                if(i == REPEAT)
                {
                    std::cout << samples_got
                        << " " << (timer.elapsed().wall / 1000000.0)
                        << " | " << (stats.io_internal_nodes 
                                + stats.io_leaf_nodes
                                + stats.io_sample_nodes)
                        << " " << cursor.get_io_cost()
                        << " | " << cursor.node_count()
                        << " | " << stats.internal_nodes
                        << " " << stats.leaf_nodes
                        << " | " << stats.io_internal_nodes
                        << " " << stats.io_leaf_nodes
#ifdef RSTREE_PROFILING
                        << " rejected " << stats.values_rejected
                        << " scanned " << stats.leaf_values_scanned
#endif
                        << std::endl;
                    break;
                }

                buffer.clear();
                timer.resume();
                cursor.get_samples(SAMPLE_SIZE, std::back_inserter(buffer));
                timer.stop();
                samples_got += buffer.size();
            }
        }

        if(0)
        { // level sampling
            ls_ptr->flush_cache();
//            clear_cache();

            bt::cpu_timer timer;
            auto cursor = ls_ptr->sample_query(query);

            std::cout << "No Time(ms) IO IO #internal #leaf" << std::endl;
            for(int i = 0; i < REPEAT; ++i)
            {
                auto stats = cursor.get_stats();
                std::cout << i 
                    << " " << (timer.elapsed().wall / 1000000.0)
                    << " " << (stats.io_internal_nodes + stats.io_leaf_nodes)
                    << " " << cursor.get_io_cost()
                    << " " << stats.internal_nodes
                    << " " << stats.leaf_nodes
                    << std::endl;

                buffer.clear();
                cursor.get_samples(SAMPLE_SIZE, std::back_inserter(buffer));
            }
        }
    }

    std::unique_ptr<rtree_t> rtree_ptr;
    std::unique_ptr<ls_t> ls_ptr;
    std::string rtree_name, ls_name;

    std::vector<box> queries;
};

int main(int argc, char ** argv)
{
    //std::string filename = argv[1];
    //std::string path = "/uusoc/scratch/magpie21/luwang/data/built/" + DATASET;

    //Exp(filename+"-rtree", filename+"-ls").run();
    //Exp(path + "_tree", path + "_level").run();

    //Exp("./built/geo_tree", "").run();
    Exp(
        "./built/" + DATASET + "_tree_blockSize-8192", 
        "./built/" + DATASET + "_level"
    ).run();
    //Exp("./built/" + DATASET + "_tree", "").run();
    //Exp("./built/geo_tree", "").test();

    return 0;
}
