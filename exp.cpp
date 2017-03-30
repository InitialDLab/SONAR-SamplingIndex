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
#include <cctype>
#include <fstream>

#include <boost/geometry/geometries/box.hpp>
#include <boost/timer/timer.hpp>
#include <boost/lexical_cast.hpp>

// required to support erasing feature.  This adds extra data to the sampling element
// which is used to check for equality
//  This is defined in the command to compile because I am compiling several compilation units.
//#define RTREE_ERASE
#include "rtree/rtree.h"
#include "rtree/tests/integrity_checker.h"

#include "level_sampling/level_sampling.h"

#include "mongo_types.h"
#include "hilbert/hilbert.h"

#include "random_shuffle.h"

#include "experiments/build_tree_experiments.h"
#include "experiments/query_tree_experiments.h"
#include "experiments/aggragate_experiments.h"
#include "experiments/insert_and_delete_tree_experiments.h"
#include "experiments/Data_Source_Information.h"
#include "experiments/experiment_utilities.h"
#include "experiments/vary_sample_buffer.h"
#include "experiments/Independence_Experiment.h"
#include "experiments/query_latency_experiment.h"

#define ENABLE_NAIVE
#define ENABLE_SAMPLE
#define ENABLE_LEVEL

#define SHOW_PROGRESS

constexpr size_t NODE_SAMPLE_SIZE = 16;

#define BOOST_NO_EXCEPTION

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

#ifdef DEBUG
constexpr size_t memory_use = 6L * 1024 * 1024 * 1024;
#else
constexpr size_t memory_use = 52L * 1024 * 1024 * 1024;
#endif

int main(int argc, char ** argv)
{
    if (argc == 1)
        std::cerr << "give options for which experiments to run\n query_file_osm_a=[eblrs] (e=estimation test, b=baseline, l=level sampling, r=range query, s=random shuffle)\n query_file_geo_a=[eblrs] (e=estimation test, b=baseline, l=level sampling, r=range query, s=random shuffle)\n aggragate_geo\n aggragate_osm\n build_osm\n build_geo\n modify_geo_file\n modify_osm_file\n modify_geo_file_baseline \n modify_osm_file_baseline\n modify_geo_file_lstree\n build_osm_single\n construct_only_geo_rtree\n construct_only_osm_rtree\n construct_only_geo_level\n construct_only_osm_level\n construct_only_geo_shuffle\n construct_only_osm_shuffle\n find_queries_geo=|Q|,tolerance\n find_queries_osm=|Q|,tolerance\n extra\n construct_sample_node_structure\n run_sample_node_structure\n build_geo_block_size=K Build the geo data with block size K\n build_osm_block_size=K Build the osm data with block size of K\n build_mem_nodes_file=input rtree\n" << std::endl;
        //std::cerr << "give options for which experiments to run\n query_osm\n query_geo\n query_osm_a=[eblr] (e=estimation test, b=baseline, l=level sampling, r=range query)\n query_geo_a=[eblr] (e=estimation test, b=baseline, l=level sampling, r=range query)\n query_file_osm_a=[eblr] (e=estimation test, b=baseline, l=level sampling, r=range query)\n query_file_geo_a=[eblr] (e=estimation test, b=baseline, l=level sampling, r=range query)\n aggragate_geo\n aggragate_osm\n build_osm\n build_geo\n modify_osm\n modify_osm_p=(float) <- used to indicate specific cover\n modify_geo\n modify_geo_p=(float) <- used to indicate specific cover\n build_geo_single\n build_osm_single\n construct_only_geo_rtree\n construct_only_osm_rtree\n construct_only_geo_level\n construct_only_osm_level\n find_queries_geo=|Q|,tolerance\n find_queries_osm=|Q|,tolerance" << std::endl;

    for (int i = 1; i < argc; ++i)
    {
        std::string argument{ argv[i] };
        
 
        if (argument.substr(0, 17) == "query_file_osm_a=")
        {
            Query_tree_experiments querying_experiments;

            for (int i = 17; i < argument.size(); i++)
            {
                char value = tolower(argument[i]);
                switch (value)
                {
                case 'e':
                    querying_experiments.set_testEstimation(true);
                    break;
                case 'b':
                    querying_experiments.set_testBaseline(true);
                    break;
                case 'l':
                    querying_experiments.set_testLevelSample(true);
                    break;
                case 'r':
                    querying_experiments.set_testRangeSample(true);
                    break;
                case 's':
                    querying_experiments.set_testRandomShuffle(true);
                    break;
                default:
                    std::cerr << "unknown option \'" << value << '\"' << std::endl;
                }
            }
            querying_experiments.perform_file_experiments_OSM();

            querying_experiments.set_fixk(10000);
            querying_experiments.perform_file_experiments_OSM();
        }
        else if (argument.substr(0, 17) == "query_file_geo_a=")
        {
            Query_tree_experiments querying_experiments;

            for (int i = 17; i < argument.size(); i++)
            {
                char value = tolower(argument[i]);
                switch (value)
                {
                case 'e':
                    querying_experiments.set_testEstimation(true);
                    break;
                case 'b':
                    querying_experiments.set_testBaseline(true);
                    break;
                case 'l':
                    querying_experiments.set_testLevelSample(true);
                    break;
                case 'r':
                    querying_experiments.set_testRangeSample(true);
                    break;
                case 's':
                    querying_experiments.set_testRandomShuffle(true);
                    break;
                default:
                    std::cerr << "unknown option \'" << value << '\"' << std::endl;
                }
            }
            querying_experiments.perform_file_experiments_GEO();

            querying_experiments.set_fixk(10000);
            querying_experiments.perform_file_experiments_GEO();
        }
        else if (argument.substr(0, 17) == "find_queries_geo=")
        {
            long count;
            long tolerance;
            sscanf(argument.substr(17).c_str(), "%li,%li", &count, &tolerance);

            auto file_out = new std::fstream{ "geo_queries.txt", std::fstream::out | std::fstream::app };
            auto source = Data_Source_Information::get_data_information(Geolife);

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using rtree_t = rtree::rtree <
                entry,
                sample_entry > ;
            std::shared_ptr<rtree_t> p_tree(new rtree_t(source->get_source_created(), false, false));


            int count_to_gen = 10;
            for (int i = 0; i < count_to_gen; i++)
            {
                size_t elements_counted = 0;
                mongo_types::box3d Q = utilities::get_query_region_with_element_count(p_tree, *source, count, tolerance, elements_counted);
                *file_out << "(" << std::setprecision(10) << Q.min_corner().get<0>() << "," << std::setprecision(10) << Q.min_corner().get<1>() << "," << std::setprecision(10) << Q.min_corner().get<2>()
                    << ")--(" << std::setprecision(10) << Q.max_corner().get<0>() << "," << std::setprecision(10) << Q.max_corner().get<1>() << "," << std::setprecision(10) << Q.max_corner().get<2>() << "),"
                    << count << "," << elements_counted << std::endl;
                std::cout << "." << std::flush;
            }

            file_out->close();
            delete file_out;
        }
        else if (argument.substr(0, 17) == "find_queries_osm=")
        {
            long count;
            long tolerance;
            sscanf(argument.substr(17).c_str(), "%li,%li", &count, &tolerance);

            auto file_out = new std::fstream{ "osm_queries.txt", std::fstream::out | std::fstream::app };
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using rtree_t = rtree::rtree <
                entry,
                sample_entry >;
            std::shared_ptr<rtree_t> p_tree(new rtree_t(source->get_source_created()));


            int count_to_gen = 10;
            for (int i = 0; i < count_to_gen; i++)
            {
                size_t elements_counted = 0;
                mongo_types::box3d Q = utilities::get_query_region_with_element_count(p_tree, *source, count, tolerance, elements_counted);
                *file_out << "(" << std::setprecision(10) << Q.min_corner().get<0>() << "," << std::setprecision(10) << Q.min_corner().get<1>() << "," << std::setprecision(10) << Q.min_corner().get<2>()
                    << ")--(" << std::setprecision(10) << Q.max_corner().get<0>() << "," << std::setprecision(10) << Q.max_corner().get<1>() << "," << std::setprecision(10) << Q.max_corner().get<2>() << "),"
                    << count << "," << elements_counted << std::endl;
                std::cout << "." << std::flush;
            }

            file_out->close();
            delete file_out;
        }
        else if (argument == "build_geo")
        {
            Build_tree_experiments building_experiments;

            building_experiments.perform_listed_experiments_GEO();
        }
        else if (argument == "aggragate_geo")
        {
            aggragate_experiments ag_exp;

            ag_exp.perform_listed_experiments_GEO();
        }
        else if (argument == "aggragate_osm")
        {
            aggragate_experiments ag_exp;

            ag_exp.perform_listed_experiments_OSM();
        }
        else if (argument == "build_osm")
        {
            Build_tree_experiments building_experiments;

            building_experiments.perform_listed_experiments_OSM();
        }
        else if (argument == "build_geo_single")
        {
            Build_tree_experiments building_experiments;
            building_experiments.test_GeoLife(memory_use, 1);
        }
        else if (argument == "build_osm_single")
        {
            Build_tree_experiments building_experiments;
            building_experiments.test_OSMData(memory_use, 1);
        }
        else if (argument == "modify_geo_file")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_GEO(0);
        }
        else if (argument == "modify_osm_file")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_OSM(0);
        }
        else if (argument == "modify_geo_file_baseline")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_GEO(1);
        }
        else if (argument == "modify_osm_file_baseline")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_OSM(1);
        }
        else if (argument == "modify_geo_file_lstree")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_GEO(2);
        }
        else if (argument == "modify_osm_file_lstree")
        {
            Insert_and_delete_tree_experiments in_del_experiments;
            in_del_experiments.perform_file_experiments_OSM(2);
        }
        else if (argument == "construct_only_geo_rtree")
        {
            std::cout << "building geo rtree" << std::endl;
            auto source = Data_Source_Information::get_data_information(Geolife);

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;
            rtree::rtree<
                entry,
                sample_entry,
                box
            >::build_io_layers(source->get_source_raw(), source->get_source_created(), source->get_convert_function(), memory_use);
            std::cout << "done building geo" << std::endl;
        }
        else if (argument == "construct_only_osm_rtree")
        {
            std::cout << "building osm rtree" << std::endl;
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;
            rtree::rtree<
                entry,
                sample_entry,
                box
            >::build_io_layers(source->get_source_raw(), source->get_source_created(), source->get_convert_function(), memory_use);
            std::cout << "done building osm" << std::endl;
        }
        else if (argument == "construct_only_geo_level")
        {
            std::cout << "building geo level tree" << std::endl;
            auto source = Data_Source_Information::get_data_information(Geolife);

            //constexpr size_t NODE_SAMPLE_SIZE = 256;
            constexpr size_t MAX_FANOUT = 16;

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;

            rtree::IOLayersParameters parameters;
            //parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
            //parameters.max_top_layer_io_node_count /= MAX_FANOUT;

            level_sampling::level_sampling<entry, sample_entry>::build(source->get_source_raw(), source->get_source_level_created(), source->get_convert_function(), memory_use, parameters);

        }
        else if (argument == "construct_only_osm_level")
        {
            std::cout << "building osm level tree" << std::endl;
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

           // constexpr size_t NODE_SAMPLE_SIZE = 256;
            constexpr size_t MAX_FANOUT = 16;

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;

            rtree::IOLayersParameters parameters;
            //parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
            //parameters.max_top_layer_io_node_count /= MAX_FANOUT;

            level_sampling::level_sampling<entry, sample_entry>::build(source->get_source_raw(), source->get_source_level_created(), source->get_convert_function(), memory_use, parameters);

        }
        else if (argument == "construct_only_geo_shuffle")
        {
            auto source = Data_Source_Information::get_data_information(Geolife);

            using entry = mongo_types::entry;

            RandomShuffle<entry>::Parameters parameters;
            RandomShuffle<entry>::build(source->get_source_raw(), source->get_source_shuffle_created(), source->get_convert_function(), parameters, memory_use);
        }
        else if (argument == "construct_only_osm_shuffle")
        {
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

            using entry = mongo_types::entry;

            RandomShuffle<entry>::Parameters parameters;
            RandomShuffle<entry>::build(source->get_source_raw(), source->get_source_shuffle_created(), source->get_convert_function(), parameters, memory_use);
        }
        else if (argument == "construct_sample_node_structure")
        {
            auto source = Data_Source_Information::get_data_information(Geolife);
            const int DEFAULT_NODESAMPLESIZE = 256;
            const int DEFAULT_IO_NODE_COUNT = 10000;

            const float scale_16 = 16.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 16" << std::endl;
            rtree::IOLayersParameters test_16_parameters;
            test_16_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_16;
            Vary_sample_buffer<16> test_16;
            test_16.build_structure(*source, test_16_parameters);


            const float scale_32 = 32.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 32" << std::endl;
            rtree::IOLayersParameters test_32_parameters;
            test_32_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_32;
            Vary_sample_buffer<32> test_32;
            test_32.build_structure(*source, test_32_parameters);

            const float scale_64 = 64.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 64" << std::endl;
            rtree::IOLayersParameters test_64_parameters;
            test_64_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_64;
            Vary_sample_buffer<64> test_64;
            test_64.build_structure(*source, test_64_parameters);

            const float scale_128 = 128.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 128" << std::endl;
            rtree::IOLayersParameters test_128_parameters;
            test_128_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_128;
            Vary_sample_buffer<128> test_128;
            test_128.build_structure(*source, test_128_parameters);

            const float scale_256 = 256.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 256" << std::endl;
            rtree::IOLayersParameters test_256_parameters;
            test_256_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_256;
            Vary_sample_buffer<256> test_256;
            test_256.build_structure(*source, test_256_parameters);

            const float scale_512 = 512.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 512" << std::endl;
            rtree::IOLayersParameters test_512_parameters;
            test_512_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_512;
            Vary_sample_buffer<512> test_512;
            test_512.build_structure(*source, test_512_parameters);

            const float scale_4096 = 4096.0F / DEFAULT_NODESAMPLESIZE;
            std::cout << "creating structure for 4096" << std::endl;
            rtree::IOLayersParameters test_4096_parameters;
            test_4096_parameters.max_top_layer_io_node_count = DEFAULT_IO_NODE_COUNT / scale_4096;
            Vary_sample_buffer<4096> test_4096;
            test_4096.build_structure(*source, test_4096_parameters);
        }
        else if (argument == "run_sample_node_structure")
        {
            std::cout << "starting experiment for 16" << std::endl;
            auto test_16 = new Vary_sample_buffer<16>();
            test_16->perform_file_experiments_GEO();
            delete test_16;

            std::cout << "starting experiment for 32" << std::endl;
            auto test_32 = new Vary_sample_buffer<32>();
            test_32->perform_file_experiments_GEO();
            delete test_32;

            std::cout << "starting experiment for 64" << std::endl;
            auto test_64 = new Vary_sample_buffer<64>();
            test_64->perform_file_experiments_GEO();
            delete test_64;

            std::cout << "starting experiment for 128" << std::endl;
            auto test_128 = new Vary_sample_buffer<128>();
            test_128->perform_file_experiments_GEO();
            delete test_128;

            std::cout << "starting experiment for 256" << std::endl;
            auto test_256 = new Vary_sample_buffer<256>();
            test_256->perform_file_experiments_GEO();
            delete test_256;

            std::cout << "starting experiment for 512" << std::endl;
            auto test_512 = new Vary_sample_buffer<512>();
            test_512->perform_file_experiments_GEO();
            delete test_512;

            std::cout << "starting experiment for 4096" << std::endl;
            auto test_4096 = new Vary_sample_buffer<4096>();
            test_4096->perform_file_experiments_GEO();
            delete test_4096;
           
        }
        else if (argument.substr(0, 21) == "build_geo_block_size=")
        {
            size_t block_size = atoi(argument.substr(21).c_str());
            auto source = Data_Source_Information::get_data_information(Geolife);

            rtree::IOLayersParameters parameters;
           // constexpr size_t NODE_SAMPLE_SIZE = 256;
            constexpr size_t MAX_FANOUT = 16;

            //parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
            //parameters.max_top_layer_io_node_count /= MAX_FANOUT;
            //parameters.max_top_layer_io_node_count *= 5; // some arbitrary scale value
            parameters.max_top_layer_io_node_count = 1024;
            parameters.block_size = block_size;

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;
            rtree::rtree<
                entry,
                sample_entry,
                box
            >::build_io_layers(source->get_source_raw(),
                source->get_source_created() + "_blockSize-" + argument.substr(21).c_str(),
                source->get_convert_function(),
                memory_use,
                nullptr,
                parameters);
        }
        else if (argument.substr(0, 21) == "build_osm_block_size=")
        {
            size_t block_size = atoi(argument.substr(21).c_str());
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

            rtree::IOLayersParameters parameters;
           // constexpr size_t NODE_SAMPLE_SIZE = 256;
            constexpr size_t MAX_FANOUT = 16;

            //parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
            //parameters.max_top_layer_io_node_count /= MAX_FANOUT;
            //parameters.max_top_layer_io_node_count *= 5; // some arbitrary scale value
            parameters.max_top_layer_io_node_count = 1024;
            parameters.block_size = block_size;

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;
            rtree::rtree<
                entry,
                sample_entry,
                box
            >::build_io_layers(source->get_source_raw(),
            source->get_source_created() + "_blockSize-" + argument.substr(21).c_str(),
            source->get_convert_function(),
            memory_use,
            nullptr,
            parameters);
        }
        else if (argument.substr(0, 21) == "build_mem_nodes_file=")
        {
            std::string filename = argument.substr(21);
            //auto source = Data_Source_Information::get_data_information(Geolife);

            std::cout << "building mem nodes on file " << filename << std::endl;

            rtree::IOLayersParameters parameters;

            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;

            using rtree_t = rtree::rtree <
                entry,
                sample_entry,
                box
            >;
            std::unique_ptr<rtree_t> rtree_ptr;

            rtree_ptr.reset(new rtree_t(filename));

            rtree_ptr->save_mem_nodes();

        }
        else if (argument == "extra")
        {
            auto source = Data_Source_Information::get_data_information(OSM_nodes);

           // constexpr size_t NODE_SAMPLE_SIZE = 256;
            constexpr size_t MAX_FANOUT = 16;

            rtree::IOLayersParameters parameters;
            parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
            parameters.max_top_layer_io_node_count /= MAX_FANOUT;

            bt::cpu_timer rtree_build_timer;
            using entry = mongo_types::entry;
            using sample_entry = mongo_types::sample_entry;
            using point = mongo_types::point3d;
            using box = mongo_types::box3d;
            using rtree_t = rtree::rtree <
                entry,
                sample_entry,
                box
            >;

            bt::cpu_timer samples_build_timer;
            std::unique_ptr<rtree_t> mp_rtree_ptr;
            mp_rtree_ptr.reset(new rtree_t(source->get_source_created()));
            samples_build_timer.stop();

            std::cout << "tree type\ttime to build" << std::endl;
            std::cout << "rstree\t" << (samples_build_timer.elapsed().wall / (1000.0 + 1000 + 1000)) << std::endl;
        }
        else if (argument == "filter_osm")
        {
            // open unfiltered osm
            std::fstream in_file(Data_Source_Information::get_data_information(OSM_nodes)->get_source_raw(), std::ios_base::in);

            // open an output osm
            std::fstream out_file("./raw/filtered.osm", std::ios_base::out);

            long element_count{};
            std::string str;
            char x_val[16];
            char y_val[16];
            char DATEstring[32];
            char uid[16];
            long OIDvalue;

            char output[256];

            while (!in_file.eof())
            {
                if (element_count % 1000000 == 0)
                {
                    std::cout << element_count << '\n';
                }

                // TODO: optimize the copying of data in this loop.
                std::getline(in_file, str);

                // empty string are considered null, so don't enter any data from a zero length string
                if (str.size() == 0)
                    continue;
                
                sscanf(str.c_str(), "%16s\t%16s\t%16s\t%30s\t%ld", x_val, y_val, uid, DATEstring, &OIDvalue);

                // output will be lat   lon uid date    oid
                snprintf(output, 255, "%024ld\t%s\t%s\t%s\t%s", OIDvalue, x_val, y_val, DATEstring, uid);
                //snprintf(output, 255, "%024ld", OIDvalue);

                out_file << output << '\n';

                //out_file << x_val << '\t' << y_val << '\t' << uid << '\t' << time_since_epoc.length().total_seconds() << '\t' << OIDvalue << '\n';

                ++element_count;
            }
            // read every line, convert and write our converted value for each record

        }
        else if (argument.substr(0, 20) == "independence_static=")
        {
            Independence_Experiment experiment(10000000, 1000000);
            int trials = 100000;

            for (int i = 20; i < argument.size(); i++)
            {
                char value = tolower(argument[i]);
                switch (value)
                {
                case 'e':
                    experiment.run_experiment("static_independence_rstree.txt", experiment.SAMPLE, trials);
                    break;
                case 'b':
                    experiment.run_experiment("static_independence_baseline.txt", experiment.NAIVE, trials);
                    break;
                case 'l':
                    experiment.run_experiment("static_independence_level.txt", experiment.LEVEL_SAMPLE, trials);
                    break;
                case 'i':
                    experiment.run_experiment("static_independence_iid.txt", experiment.IID, trials);
                    break;
                default:
                    std::cerr << "unknown option \'" << value << '\"' << std::endl;
                }
            }
        }
        else if (argument.substr(0, 19) == "latency_experiment=")
        {
            for (int i = 19; i < argument.size(); i++)
            {
                std::unique_ptr<query_latency_experiment> experiment;

                char value = tolower(argument[i]);
                switch (value)
                {
                case 'e':
                    experiment.reset(new query_latency_experiment("sampling_creation_latency.txt", "sampling_sampling_latency.txt", Geolife, query_latency_experiment::SAMPLE));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'b':
                    experiment.reset(new query_latency_experiment("baseline_creation_latency.txt", "baseline_sampling_latency.txt", Geolife, query_latency_experiment::NAIVE));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'l':
                    experiment.reset(new query_latency_experiment("level_creation_latency.txt", "level_sampling_latency.txt", Geolife, query_latency_experiment::LEVEL_SAMPLE));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'i':
                    experiment.reset(new query_latency_experiment("iid_creation_latency.txt", "iid_sampling_latency.txt", Geolife, query_latency_experiment::IID));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'm':
                    experiment.reset(new query_latency_experiment("iidmem_creation_latency.txt", "iidmem_sampling_latency.txt", Geolife, query_latency_experiment::IID_MEM));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'n':
                    experiment.reset(new query_latency_experiment("iidmemleaf_creation_latency.txt", "iidmemleaf_sampling_latency.txt", Geolife, query_latency_experiment::IID_MEMLEAF));
                    experiment->run_experiment(10, 10000);
                    break;
                case 'r':
                    experiment.reset(new query_latency_experiment("range_creation_latency.txt", "range_sampling_latency.txt", Geolife, query_latency_experiment::RANGE_REPORT));
                    experiment->run_experiment(10, 10000);
                    break;
                default:
                    std::cerr << "unknown option \'" << value << '\"' << std::endl;
                }
            }
        }
        else if (argument == "test") {
            auto source = Data_Source_Information::get_data_information(Geolife);

            Independence_Experiment experiment(1000000, 10000);

            experiment.run_experiment(10000);
        }
        else
        {
            std::cerr << "unknown option \'" << argument << "\'" << std::endl;
        }
    }


    //    std::cerr << "need 1 argument <filename>" << std::endl;
    //    return -1;
    //}

    //std::string filename = argv[1];

    //Exp(filename+"-rtree", filename+"-ls").run();

    return 0;
}
