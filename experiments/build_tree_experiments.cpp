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
#include "build_tree_experiments.h"

#include <iostream>
#include <fstream>
#include <functional>
#include <vector>

#include <boost/timer/timer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "../mongo_types.h"
#include "../rtree/rtree.h"


#include "Data_Source_Information.h"

namespace bt = boost::timer;

constexpr int ns_per_ms = 1000;
constexpr int ns_per_sec = ns_per_ms * 1000;

void Build_tree_experiments::perform_listed_experiments_GEO()
{
    std::cout << "setting up build experiments for GEO" << std::endl;

    int iterations = 2;
    auto data_source = Data_Source_Information::get_data_information(Geolife);

    std::vector<uint64_t> max_tpie_memory_list = { 1UL * (1024L * 1024 * 1024),
                                                 2UL * (1024L * 1024 * 1024),
                                                 4UL * (1024L * 1024 * 1024),
                                                 6UL * (1024L * 1024 * 1024),
                                                 8UL * (1024L * 1024 * 1024),
                                                 1UL * (1024L * 1024 * 512),
                                                 3UL * (1024L * 1024 * 512),
                                                 5UL * (1024L * 1024 * 512),
                                                 3UL * (1024L * 1024 * 1024),
                                                 1UL * (1024L * 1024 * 1024),
                                                 2UL * (1024L * 1024 * 1024),
                                                 4UL * (1024L * 1024 * 1024),
                                                 6UL * (1024L * 1024 * 1024),
                                                 8UL * (1024L * 1024 * 1024),
                                                 1UL * (1024L * 1024 * 512),
                                                 3UL * (1024L * 1024 * 512),
                                                 5UL * (1024L * 1024 * 512),
                                                 3UL * (1024L * 1024 * 1024)
                                                };

    std::cout << "starting experiment" << std::endl;

    for (auto current_memory_max : max_tpie_memory_list)
    {
        this->test_GeoLife(current_memory_max, iterations);
        std::cout << '.' << std::flush;
    }

    std::cout << "done with build experiment for GEO" << std::endl;
}

void Build_tree_experiments::perform_listed_experiments_OSM()
{
    std::cout << "setting up query tree experiments for OSM" << std::endl;

    int iterations = 2;
    auto data_source = Data_Source_Information::get_data_information(OSM_nodes);

    std::vector<size_t> max_tpie_memory_list = { 60UL * (1024UL * 1024 * 1024),
                                                 52UL * (1024UL * 1024 * 1024),
                                                 42UL * (1024UL * 1024 * 1024),
                                                 32UL * (1024UL * 1024 * 1024),
                                                 24UL * (1024UL * 1024 * 1024),
                                                 16UL * (1024UL * 1024 * 1024),
                                                 12UL * (1024UL * 1024 * 1024),
                                                  8UL * (1024UL * 1024 * 1024),
                                                  6UL * (1024UL * 1024 * 1024),
                                                  4UL * (1024UL * 1024 * 1024),
                                                  2UL * (1024UL * 1024 * 1024) };
        

    std::cout << "starting experiment" << std::endl;

    for (auto current_memory_max : max_tpie_memory_list)
    {
        this->test_OSMData(current_memory_max, iterations);
        std::cout << '.' << std::flush;
    }

    std::cout << "done with build experiment for OSM" << std::endl;
}


void Build_tree_experiments::test_GeoLife(size_t max_tpie_memory, int experiment_iterations)
{
    using entry        = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point        = mongo_types::point3d;
    using box          = mongo_types::box3d;

    auto data_source = Data_Source_Information::get_data_information(Geolife);

    constexpr int NODESAMPLESIZE = 256;
    constexpr int LEAFSIZE = 256;
    constexpr int INTERNALSIZE = 16;
    const std::string GeoLifeInput{ data_source->get_source_raw() };
    const std::string GeoLifeOutput{ data_source->get_source_created() };

    // geolife converter to map into the mongodb data type inside rtree
    // NOTE: the data structures in mongo types requires a OID.  Since the
    // geolife data is not dumped from a mongo database, we have assigned
    // each entry in the input file a unique OID.
    std::function<entry(const std::string&)> GeoLife_converter = data_source->get_convert_function();

    for (int i = 0; i < experiment_iterations; ++i)
    {
        rtree::IOLayerBuildStatistics build_stats;

        boost::posix_time::ptime start_date{ boost::posix_time::second_clock::local_time() };
        bt::cpu_timer build_timer;
        std::fstream in_file(GeoLifeInput.c_str(), std::ios_base::in);
        std::vector<entry> input_buffer;

        rtree::rtree<
            entry,
            sample_entry,
            box,
            NODESAMPLESIZE,
            LEAFSIZE
        >::build_io_layers(GeoLifeInput, GeoLifeOutput, GeoLife_converter, max_tpie_memory, &build_stats);
        build_timer.stop();

        using rtree_t = rtree::rtree <
            entry,
            sample_entry,
            box,
            NODESAMPLESIZE,
            LEAFSIZE
        > ;
        std::unique_ptr<rtree_t> rtree_ptr;
        rtree_ptr.reset(new rtree_t(GeoLifeOutput));

        write_experiment_entry(GeoLifeInput,
            max_tpie_memory,
            start_date,
            data_source->get_element_count(),
            build_timer.elapsed().wall / (1000.0 * 1000 * 1000),
            rtree_ptr->get_stats().io_internal_nodes,
            rtree_ptr->get_stats().leaf_nodes,
            data_source->get_readTime(),
            build_stats);
    }
}

void Build_tree_experiments::test_OSMData(size_t max_tpie_memory, int experiment_iterations)
{
    using entry        = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point        = mongo_types::point3d;
    using box          = mongo_types::box3d;

    auto data_source = Data_Source_Information::get_data_information(OSM_nodes);

    constexpr int NODESAMPLESIZE = 256;
    constexpr int LEAFSIZE = 256;
    constexpr int INTERNALSIZE = 16;
    const std::string OSMDataInput{ data_source->get_source_raw() };
    const std::string OSMDataOutput{ data_source->get_source_created() };

    // OSM converter to map into the mongodb data type inside rtree
    // NOTE: the data structures in mongo types requires a OID.  Since the
    // OSM data is not dumped from a mongo database, we have assigned
    // each entry in the input file a unique OID from the map id value.
    std::function<entry(const std::string&)> OSMData_converter = data_source->get_convert_function();

    for (int i = 0; i < experiment_iterations; ++i)
    {
        rtree::IOLayerBuildStatistics build_stats;

        boost::posix_time::ptime start_date{ boost::posix_time::second_clock::local_time() };
        bt::cpu_timer build_timer;
        std::fstream in_file(OSMDataInput.c_str(), std::ios_base::in);
        std::vector<entry> input_buffer;

        rtree::rtree<
            entry,
            sample_entry,
            box,
            NODESAMPLESIZE,
            LEAFSIZE
        >::build_io_layers(OSMDataInput, OSMDataOutput, OSMData_converter, max_tpie_memory, &build_stats);
        build_timer.stop();

        using rtree_t = rtree::rtree <
            entry,
            sample_entry,
            box,
            NODESAMPLESIZE,
            LEAFSIZE
        > ;
        std::unique_ptr<rtree_t> rtree_ptr;
        rtree_ptr.reset(new rtree_t(OSMDataOutput));

        write_experiment_entry(OSMDataInput,
            max_tpie_memory,
            start_date,
            data_source->get_element_count(),
            build_timer.elapsed().wall / (1000.0 * 1000 * 1000),
            rtree_ptr->get_stats().internal_nodes,
            rtree_ptr->get_stats().leaf_nodes,
            data_source->get_readTime(),
            build_stats);
    }
}

void Build_tree_experiments::open_outputfile()
{
    m_output.reset(new std::fstream{ m_outputFilename, std::fstream::out | std::fstream::app});
    //m_output->seekp(0, ios_base::end);

    if (!m_output->tellp())
    {
        write_header();
    }
}

void Build_tree_experiments::write_experiment_entry(std::string inputFile,
    size_t tpie_memory,
    boost::posix_time::ptime start_time,
    size_t element_count,
    double build_time,
    int internal_nodes,
    int leaf_nodes,
    double read_time,
    rtree::IOLayerBuildStatistics internal_build_stats
    )
{
    *m_output << inputFile              << m_dataSeperator
        << tpie_memory                  << m_dataSeperator
        << start_time                   << m_dataSeperator
        << build_time                   << m_dataSeperator
        << element_count                << m_dataSeperator
        << (element_count / build_time) << m_dataSeperator
        << internal_nodes               << m_dataSeperator
        << leaf_nodes                   << m_dataSeperator
        << read_time                    << m_dataSeperator
        << (build_time - read_time)     << m_dataSeperator
        << element_count / (build_time - read_time)  << m_dataSeperator
        << internal_build_stats.read_time_sec << m_dataSeperator
        << internal_build_stats.sort_time_sec << m_dataSeperator
        << internal_build_stats.construct_time_sec << m_dataSeperator
        << internal_build_stats.node_construct_time_sec << m_dataSeperator
        << internal_build_stats.leaf_construct_time_sec //<< m_dataSeperator
        << std::endl;
}

void Build_tree_experiments::write_header()
{

    *m_output << "input file"          << m_dataSeperator
        << "max TPIE memory"           << m_dataSeperator
        << "Start Time"                << m_dataSeperator
        << "build time(s)"             << m_dataSeperator
        << "element count"             << m_dataSeperator
        << "elements inserted per sec" << m_dataSeperator
        << "internal node count"       << m_dataSeperator
        << "leaf node count"           << m_dataSeperator
        << "read time"                 << m_dataSeperator
        << "normalized build time"     << m_dataSeperator
        << "normalized elements per sec" << m_dataSeperator
        << "internal read time"        << m_dataSeperator
        << "internal sort time"        << m_dataSeperator
        << "internal construct time"   << m_dataSeperator
        << "internal node construct time" << m_dataSeperator
        << "internal leaf construct time" << m_dataSeperator
        << std::endl;

}