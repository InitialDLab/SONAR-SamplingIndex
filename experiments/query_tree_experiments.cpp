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
#include "query_tree_experiments.h"

#include <vector>
#include <random>
#include <algorithm>

#include <boost/timer/timer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "../mongo_types.h"
#include "../rtree/rtree.h"
#include "../random_shuffle.h"

#include "Data_Source_Information.h"
#include "experiment_utilities.h"

namespace bt = boost::timer;

// parameters which express the maximum number of samples that should be sampled
// from a region.  If the number of samples expected in a region > max_sample_request,
// we will only take max_sample_request.  The amount we will sample will be proportional
// to the max sample percent (where the max_sample_percent should be considered sampling
// the max_sample_request)
const float max_sample_percent = 0.25f;
size_t memory_limit = 4L * 1024L * 1024L * 1024L;


void Query_tree_experiments::perform_file_experiments_GEO()
{
    query_sample_frequency = 100;
    auto data_source = Data_Source_Information::get_data_information(Geolife);
    std::string filename = "geo_queries.txt";
    test_experiments_from_file(filename, *data_source);
}

void Query_tree_experiments::perform_file_experiments_OSM()
{
    query_sample_frequency = 1000;
    auto data_source = Data_Source_Information::get_data_information(OSM_nodes);
    std::string filename = "osm_queries.txt";
    test_experiments_from_file(filename, *data_source);
}



void Query_tree_experiments::test_experiments_from_file(std::string filename, Data_Source_Information& data_info)
{
    //open the rtree
    bool close_rtree = false;
    bool close_ltree = false;
    std::cout << "opening tree" << std::endl;
    if (!mp_rtree_ptr && this->get_usingRtree())
    {
        std::cout << "starting to open rtree" << std::endl;
        std::cout << "setting memory use for rtree to be " << memory_limit << "bytes" << std::endl;
        mp_rtree_ptr.reset(new rtree_t(data_info.get_source_created(), true /*unlimited memory if true?*/, false /*load using mem nodes*/, memory_limit /* memory limit */));

        // we want to be able to use the same amount of memory regardless of the block size.
        //  By default, the cache can hold 4096 blocks where the block size is 4096 bytes.
        //  we scale appropriately.
        // set some large value just to make it not unlimited, but almost unlimited
        //size_t cache_size = 1024 * 1024 * 128;
        //size_t cache_size = 4096 * 4096 / mp_rtree_ptr->get_block_manager().get_block_size();
        //std::cout << "setting cache size to " << cache_size << std::endl;
        //mp_rtree_ptr->get_block_manager().set_cache_capacity(cache_size);
        //mp_rtree_ptr->get_block_manager().set_cache_capacity(0);
        close_rtree = true;
    }
    if (!mp_levelSample_ptr && this->get_usingLeveltree())
    {
        std::cout << "starting to open level tree" << std::endl;
        std::cout << "setting memory use for level sampling to be " << memory_limit << "bytes" << std::endl;
        mp_levelSample_ptr.reset(new level_t(data_info.get_source_level_created(), data_info.get_min_level_sample(), memory_limit));
        close_ltree = true;
        std::cout << "level sampling tree open" << std::endl;
    }
    if (!mp_randomShuffle_ptr && this->get_usingRandomShuffle())
    {
        //size_t NODE_SAMPLE_SIZE = 16;
        size_t max_top_layer_io_count = 1024 * 1024;

        // the number of elements were found by scanning the raw dataset.
        size_t elements;
        switch (data_info.get_dataSource())
        {
        case Data_sources::Geolife:
            elements = 24876977;
            break;
        case Data_sources::OSM_nodes:
            elements = 2210524989;
            break;
        default:
            throw "Error";
        }

        size_t memory_use = memory_limit;// rtree_t::estimate_memory_usage(elements);

        std::cout << "setting memory use for random shuffle to be " << memory_use << "bytes" << std::endl;

        mp_randomShuffle_ptr.reset(new shuffle_t(data_info.get_source_shuffle_created(), memory_use));
    }
    std::cout << "trees open, starting experiments" << std::endl;

    // open the file
    auto file_in = new std::fstream{ filename.c_str() , std::fstream::in };

    std::string line;
    while (std::getline(*file_in, line))
    {
        if (line.size() == 0)
            continue;

        float x_min, y_min, t_min, x_max, y_max, t_max;
        long estimated_count;
        long actual_count;

        sscanf(line.c_str(), "(%f,%f,%f)--(%f,%f,%f),%ld,%ld", &x_min, &y_min, &t_min, &x_max, &y_max, &t_max, &estimated_count, &actual_count);

        mongo_types::box3d query;
        query.min_corner().set<0>(x_min);
        query.min_corner().set<1>(y_min);
        query.min_corner().set<2>(t_min);
        query.max_corner().set<0>(x_max);
        query.max_corner().set<1>(y_max);
        query.max_corner().set<2>(t_max);

        long k = m_fixedk;
        if (m_fixedk == 0)
            k = estimated_count / 10;
        //long k = 500000;

        if (m_testEstimation)
            test_est(k, query, estimated_count, data_info, 0.0, actual_count);
        if (m_testBaseline)
            test_baseline(k, query, estimated_count, data_info, 0.0, actual_count);
        if (m_testRangeSample)
            test_rangeReport(k, query, estimated_count, data_info, 0.0, actual_count);
        if (m_testLevelSample)
            test_levelSample(k, query, estimated_count, data_info, 0.0, actual_count);
        if (m_testRandomShuffle)
            test_randomShuffle(k, query, estimated_count, data_info, 0.0, actual_count);
        std::cout << '.' << std::flush;
    }

    file_in->close();
    delete file_in;

    //if (close_rtree)
    //    mp_rtree_ptr.release();
    //if (close_ltree)
    //    mp_levelSample_ptr.release();
}

void Query_tree_experiments::test_est(long k,
                                      mongo_types::box3d query_box, 
                                      long q_group,
                                      Data_Source_Information& data_info,
                                      float sample_percentage_max,
                                      size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    //utilities::null_iterator<mongo_types::sample_entry> iter;

    if (!m_testInMem)
        this->clear_system_cache();

    //auto cursor = mp_rtree_ptr->sample_query(query_box);
    bt::cpu_timer query_timer;
    query_timer.stop();
    //for (long i = std::min(query_sample_frequency, k);
    //    i <= k; i += query_sample_frequency)
    for (long i = 1; i <= 4; i+=1)
    {
        utilities::null_iterator<mongo_types::sample_entry> iter;


        auto cursor = mp_rtree_ptr->sample_query(query_box);

        long requests = i * k * 25 / 100;

        ////
        bt::cpu_timer query_timer;
        //query_timer.resume();
        cursor.get_samples(requests, iter);
        query_timer.stop();
        ////


        float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

        query_results results;
        results.inputFile = data_info.get_source_created();
        results.method_name = "sample";
        results.start_time = start_time;
        results.k = requests;
        results.q_exact = element_count_in_region;
        results.q_group = q_group;
        results.query_box = query_box;
        results.query_time_sec = time;
        results.samples_returned = iter.get_count();
        results.io_cost = cursor.get_io_cost();
        results.block_size = mp_rtree_ptr->get_block_manager().get_block_size();
        results.cache_size = mp_rtree_ptr->get_block_manager().get_cache_size();
        results.cursor_stats = cursor.get_stats();

        m_IOUsed = m_IOUsed || cursor.get_io_cost();

        write_experiment_entry(results);
    }
}

void Query_tree_experiments::test_baseline(long k,
                                           mongo_types::box3d query_box,
                                           long q_group,
                                           Data_Source_Information& data_info,
                                           float sample_percentage_max,
                                           size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    if (!m_testInMem)
        this->clear_system_cache();

    utilities::null_iterator<mongo_types::sample_entry> iter;

    auto cursor = mp_rtree_ptr->naive_sample_query(query_box);
    bt::cpu_timer query_timer;
    query_timer.stop();
    for (long i = std::min(query_sample_frequency, k);
        i <= k; i += query_sample_frequency)
    {

        ////
        query_timer.resume();
        cursor.get_samples(query_sample_frequency, iter);
        query_timer.stop();
        ////

        float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

        query_results results;
        results.inputFile = data_info.get_source_created();
        results.method_name = "baseline";
        results.start_time = start_time;
        results.k = i;
        results.q_exact = element_count_in_region;
        results.q_group = q_group;
        results.query_box = query_box;
        results.query_time_sec = time;
        results.samples_returned = iter.get_count();;
        results.io_cost = cursor.get_io_cost();
        results.block_size = mp_rtree_ptr->get_block_manager().get_block_size();
        results.cache_size = mp_rtree_ptr->get_block_manager().get_cache_size();

        m_IOUsed = m_IOUsed || cursor.get_io_cost();

        write_experiment_entry(results);
    }
}

void Query_tree_experiments::test_rangeReport(long k,
                                              mongo_types::box3d query_box,
                                              long q_group,
                                              Data_Source_Information& data_info,
                                              float sample_percentage_max,
                                              size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    size_t samples_returned;

    bt::cpu_timer query_timer;
    query_timer.stop();

    if (!m_testInMem)
        this->clear_system_cache();

    utilities::null_iterator<mongo_types::sample_entry> iter;

    size_t initial_io_cost = mp_rtree_ptr->get_block_manager().get_stats().cost();

    ////
    query_timer.resume();
    auto stats = mp_rtree_ptr->range_report(query_box, iter);
    //cursor.get_samples(query_sample_frequency, iter);
    query_timer.stop();
    ////

    size_t final_io_cost = mp_rtree_ptr->get_block_manager().get_stats().cost();

    float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

    query_results results;
    results.inputFile = data_info.get_source_created();
    results.method_name = "range_report";
    results.start_time = start_time;
    results.k = k;
    results.q_exact = element_count_in_region;
    results.q_group = q_group;
    results.query_box = query_box;
    results.query_time_sec = time;
    results.samples_returned = iter.get_count();;
    results.io_cost = final_io_cost - initial_io_cost;
    results.block_size = mp_rtree_ptr->get_block_manager().get_block_size();
    results.cache_size = mp_rtree_ptr->get_block_manager().get_cache_size();

    m_IOUsed = m_IOUsed || results.io_cost;


    write_experiment_entry(results);
}

void Query_tree_experiments::test_levelSample(long k,
                                              mongo_types::box3d query_box,
                                              long q_group,
                                              Data_Source_Information& data_info,
                                              float sample_percentage_max,
                                              size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    if (!m_testInMem)
        this->clear_system_cache();
    //utilities::null_iterator<mongo_types::sample_entry> iter;


    auto cursor = mp_levelSample_ptr->sample_query(query_box);
    bt::cpu_timer query_timer;
    query_timer.stop();
    //for (long i = std::min(query_sample_frequency, k);
    //    i <= k; i += query_sample_frequency)
    //{
    //    ////
    //    query_timer.resume();
    //    cursor.get_samples(query_sample_frequency, iter);
    //    query_timer.stop();
        ////
    for (long i = 1; i <= 4; i += 1)
    {
        utilities::null_iterator<mongo_types::sample_entry> iter;


        auto cursor = mp_rtree_ptr->sample_query(query_box);

        long requests = i * k * 25 / 100;

        ////
        bt::cpu_timer query_timer;
        //query_timer.resume();
        cursor.get_samples(requests, iter);
        query_timer.stop();
        ////


        float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

        query_results results;
        results.inputFile = data_info.get_source_created();
        results.method_name = "level_sample";
        results.start_time = start_time;
        results.k = requests;
        results.q_exact = element_count_in_region;
        results.q_group = q_group;
        results.query_box = query_box;
        results.query_time_sec = time;
        results.samples_returned = iter.get_count();;
        results.io_cost = cursor.get_io_cost();
        results.block_size = 0;
        results.cache_size = 0;

        m_IOUsed = m_IOUsed || cursor.get_io_cost();

        write_experiment_entry(results);
    }
}

void Query_tree_experiments::test_randomShuffle(long k,
                                                mongo_types::box3d query_box,
                                                long q_group,
                                                Data_Source_Information& data_info,
                                                float sample_percentage_max,
                                                size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    if (!m_testInMem)
        this->clear_system_cache();
    utilities::null_iterator<mongo_types::sample_entry> iter;


    auto cursor = mp_randomShuffle_ptr->sample_query(query_box);
    bt::cpu_timer query_timer;
    query_timer.stop();
    for (long i = std::min(query_sample_frequency, k);
        i <= k; i += query_sample_frequency)
    {
        ////
        query_timer.resume();
        cursor.get_samples(query_sample_frequency, iter);
        query_timer.stop();
        ////


        float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

        query_results results;
        results.inputFile = data_info.get_source_shuffle_created();
        results.method_name = "random_shuffle";
        results.start_time = start_time;
        results.k = i;
        results.q_exact = element_count_in_region;
        results.q_group = q_group;
        results.query_box = query_box;
        results.query_time_sec = time;
        results.samples_returned = iter.get_count();;
        results.io_cost = cursor.get_stats().blocks_read;
        results.block_size = -1;
        results.cache_size = -1;

        m_IOUsed = m_IOUsed || cursor.get_stats().blocks_read;

        write_experiment_entry(results);
    }
}

void Query_tree_experiments::open_outputfile()
{
    m_output.reset(new std::fstream{ m_outputFilename, std::fstream::out | std::fstream::app });

    if (!m_output->tellp())
    {
        write_header();
    }
}

void Query_tree_experiments::clear_system_cache()
{
    if (!m_IOUsed)
        return;
    // this uses a custom utility to clear the system cache
    if (mp_rtree_ptr)
        mp_rtree_ptr->flush_cache();
    if (mp_levelSample_ptr)
        mp_levelSample_ptr->flush_cache();
    auto unused = system("./clearCache > /dev/null");
    m_IOUsed = false;
    std::cout << "C";
}

void Query_tree_experiments::write_header()
{
    *m_output << "input file"                 << m_dataSeperator
              << "Start Time"                 << m_dataSeperator
              << "method name"                << m_dataSeperator
              << "k"                          << m_dataSeperator
              << "actual |Q|"                 << m_dataSeperator
              << "expected |Q|"               << m_dataSeperator
              << "query box min"              << m_dataSeperator
              << "query box max"              << m_dataSeperator
              << "query time"                 << m_dataSeperator
              << "samples returned"           << m_dataSeperator
              << "io  cost"                   << m_dataSeperator
              << "block size"                 << m_dataSeperator
              << "cache size"                 << m_dataSeperator
              << "rtree_internal_nodes"            << m_dataSeperator
              << "rtree_leaf_nodes"            << m_dataSeperator
              << "rtree_io_internal_nodes"     << m_dataSeperator
              << "rtree_io_leaf_nodes"            << m_dataSeperator
              << "rtree_io_sample_nodes"          << m_dataSeperator
              << std::endl;
}

void Query_tree_experiments::write_experiment_entry(query_results results)
{
    *m_output << results.inputFile                         << m_dataSeperator
              << results.start_time                        << m_dataSeperator
              << results.method_name                       << m_dataSeperator
              << results.k                                 << m_dataSeperator
              << results.q_exact                           << m_dataSeperator
              << results.q_group                           << m_dataSeperator
              << '(' << results.query_box.min_corner().get<0>() << ',' << results.query_box.min_corner().get<1>() << ',' << results.query_box.min_corner().get<2>() << ')' << m_dataSeperator
              << '(' << results.query_box.max_corner().get<0>() << ',' << results.query_box.max_corner().get<1>() << ',' << results.query_box.max_corner().get<2>() << ')' << m_dataSeperator
              << results.query_time_sec                    << m_dataSeperator
              << results.samples_returned                  << m_dataSeperator
              << results.io_cost                           << m_dataSeperator
              << results.block_size                        << m_dataSeperator
              << results.cache_size                        << m_dataSeperator
              << results.cursor_stats.internal_nodes << m_dataSeperator
              << results.cursor_stats.leaf_nodes << m_dataSeperator
              << results.cursor_stats.io_internal_nodes << m_dataSeperator
              << results.cursor_stats.io_leaf_nodes << m_dataSeperator
              << results.cursor_stats.io_sample_nodes << m_dataSeperator
              << std::endl;
}
