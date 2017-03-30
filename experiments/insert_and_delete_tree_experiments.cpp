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
#include "insert_and_delete_tree_experiments.h"

#include <fstream>
#include <vector>
#include <random>
#include <algorithm>

#include <boost/timer/timer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "../mongo_types.h"
#include "../rtree/rtree.h"

#include "experiment_utilities.h"

#include "Data_Source_Information.h"

bool debug = false;

namespace bt = boost::timer;

// NOTE: the percentage cover MUST be less than 1.0.  When running with a large cover
// I have ran into several issues.

const int sample_frequency = 50;
const int insertion_batch_size = 5000;


void Insert_and_delete_tree_experiments::perform_file_experiments_GEO(int method)
{
    auto data_source = Data_Source_Information::get_data_information(Geolife);
    std::string filename = "geo_queries.txt";
    test_experiments_from_file(filename, *data_source, method);
}

void Insert_and_delete_tree_experiments::perform_file_experiments_OSM(int method)
{
    auto data_source = Data_Source_Information::get_data_information(OSM_nodes);
    std::string filename = "osm_queries.txt";
    test_experiments_from_file(filename, *data_source, method);
}

void Insert_and_delete_tree_experiments::test_experiments_from_file(std::string filename, Data_Source_Information& data_info, int method)
{
    //open the rtree
    bool close_tree = false;
    if (method != 2)
        mp_rtree_ptr.reset(new rtree_t(data_info.get_source_created(), false, true));
    else
        mp_levelSample_ptr.reset(new level_t(data_info.get_source_level_created(), data_info.get_min_level_sample()));
    close_tree = true;

    // open the file
    auto file_in = new std::fstream{ filename.c_str(), std::fstream::in };

    long seed = 1;

    std::string line;
    while (std::getline(*file_in, line))
    {
        if (line.size() == 0)
            continue;

        float x_min, y_min, t_min, x_max, y_max, t_max;
        long estimated_count;
        long actual_count;

        sscanf(line.c_str(), "(%f,%f,%f)--(%f,%f,%f),%ld,%ld", &x_min, &y_min, &t_min, &x_max, &y_max, &t_max, &estimated_count, &actual_count);

        mongo_types::box3d region_Q;
        region_Q.min_corner().set<0>(x_min);
        region_Q.min_corner().set<1>(y_min);
        region_Q.min_corner().set<2>(t_min);
        region_Q.max_corner().set<0>(x_max);
        region_Q.max_corner().set<1>(y_max);
        region_Q.max_corner().set<2>(t_max);

        // generate input data

        const int iteration_count = 5;
        for (int i = 0; i<iteration_count; i++){
        seed++; // i manually change the seed so the random sequence is predictable
        std::vector<mongo_types::entry> insert_data = utilities::get_random_entry_vector(region_Q, insertion_batch_size, seed);

        if (method == 2)
            test_insert_and_delete_level(insert_data, region_Q, estimated_count, seed, data_info, method);
        else
            test_insert_and_delete_singleTree(insert_data, region_Q, estimated_count, seed, data_info, method);
        }

        std::cout << '.' << std::flush;
    }

    file_in->close();
    delete file_in;

    if (close_tree)
    {
        mp_rtree_ptr.release();
        mp_levelSample_ptr.release();
    }
}

void Insert_and_delete_tree_experiments::test_insert_and_delete_singleTree(std::vector<mongo_types::entry>& to_insert,
                                                                mongo_types::box3d region_Q,
                                                                size_t elements_in_region,
                                                                long seed,
                                                                Data_Source_Information& data_info,
                                                                int method)
{
    tree_results experiment_results;
    experiment_results.inputfile = data_info.get_source_created();
    experiment_results.k = to_insert.size();
    experiment_results.query_box = region_Q;
    experiment_results.elements_in_region = elements_in_region;
    experiment_results.seed = seed;
    experiment_results.method = method;
    experiment_results.start_time = boost::posix_time::second_clock::local_time();


    // do the insertion experiments for the data
    clear_system_cache();
    bt::cpu_timer insertion_timer;
    insertion_timer.stop();

    size_t insertion_initial_reads = 0;
    insertion_initial_reads = mp_rtree_ptr->get_block_manager().get_stats().cost();
    size_t index = 0;
    while (index < to_insert.size())
    {
        // insert a block of elements
        insertion_timer.resume();
        for (int inserted = 0; index < to_insert.size() && inserted < sample_frequency; index++, inserted++)
        {
            switch (method)
            {
            case 0:
                mp_rtree_ptr->insert<true>(to_insert[index]);
                break;
            case 1:
                mp_rtree_ptr->insert<false>(to_insert[index]);
                break;
            }
        }
        if (index == to_insert.size())
            mp_rtree_ptr->flush_cache();

        insertion_timer.stop();

        size_t insertion_final_reads = 0;
        insertion_final_reads = mp_rtree_ptr->get_block_manager().get_stats().cost();


        // pause the timer and dump the relevant data to the log file
        tree_results insert_tree_results = experiment_results;
        insert_tree_results.time = insertion_timer.elapsed().wall / (1000.0 * 1000 * 1000);
        insert_tree_results.io_read = insertion_final_reads - insertion_initial_reads;
        insert_tree_results.io_write = 0;
        insert_tree_results.k = index;

        write_insert_experiment_entry(insert_tree_results);
    }

    // do the erasing experiments for the data
    clear_system_cache();
    bt::cpu_timer erase_timer;
    erase_timer.stop();
    int erase_initial_reads = 0;
    erase_initial_reads = mp_rtree_ptr->get_block_manager().get_stats().cost();
    index = 0;
    while (index < to_insert.size())
    {
        erase_timer.resume();
        for (int erased = 0; index < to_insert.size() && erased < sample_frequency; index++, erased++)
        {
            switch (method)
            {
            case 0:
                mp_rtree_ptr->erase<true>(to_insert[index]);
                break;
            case 1:
                mp_rtree_ptr->erase<false>(to_insert[index]);
                break;
            }
        }
        if (index == to_insert.size())
            mp_rtree_ptr->flush_cache();
        erase_timer.stop();

        size_t erase_final_reads = 0;
        erase_final_reads = mp_rtree_ptr->get_block_manager().get_stats().cost();

        // pause the timer and dump the relevant data to the log file
        tree_results erase_tree_results = experiment_results;
        erase_tree_results.time = erase_timer.elapsed().wall / (1000.0 * 1000 * 1000);
        erase_tree_results.io_read = erase_final_reads - erase_initial_reads;
        erase_tree_results.io_write = 0;
        erase_tree_results.k = index;

        write_delete_experiment_entry(erase_tree_results);
    }
}

void Insert_and_delete_tree_experiments::test_insert_and_delete_level(std::vector<mongo_types::entry>& to_insert,
    mongo_types::box3d region_Q,
    size_t elements_in_region,
    long seed,
    Data_Source_Information& data_info,
    int method)
{
    tree_results experiment_results;
    experiment_results.inputfile = data_info.get_source_created();
    experiment_results.k = to_insert.size();
    experiment_results.query_box = region_Q;
    experiment_results.elements_in_region = elements_in_region;
    experiment_results.seed = seed;
    experiment_results.method = method;
    experiment_results.io_write = 0;
    experiment_results.start_time = boost::posix_time::second_clock::local_time();


    // do the insertion experiments for the data
    clear_system_cache();
    bt::cpu_timer insertion_timer;
    insertion_timer.stop();

    size_t insertion_initial_reads = 0;
    insertion_initial_reads = mp_levelSample_ptr->io_cost;
    size_t index = 0;
    while (index < to_insert.size())
    {
        // insert a block of elements
        insertion_timer.resume();
        for (int inserted = 0; index < to_insert.size() && inserted < sample_frequency; index++, inserted++)
        {
           mp_levelSample_ptr->insert(to_insert[index]);
        }
        if (index == to_insert.size())
            mp_levelSample_ptr->flush_cache();

        insertion_timer.stop();

        size_t insertion_final_reads = 0;
        insertion_final_reads = mp_levelSample_ptr->io_cost;

        // pause the timer and dump the relevant data to the log file
        tree_results insert_tree_results = experiment_results;
        insert_tree_results.time = insertion_timer.elapsed().wall / (1000.0 * 1000 * 1000);
        insert_tree_results.io_read = insertion_final_reads - insertion_initial_reads;
        insert_tree_results.io_write = 0;
        insert_tree_results.k = index;

        write_insert_experiment_entry(insert_tree_results);
    }

    // do the erasing experiments for the data
    clear_system_cache();
    bt::cpu_timer erase_timer;
    erase_timer.stop();
    int erase_initial_reads = 0;
    erase_initial_reads = mp_levelSample_ptr->io_cost;
    index = 0;
    while (index < to_insert.size())
    {
        erase_timer.resume();
        for (int erased = 0; index < to_insert.size() && erased < sample_frequency; index++, erased++)
        {
                mp_levelSample_ptr->erase(to_insert[index]);
        }
        if (index == to_insert.size())
            mp_levelSample_ptr->flush_cache();
            
        erase_timer.stop();

        size_t erase_final_reads = 0;
        erase_final_reads = mp_levelSample_ptr->io_cost;

        // pause the timer and dump the relevant data to the log file
        tree_results erase_tree_results = experiment_results;
        erase_tree_results.time = erase_timer.elapsed().wall / (1000.0 * 1000 * 1000);
        erase_tree_results.io_read = erase_final_reads - erase_initial_reads;
        erase_tree_results.io_write = 0;
        erase_tree_results.k = index;

        write_delete_experiment_entry(erase_tree_results);
    }
}

void Insert_and_delete_tree_experiments::clear_system_cache()
{
    // this uses a custom utility to clear the system cache
    if (mp_rtree_ptr)
        mp_rtree_ptr->flush_cache();
    if(mp_levelSample_ptr)
        mp_levelSample_ptr->flush_cache();
    auto unused = system("./clearCache > /dev/null");
}

void Insert_and_delete_tree_experiments::open_insert_outputfile()
{
    m_insert_output.reset(new std::fstream{ m_insert_outputFilename, std::fstream::out | std::fstream::app });

    if (!m_insert_output->tellp())
    {
        write_insert_header();
    }
}

void Insert_and_delete_tree_experiments::open_delete_outputfile()
{
    m_delete_output.reset(new std::fstream{ m_delete_outputFilename, std::fstream::out | std::fstream::app });

    if (!m_delete_output->tellp())
    {
        write_delete_header();
    }
}

void Insert_and_delete_tree_experiments::write_insert_header()
{
    *m_insert_output << "input file"                    << m_dataSeperator
                     << "Start Time"                    << m_dataSeperator
                     << "k"                             << m_dataSeperator
                     << "|Q|"                           << m_dataSeperator
                     << "seed"                          << m_dataSeperator
                     << "query box min"                 << m_dataSeperator
                     << "query box max"                 << m_dataSeperator
                     << "time"                          << m_dataSeperator
                     << "io read"                       << m_dataSeperator
                     << "io write"                      << m_dataSeperator
                     << "method"
                     << std::endl;
}

void Insert_and_delete_tree_experiments::write_delete_header()
{
    *m_delete_output << "input file"                << m_dataSeperator
                     << "Start Time"                << m_dataSeperator
                     << "k"                         << m_dataSeperator
                     << "|Q|"                       << m_dataSeperator
                     << "seed"                      << m_dataSeperator
                     << "query box min"             << m_dataSeperator
                     << "query box max"             << m_dataSeperator
                     << "time"                      << m_dataSeperator
                     << "io read"                   << m_dataSeperator
                     << "io write"                  << m_dataSeperator
                     << "method"
                     << std::endl;
}

void Insert_and_delete_tree_experiments::write_insert_experiment_entry(tree_results results)
{
    *m_insert_output << results.inputfile                            << m_dataSeperator
                     << results.start_time                           << m_dataSeperator
                     << results.k                                    << m_dataSeperator
                     << results.elements_in_region                   << m_dataSeperator
                     << results.seed                                 << m_dataSeperator
                       << '(' << results.query_box.min_corner().get<0>() << ',' << results.query_box.min_corner().get<1>() << ',' << results.query_box.min_corner().get<2>() << ')' << m_dataSeperator
                       << '(' << results.query_box.max_corner().get<0>() << ',' << results.query_box.max_corner().get<1>() << ',' << results.query_box.max_corner().get<2>() << ')' << m_dataSeperator
                     << results.time                                 << m_dataSeperator
                     << results.io_read                              << m_dataSeperator
                     << results.io_write                             << m_dataSeperator
                     << results.method
                     << std::endl;
}

void Insert_and_delete_tree_experiments::write_delete_experiment_entry(tree_results results)
{
    *m_delete_output << results.inputfile                            << m_dataSeperator
                     << results.start_time                           << m_dataSeperator
                     << results.k                                    << m_dataSeperator
                     << results.elements_in_region                   << m_dataSeperator
                     << results.seed                                 << m_dataSeperator
                       << '(' << results.query_box.min_corner().get<0>() << ',' << results.query_box.min_corner().get<1>() << ',' << results.query_box.min_corner().get<2>() << ')' << m_dataSeperator
                       << '(' << results.query_box.max_corner().get<0>() << ',' << results.query_box.max_corner().get<1>() << ',' << results.query_box.max_corner().get<2>() << ')' << m_dataSeperator
                     << results.time                                 << m_dataSeperator
                     << results.io_read                              << m_dataSeperator
                     << results.io_write                             << m_dataSeperator
                     << results.method
                     << std::endl;
}
