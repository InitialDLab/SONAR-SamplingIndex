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
#ifndef VARY_SAMPLE_BUFFER_H__
#define VARY_SAMPLE_BUFFER_H__

#include <fstream>
#include <string>
#include <memory>
#include <chrono>

#include "boost/date_time/posix_time/posix_time.hpp"

#include <boost/geometry/geometry.hpp>

#include <rtree/rtree.h>

#include "mongo_types.h"
#include "Data_Source_Information.h"

#include "experiment_utilities.h"

struct vary_sample_buffer_results
{
    std::string inputFile;
    std::string method_name;

    boost::posix_time::ptime start_time;

    long k;
    long q_exact;
    long q_group;
    mongo_types::box3d query_box;
    double query_time_sec;
    int samples_returned;
    size_t io_cost;
    int sample_size;
};

template<size_t t_NodeSampleSize = 256>
class Vary_sample_buffer
{
public:

    Vary_sample_buffer(std::string outputFile = "vary_sample_buffer.txt")
        :
        m_dataSeperator{ '\t' },
        m_outputFilename{ outputFile }
    {
        open_outputfile();
    }

    void build_structure(Data_Source_Information& data_info, rtree::IOLayersParameters parameters);

    virtual ~Vary_sample_buffer()
    {
        m_output->close();
    }

    void perform_file_experiments_GEO();

private:
    //void test_experiments_from_file(std::string filename, Data_Source_Information& data_info);

    void perform_query(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, size_t element_count_in_region);

    // return the file where this particular rs tree should reside
    // (the file name changes depending on the node sample size)
    std::string get_rstreeName(Data_Source_Information& data_info);

    void open_outputfile();
    void write_header();
    void write_experiment_entry(vary_sample_buffer_results results);

    void clear_system_cache();

    const std::string m_outputFilename;
    std::unique_ptr<std::fstream> m_output;
    char m_dataSeperator;

    // after this many elements are returned we record the statistics.
    long m_query_sample_frequency;

    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    using rtree_t = rtree::rtree <
        entry,
        sample_entry,
        box,
        t_NodeSampleSize>;
    std::unique_ptr<rtree_t> mp_rtree_ptr;

};

template<size_t t_NodeSampleSize>
void Vary_sample_buffer<t_NodeSampleSize>::build_structure(Data_Source_Information& data_info, rtree::IOLayersParameters parameters )
{
    rtree_t::build_io_layers(data_info.get_source_raw(),
                             this->get_rstreeName(data_info),
                             data_info.get_convert_function(),
                             1024 * 1024 * 1024,
                             nullptr,
                             parameters);
};

template<size_t t_NodeSampleSize>
void Vary_sample_buffer<t_NodeSampleSize>::perform_file_experiments_GEO()
{
    //const int fixed_q = 18000000;
    m_query_sample_frequency = 400000;

    auto data_source = Data_Source_Information::get_data_information(Geolife);
    std::string filename = "geo_queries.txt";
    std::string input_filename = this->get_rstreeName(*data_source);

    std::cout << "opening tree for " << t_NodeSampleSize << std::endl;
    mp_rtree_ptr.reset(new rtree_t(input_filename));
    mp_rtree_ptr->get_block_manager().set_cache_capacity(0);
    std::cout << "tree is open, starting experiments" << std::endl;

    auto file_in = new std::fstream{ filename.c_str(), std::fstream::in };

    std::string line;
    while (std::getline(*file_in, line))
    {
        if (line.size() == 0)
            continue;

        float x_min, y_min, t_min, x_max, y_max, t_max;
        long estimated_count;
        long actual_count;

        sscanf(line.c_str(), "(%f,%f,%f)--(%f,%f,%f),%ld,%ld", &x_min, &y_min, &t_min, &x_max, &y_max, &t_max, &estimated_count, &actual_count);

        // we only want queries which are equal to our fixed value.
        //if (estimated_count != fixed_q)
        //    continue;

        mongo_types::box3d query;
        query.min_corner().set<0>(x_min);
        query.min_corner().set<1>(y_min);
        query.min_corner().set<2>(t_min);
        query.max_corner().set<0>(x_max);
        query.max_corner().set<1>(y_max);
        query.max_corner().set<2>(t_max);

        // we want to take a 10% sample
        //long k = estimated_count / 10;

        // we want to fix k
        long k = 400000;

        std::cout << "query: " << line << std::endl;
        this->perform_query(k, query, estimated_count, *data_source, actual_count);
    }
}


template<size_t t_NodeSampleSize>
void Vary_sample_buffer<t_NodeSampleSize>::perform_query(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, size_t element_count_in_region)
{
    boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

    utilities::null_iterator<mongo_types::sample_entry> iter;

    this->clear_system_cache();

    boost::timer::cpu_timer query_timer;
    auto cursor = mp_rtree_ptr->sample_query(query_box);
    query_timer.stop();
    for (long i = std::min(m_query_sample_frequency, k); i <= k; i += m_query_sample_frequency)
    {
        ////
        query_timer.resume();
        cursor.get_samples(m_query_sample_frequency, iter);
        query_timer.stop();
        ////

        float time = query_timer.elapsed().wall / (1000.0 * 1000 * 1000);

        vary_sample_buffer_results results;
        results.inputFile = get_rstreeName(data_info);
        results.method_name = "sample";
        results.start_time = start_time;
        results.k = i;
        results.q_exact = element_count_in_region;
        results.q_group = q_group;
        results.query_box = query_box;
        results.query_time_sec = time;
        results.samples_returned = iter.get_count();
        results.io_cost = cursor.get_io_cost();
        results.sample_size = t_NodeSampleSize;

        write_experiment_entry(results);
    }
}

template<size_t t_NodeSampleSize>
std::string Vary_sample_buffer<t_NodeSampleSize>::get_rstreeName(Data_Source_Information& data_info)
{
    return data_info.get_source_created() + '_' + std::to_string(t_NodeSampleSize);
}

template <size_t t_NodeSampleSize>
void Vary_sample_buffer<t_NodeSampleSize>::clear_system_cache()
{
    // this uses a custom utility to clear the system cache
    if (mp_rtree_ptr)
        mp_rtree_ptr->flush_cache();
    auto unused = system("./clearCache > /dev/null");
}
//
template<size_t t_NodeSampleSize>
void Vary_sample_buffer<t_NodeSampleSize>::open_outputfile()
{
    m_output.reset(new std::fstream{ m_outputFilename, std::fstream::out | std::fstream::app });

    if (!m_output->tellp())
    {
        this->write_header();
    }
}

template<size_t t_nodesamplesize>
void Vary_sample_buffer<t_nodesamplesize>::write_header()
{
    *m_output << "input file" << m_dataSeperator
        << "start time" << m_dataSeperator
        << "method name" << m_dataSeperator
        << "k" << m_dataSeperator
        << "actual |q|" << m_dataSeperator
        << "expected |q|" << m_dataSeperator
        << "query box min" << m_dataSeperator
        << "query box max" << m_dataSeperator
        << "query time" << m_dataSeperator
        << "samples returned" << m_dataSeperator
        << "io cost" << m_dataSeperator
        << "sample size" << m_dataSeperator
        << std::endl;
}

template<size_t t_nodesamplesize>
void Vary_sample_buffer<t_nodesamplesize>::write_experiment_entry(vary_sample_buffer_results results)
{
    *m_output << results.inputFile << m_dataSeperator
        << results.start_time << m_dataSeperator
        << results.method_name << m_dataSeperator
        << results.k << m_dataSeperator
        << results.q_exact << m_dataSeperator
        << results.q_group << m_dataSeperator
        << '(' << results.query_box.min_corner().get<0>() << ',' << results.query_box.min_corner().get<1>() << ',' << results.query_box.min_corner().get<2>() << ')' << m_dataSeperator
        << '(' << results.query_box.max_corner().get<0>() << ',' << results.query_box.max_corner().get<1>() << ',' << results.query_box.max_corner().get<2>() << ')' << m_dataSeperator
        << results.query_time_sec << m_dataSeperator
        << results.samples_returned << m_dataSeperator
        << results.io_cost << m_dataSeperator
        << results.sample_size
        << std::endl;
}


#endif