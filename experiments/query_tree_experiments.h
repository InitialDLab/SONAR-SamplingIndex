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
//
// Tests which are done when querying the tree
//
#ifndef QUERY_TREE_EXPERIMENTS_H__
#define QUERY_TREE_EXPERIMENTS_H__

#include <fstream>
#include <string>
#include <memory>
#include <chrono>

#include "boost/date_time/posix_time/posix_time.hpp"

#include <boost/geometry/geometry.hpp>

#include <rtree/rtree.h>

#include <level_sampling/level_sampling.h>

#include "mongo_types.h"
#include "Data_Source_Information.h"
#include "../random_shuffle.h"


struct query_results
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

    size_t block_size;
    size_t cache_size;

    rtree::Stats cursor_stats;
};

class Query_tree_experiments
{
public:
    Query_tree_experiments(std::string outputFile = "query_tree_experiments.txt")
        :
        m_dataSeperator{ '\t' },
        m_outputFilename{ outputFile },
        m_testEstimation{ false },
        m_testBaseline{ false },
        m_testLevelSample{ false },
        m_testRangeSample{ false },
        m_testRandomShuffle{ false },
        m_testInMem{ false },
        m_fixedk{ 0 },
        m_IOUsed{ false }
    {
        open_outputfile();
    };

    virtual ~Query_tree_experiments()
    {
        m_output->close();
    };

    //void perform_listed_experiments_GEO();

    //void perform_listed_experiments_OSM();

    void perform_file_experiments_GEO();

    void perform_file_experiments_OSM();

    // launches the test with a random seed value
    long generate_seed()
    {
        std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
        std::chrono::system_clock::duration dtn = tp.time_since_epoch();

        return dtn.count();
    }

    void set_testEstimation(bool enable = true)
    {
        m_testEstimation = enable;
    }
    void set_testBaseline(bool enable = true)
    {
        m_testBaseline = enable;
    }
    void set_testLevelSample(bool enable = true)
    {
        m_testLevelSample = enable;
    }
    void set_testRangeSample(bool enable = true)
    {
        m_testRangeSample = enable;
    }
    void set_testRandomShuffle(bool enable = true)
    {
        m_testRandomShuffle = enable;
    }
    bool get_usingRtree()
    {
        return m_testEstimation || m_testBaseline || m_testRangeSample;
    }
    bool get_usingLeveltree()
    {
        return m_testLevelSample;
    }
    bool get_usingRandomShuffle()
    {
        return m_testRandomShuffle;
    }
    void set_fixk(long k)
    {
        m_fixedk = k;
    }



private:
    void test_experiments_from_file(std::string filename, Data_Source_Information& data_info);

    //void test_GeoLife(float percentage_cover, float sample_percentage, long seed);
    //void test_OSMData(float percentage_cover, float sample_percentage, long seed);

    void test_est(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, float sample_percentage_max, size_t element_count_in_region);
    void test_baseline(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, float sample_percentage_max, size_t element_count_in_region);
    void test_rangeReport(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, float sample_percentage_max, size_t element_count_in_region);
    void test_levelSample(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, float sample_percentage_max, size_t element_count_in_region);
    void test_randomShuffle(long k, mongo_types::box3d query_box, long q_group, Data_Source_Information& data_info, float sample_percentage_max, size_t element_count_in_region);

    bool m_testEstimation;
    bool m_testBaseline;
    bool m_testLevelSample;
    bool m_testRangeSample;
    bool m_testRandomShuffle;

    bool m_testInMem;

    bool m_IOUsed;

    void open_outputfile();
    void write_header();
    void write_experiment_entry(query_results results);

    void clear_system_cache();

    long query_sample_frequency;
    long m_fixedk;

    const std::string m_outputFilename;
    std::unique_ptr<std::fstream> m_output;
    char  m_dataSeperator;

    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    using rtree_t = rtree::rtree <
        entry,
        sample_entry,
        box
    >;
    std::unique_ptr<rtree_t> mp_rtree_ptr;

    using level_t = level_sampling::level_sampling < entry, sample_entry > ;
    std::unique_ptr<level_t> mp_levelSample_ptr;

    using shuffle_t = RandomShuffle < entry > ;
    std::unique_ptr<shuffle_t> mp_randomShuffle_ptr;
};


#endif
