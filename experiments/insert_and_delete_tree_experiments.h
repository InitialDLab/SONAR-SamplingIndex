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
#ifndef INSERT_TREE_EXPERIMENTS_H__
#define INSERT_TREE_EXPERIMENTS_H__

#include <fstream>
#include <vector>
#include <string>
#include <memory>
#include <chrono>

#include "boost/date_time/posix_time/posix_time.hpp"

#include <boost/geometry/geometry.hpp>

#include "../rtree/rtree.h"
#include "../level_sampling/level_sampling.h"

#include "mongo_types.h"
#include "Data_Source_Information.h"

struct tree_results
{
    std::string inputfile;
    boost::posix_time::ptime start_time;
    size_t elements_in_region;
    int k;
    mongo_types::box3d query_box;
    long seed;
    double time;
    int io_write;
    size_t io_read;
    int method; // 0 = rs-tree, 1 = baseline
};

class Insert_and_delete_tree_experiments
{
    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;
public:
    Insert_and_delete_tree_experiments(std::string insert_outputFile = "insert_tree_experiment.txt", std::string delete_outputFile = "delete_tree_experiment.txt")
    :
        m_dataSeperator{ '\t' },
        m_insert_outputFilename{ insert_outputFile },
        m_delete_outputFilename{ delete_outputFile }
    {
        open_insert_outputfile();
        open_delete_outputfile();
    };

    virtual ~Insert_and_delete_tree_experiments()
    {
        m_insert_output->close();
        m_delete_output->close();
    };

    void perform_file_experiments_GEO(int method = 0);
    void perform_file_experiments_OSM(int method = 0);


private:
    void test_experiments_from_file(std::string filename, Data_Source_Information& data_info, int method);

    void test_insert_and_delete_level(std::vector<mongo_types::entry>& to_insert,
        mongo_types::box3d region_Q,
        size_t elements_in_region,
        long seed,
        Data_Source_Information& data_info,
        int method = 2
        );

    void test_insert_and_delete_singleTree(std::vector<mongo_types::entry>& to_insert,
        mongo_types::box3d region_Q,
        size_t elements_in_region,
        long seed,
        Data_Source_Information& data_info,
        int method = 0
        );

    // for simplicity, we will pass in the elements to insert along with some statistics
    // about the generated items
    //void test_insert_and_delete(std::vector<mongo_types::entry>& to_insert,
    //    int iteration_count,
    //    float percentage_cover,
    //    long seed,
    //    mongo_types::box3d query_box,
    //    Data_Source_Information& data_info);

    void clear_system_cache();

    void open_insert_outputfile();
    void write_insert_header();
    void write_insert_experiment_entry(tree_results results);

    void open_delete_outputfile();
    void write_delete_header();
    void write_delete_experiment_entry(tree_results results);

    const std::string m_insert_outputFilename;
    std::unique_ptr<std::fstream> m_insert_output;

    const std::string m_delete_outputFilename;
    std::unique_ptr<std::fstream> m_delete_output;

    using rtree_t = rtree::rtree <
        entry,
        sample_entry,
        box
    >;
    std::unique_ptr<rtree_t> mp_rtree_ptr;

    using level_t = level_sampling::level_sampling < entry, sample_entry >;
    std::unique_ptr<level_t> mp_levelSample_ptr;

    char  m_dataSeperator;
};




#endif