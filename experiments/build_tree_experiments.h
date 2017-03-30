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
// Tests which are done on the building of the tree
#ifndef BUILD_TREE_EXPERIMENTS_H__
#define BUILD_TREE_EXPERIMENTS_H__

#include <iostream>
#include <fstream>
#include <string>
#include <memory>

#include "boost/date_time/posix_time/posix_time.hpp"
#include "rtree/rtree.h"

class Build_tree_experiments
{
public:
    // max_tpie_memory is the maximum number of bytes tpie
    // is allowed to use if tpie is used to build the data structure
    Build_tree_experiments( std::string outputFile = "build_tree_experiments.txt" )
        : 
        m_dataSeperator{ '\t' },
        m_outputFilename{ outputFile }
    {
        open_outputfile();
    };

    virtual ~Build_tree_experiments()
    {
        m_output->close();
    };

    void test_GeoLife(size_t max_tpie_memory = (1024 * 1024 * 1024), int experiment_iterations = 1);

    void test_OSMData(size_t max_tpie_memory = (1024 * 1024 * 1024), int experiment_iterations = 1);

    void perform_listed_experiments_GEO();
    void perform_listed_experiments_OSM();
    
private:
    void open_outputfile();
    void write_header();
    void write_experiment_entry(std::string inputFile,
                               size_t tpie_memory,
                               boost::posix_time::ptime start_time,
                               size_t element_count,
                               double build_time,
                               int internal_nodes,
                               int leaf_nodes,
                               double read_time,
                               rtree::IOLayerBuildStatistics interal_build_stats);

    const std::string m_outputFilename;
    std::unique_ptr<std::fstream> m_output;
    char   m_dataSeperator;
};

#endif