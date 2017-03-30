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
// The experiments for aggragation
//
#ifndef AGGRAGATE_EXPERIMENTS_H__
#define AGGRAGATE_EXPERIMENTS_H__

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <iterator>

#include "boost/date_time/posix_time/posix_time.hpp"

#include "rtree/rtree.h"
#include "experiments/streaming_aggragate_query.h"
#include "experiments/Data_Source_Information.h"

class aggragate_experiments
{
public:
    // max_tpie_memory is the maximum number of bytes tpie
    // is allowed to use if tpie is used to build the data structure
    aggragate_experiments(std::string outputFile = "aggragate_experiments.txt")
        :
        m_dataSeperator{ '\t' },
        m_outputFilename{ outputFile }
    {
        open_outputfile();
    };

    virtual ~aggragate_experiments()
    {
        m_output->close();
    };

    void perform_listed_experiments_GEO();
    void perform_listed_experiments_OSM();

    void perform_listed_experiments(std::shared_ptr<Data_Source_Information> data_source,
                                    std::vector<mongo_types::box3d> &sample_ranges,
                                    std::vector<double> &delta_values,
                                    std::vector<double> &omega_values);

private:
    void open_outputfile();
    void write_header();
    void write_experiment_entry(std::string inputFile,
                                boost::posix_time::ptime start_time,
                                double delta,
                                double spread,
                                int r,
                                double omega,
                                double alpha,
                                streaming_aggragate_query_time actual,
                                streaming_aggragate_query_time sampled);

    const std::string m_outputFilename;
    std::unique_ptr<std::fstream> m_output;
    char   m_dataSeperator;
};

#endif