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
#pragma once

#include <vector>
#include <string>
#include <random>
#include <map>

#include "experiment_utilities.h"

class query_latency_experiment
{
public:
    enum SAMPLING_METHOD { NAIVE, IID, IID_MEM, IID_MEMLEAF, SAMPLE, LEVEL_SAMPLE, RANGE_REPORT };

    query_latency_experiment(std::string cursor_output_file, std::string sample_output_file, Data_sources method_to_use, SAMPLING_METHOD sampling_method);

    void run_experiment(int trials, int sample_size);
private:

    std::string m_cursor_output_file;
    std::string m_sample_output_file;

    std::string m_query_input_file;

    SAMPLING_METHOD m_method;

    // cursor creation time.  First key by the value of q
    std::map<long, std::vector<int> > m_cursor_creation;

    // cursor sample time
    std::map<long, std::vector<int> > m_sample_time;

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

    //using level_t = level_sampling::level_sampling < entry, sample_entry >;
    //std::unique_ptr<level_t> mp_levelSample_ptr;

    //using shuffle_t = RandomShuffle < entry >;
    //std::unique_ptr<shuffle_t> mp_randomShuffle_ptr;
};