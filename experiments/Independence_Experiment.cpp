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
#include "Independence_Experiment.h"

#include "experiment_utilities.h"

#include <iostream>
#include <fstream>
#include <functional>
#include <string>

#include <boost/timer/timer.hpp>

Independence_Experiment::Independence_Experiment(size_t baseDataSize, size_t experimentSelectionSize)
    : total_data_size{ baseDataSize }
    , selection_size{ experimentSelectionSize }
    , results_filename{ "independence_results.txt" }
    , rstree_filename{ "independence_tree" }
    , dataset_filename{ "independence_dataset.txt" }
{
    build_data();

    prepare_privateData();
}


Independence_Experiment::~Independence_Experiment()
{
}

void Independence_Experiment::build_data()
{
    // randomly generate a dataset and write to the dataset_filename
    std::default_random_engine generator;
    std::uniform_real_distribution<float> lat_dist(-90.0, 90.0);
    std::uniform_real_distribution<float> lon_dist(-90.0, 90.0);

    std::ofstream ofs;

    ofs.open(dataset_filename, std::ifstream::out);

    char output_buffer[128];

    for (int i = 1; i <= total_data_size; ++i)
    {
        snprintf(output_buffer, 128, "%024X %f %f %d\n", i, lat_dist(generator), lon_dist(generator), i);
        ofs << output_buffer;
    }

    ofs.close();

    // build the rstree using the input dataset
    std::function<utilities::entry(const std::string&)> Rdata_converter =
        [](const std::string& str)
    {
        float x_val;
        float y_val;
        int   time;
        char OIDvalue[32];

        int ret = sscanf(str.c_str(), "%24s %f %f %d", OIDvalue, &x_val, &y_val, &time);

        return mongo_types::entry(x_val, y_val, time, OIDvalue);
    };

    utilities::rtree_t::build_io_layers(this->dataset_filename, rstree_filename, Rdata_converter);
}

void Independence_Experiment::prepare_privateData()
{
    // open tree to be used
    this->m_tree.reset(new utilities::rtree_t(this->rstree_filename, true, false));

    // setup private data (hidden selection and histogram data)

    // hidden selection
    full_hidden_selection.resize(total_data_size);
    int idx = 0;
    std::generate_n(full_hidden_selection.begin(), total_data_size, [&idx]() {return ++idx; });

    regenerate_hidden();

    // histogram of intersection size
    intersection_count_histogram.resize(0);
    intersection_count_histogram.resize(selection_size);
}

void Independence_Experiment::regenerate_hidden()
{
    std::random_shuffle(full_hidden_selection.begin(), full_hidden_selection.end());
    this->hidden_selection.assign(full_hidden_selection.begin(), full_hidden_selection.begin() + selection_size);

    std::sort(hidden_selection.begin(), hidden_selection.end());
}

void Independence_Experiment::run_experiment(int trials)
{
    run_experiment(this->results_filename, SAMPLING_METHOD::NAIVE, trials);
}

void Independence_Experiment::run_experiment(std::string output_name, SAMPLING_METHOD method, int trials)
{
    throw "not supported right now";
    //mongo_types::box3d query_region;
    //query_region.min_corner().set<0>(-90.0);
    //query_region.min_corner().set<1>(-90.0);
    //query_region.min_corner().set<2>(1);
    //query_region.max_corner().set<0>(90.0);
    //query_region.max_corner().set<1>(90.0);
    //query_region.max_corner().set<2>(100000000);

    //std::vector<utilities::sample_entry> sample_buffer;

    //cursor_setup_histogram.clear();
    //cursor_query_histogram.clear();

    //// loops
    //boost::timer::cpu_timer experiment_timer;

    //for (int trials_performed = 0; trials_performed < trials; ++trials_performed) {
    //    // do a request for samples
    //    sample_buffer.resize(0);
    //    if (method == NAIVE) {
    //        auto cursor1 = this->m_tree->naive_sample_query(query_region);
    //        cursor1.get_samples(selection_size, std::back_inserter(sample_buffer));
    //    }
    //    else if (method == IID) {
    //        throw "not supported right now";
    //        //auto cursor2 = this->m_tree->iid_sample_query(query_region);
    //        //cursor2.get_samples(selection_size, std::back_inserter(sample_buffer));
    //    }
    //    else if (method == SAMPLE) {
    //        auto cursor3 = this->m_tree->sample_query(query_region);
    //        cursor3.get_samples(selection_size, std::back_inserter(sample_buffer));
    //    }
    //    else if (method == LEVEL_SAMPLE){
    //        throw "not supported right now";
    //        //throw "not implemented";
    //    }

    //    auto cursor = this->m_tree->iid_sample_query_imemleaf(query_region);

    //    // count intersection between hidden set
    //    int intersection_count = 0;
    //    for (auto &i : sample_buffer)
    //    {
    //        assert(i.timestamp != 0);

    //        if (std::binary_search(hidden_selection.begin(), hidden_selection.end(), i.timestamp))
    //            ++intersection_count;
    //    }

    //    if (trials_performed % 100 == 0)
    //        std::cout << trials_performed << std::endl;

    //    // increment internal histogram
    //    ++intersection_count_histogram[intersection_count];

    //    // update histogram counters
    //    //++cursor_setup_histogram[create_timer.elapsed().wall / (1000.0 * 1000)];
    //    //++cursor_query_histogram[query_timer.elapsed().wall / (1000.0 * 1000)];
    //}
    //experiment_timer.stop();

    //// dump the output
    //std::ofstream ofs;
    //ofs.open(output_name, std::ifstream::out);
    //for (int i = 0; i < intersection_count_histogram.size(); ++i)
    //    if (intersection_count_histogram[i] != 0) {
    //        //std::cout << i << ' ' << intersection_count_histogram[i] << '\n';
    //        ofs << i << ' ' << intersection_count_histogram[i] << '\n';
    //    }
    //ofs.close();

    //std::ofstream setup_time_file;
    //setup_time_file.open("setup_time.txt", std::ifstream::out);
    //for (auto elm : cursor_setup_histogram)
    //    setup_time_file << elm.first << '\t' << elm.second << '\n';
    //setup_time_file.close();

    //std::ofstream query_time_file;
    //query_time_file.open("query_time.txt", std::ifstream::out);
    //for (auto elm : cursor_query_histogram)
    //    query_time_file << elm.first << '\t' << elm.second << '\n';
    //query_time_file.close();

    //std::cout << "experiment took " << (experiment_timer.elapsed().wall / (1000.0 * 1000 * 1000)) << std::endl;
}