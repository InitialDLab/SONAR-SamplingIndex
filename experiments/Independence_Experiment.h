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

#include "experiment_utilities.h"

class Independence_Experiment
{
private:

public:
    enum SAMPLING_METHOD {NAIVE, IID, SAMPLE, LEVEL_SAMPLE};

    Independence_Experiment(size_t baseDataSize, size_t experimentSelectionSize);
    ~Independence_Experiment();

    void run_experiment(int trials);
    void run_experiment(std::string output_name, SAMPLING_METHOD method, int trials);
private:
    void build_data();

    void prepare_privateData();

    void regenerate_hidden();

    std::string results_filename;
    std::string rstree_filename;
    std::string dataset_filename;

    std::vector<int> hidden_selection;
    std::vector<int> full_hidden_selection;
    std::vector<int> intersection_count_histogram;

    // these maps use units ms for key and count for value
    std::map<int, int> cursor_setup_histogram;
    std::map<int, int> cursor_query_histogram;

    std::unique_ptr<utilities::rtree_t> m_tree;
    size_t total_data_size;
    size_t selection_size;
};

