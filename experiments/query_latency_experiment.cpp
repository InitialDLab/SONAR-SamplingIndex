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
#include "query_latency_experiment.h"

#include "Data_Source_Information.h"

#include <algorithm>

size_t latency_memory_limit = 4L * 1024L * 1024L * 1024L;

query_latency_experiment::query_latency_experiment(std::string cursor_output_file, std::string sample_output_file, Data_sources method_to_use, SAMPLING_METHOD sampling_method)
    : m_cursor_output_file{ cursor_output_file }
    , m_sample_output_file{ sample_output_file }
    , m_method{sampling_method}
{
    if (method_to_use == Data_sources::Geolife) {
        auto data_source = Data_Source_Information::get_data_information(Geolife);

        mp_rtree_ptr.reset(new rtree_t(data_source->get_source_created(), true /*unlimited memory*/, false /*load using mem nodes*/, latency_memory_limit));
        m_query_input_file = "geo_queries.txt";
    }
    else if (method_to_use == Data_sources::OSM_nodes) {
        auto data_source = Data_Source_Information::get_data_information(OSM_nodes);
        mp_rtree_ptr.reset(new rtree_t(data_source->get_source_created(), true /*unlimited memory*/, false /*load using mem nodes*/, latency_memory_limit));
        m_query_input_file = "osm_queries.txt";
    }
    else {
        throw "bad method to use";
    }
}

void query_latency_experiment::run_experiment(int trials, int sample_size)
{
    // get the query set loaded
    
    // make sure the maps are in working order?

    // do queries, timing the results
    std::fstream file_in{ this->m_query_input_file, std::fstream::in };
    int queries_pulled = 0;

    std::string line;
    while (std::getline(file_in, line))
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

        int create_timer_time;
        int query_time;

        utilities::null_iterator<mongo_types::sample_entry> iter;

        for (int r_trials = 0; r_trials < trials; ++r_trials) {

            if (this->m_method == SAMPLING_METHOD::NAIVE) {
                boost::timer::cpu_timer create_cursor_timer;
                auto cursor1 = this->mp_rtree_ptr->naive_sample_query(query);
                create_cursor_timer.stop();
                boost::timer::cpu_timer query_timer;
                cursor1.get_samples(sample_size, iter);
                query_timer.stop();

                create_timer_time = create_cursor_timer.elapsed().wall / (1000.0 * 1000);
                query_time = query_timer.elapsed().wall / (1000.0 * 1000);
            }
            else if (this->m_method == SAMPLING_METHOD::IID) {
                throw "not supported right now";
                //boost::timer::cpu_timer create_cursor_timer;
                //auto cursor1 = this->mp_rtree_ptr->iid_sample_query(query);
                //create_cursor_timer.stop();
                //boost::timer::cpu_timer query_timer;
                //cursor1.get_samples(sample_size, iter);
                //query_timer.stop();

                //create_timer_time = create_cursor_timer.elapsed().wall / (1000.0 * 1000);
                //query_time = query_timer.elapsed().wall / (1000.0 * 1000);
            }
            else if (this->m_method == SAMPLING_METHOD::IID_MEM) {
                throw "not supported right now";
                //boost::timer::cpu_timer create_cursor_timer;
                //auto cursor1 = this->mp_rtree_ptr->iid_sample_query_imemleaf(query);
                //create_cursor_timer.stop();
                //boost::timer::cpu_timer query_timer;
                //cursor1.get_samples(sample_size, iter);
                //query_timer.stop();

                //create_timer_time = create_cursor_timer.elapsed().wall / (1000.0 * 1000);
                //query_time = query_timer.elapsed().wall / (1000.0 * 1000);
            }
            else if (this->m_method == SAMPLING_METHOD::IID_MEMLEAF) {
                throw "not supported right now";
                //boost::timer::cpu_timer create_cursor_timer;
                //auto cursor1 = this->mp_rtree_ptr->iid_sample_query_memleaf(query);
                //create_cursor_timer.stop();
                //boost::timer::cpu_timer query_timer;
                //cursor1.get_samples(sample_size, iter);
                //query_timer.stop();

                //create_timer_time = create_cursor_timer.elapsed().wall / (1000.0 * 1000);
                //query_time = query_timer.elapsed().wall / (1000.0 * 1000);
            }
            else if (this->m_method == SAMPLING_METHOD::SAMPLE) {
                boost::timer::cpu_timer create_cursor_timer;
                auto cursor1 = this->mp_rtree_ptr->sample_query(query);
                create_cursor_timer.stop();
                boost::timer::cpu_timer query_timer;
                cursor1.get_samples(sample_size, iter);
                query_timer.stop();

                create_timer_time = create_cursor_timer.elapsed().wall / (1000.0 * 1000);
                query_time = query_timer.elapsed().wall / (1000.0 * 1000);
            }
            else if (this->m_method == SAMPLING_METHOD::LEVEL_SAMPLE) {
                //throw "does not yet exist";
            }

            // update counters
            ++m_cursor_creation[estimated_count][create_timer_time];
            ++m_sample_time[estimated_count][create_timer_time];
            queries_pulled++;
        }
    }


    // dump results?
    std::fstream file_out1{ this->m_cursor_output_file, std::fstream::out };
    std::fstream file_out2{ this->m_sample_output_file, std::fstream::out };

    double latency[] = { 0.25, 0.50, 0.75, 0.95, 0.99 };

    file_out1 << "q";
    file_out2 << "q";
    for (double d : latency) {
        file_out1 << '\t' << d;
        file_out2 << '\t' << d;
    }

    for (auto &v : m_cursor_creation) {
        std::sort(v.second.begin(), v.second.end());
        file_out1 << v.first;
        for (double d : latency)
            file_out1 << '\t' << v.second[int(v.second.size() * d)];
    }

    for (auto &v : m_sample_time) {
        std::sort(v.second.begin(), v.second.end());
        file_out2 << v.first;
        for (double d : latency)
            file_out2 << '\t' << v.second[int(v.second.size() * d)];
    }

    file_out1.close();
    file_out2.close();
}

