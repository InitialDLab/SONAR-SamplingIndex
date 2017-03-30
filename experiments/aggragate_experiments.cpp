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
#include "aggragate_experiments.h"

#include <vector>
#include <random>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "mongo_types.h"
#include "experiments/Data_Source_Information.h"
#include "experiments/experiment_utilities.h"
#include "rtree/rtree.h"

// this experiment is using chernoff bounds to calculate expected error
// delta is the expected probability of failure
// alpha is the expected variation between the actual value and aggregated value
// spread is the maximum - minimum value
// omega is 2 * alpha / spread  (this is used to quantify error under various spreads)
// r is the number of samples used for an aggregation.

void aggragate_experiments::perform_listed_experiments_GEO()
{
    // experiment settings for easy modification
    int n = 100; // the number of regions to test
    long seed = 1L; // the seed to use.  Change for actual experiment;
    float min_query_size = 0.01;
    float max_query_size = 0.30;
    std::uniform_real_distribution<float> cover_distribution(min_query_size, max_query_size);
    std::default_random_engine generator;
    generator.seed(seed);


    std::vector<double> delta_to_test{ 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80 };
    std::vector<double> omega_to_test{ 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80 };

    // open the data
    auto data_source = Data_Source_Information::get_data_information(Geolife);

    std::vector<mongo_types::box3d> regions_to_test;

    // randomly select several regions which we will test
    for (int i = 0; i < n; i++)
        regions_to_test.push_back(utilities::get_random_query_box(cover_distribution(generator), *data_source));

    // test the listed experiments
    perform_listed_experiments(data_source, regions_to_test, delta_to_test, omega_to_test);
}

void aggragate_experiments::perform_listed_experiments_OSM()
{
    // experiment settings for easy modification
    int n = 1000; // the number of regions to test
    long seed = 1L; // the seed to use.  Change for actual experiment;
    float min_query_size = 0.01;
    float max_query_size = 0.30;
    std::uniform_real_distribution<float> cover_distribution(min_query_size, max_query_size);
    std::default_random_engine generator;
    generator.seed(seed);


    std::vector<double> delta_to_test{ 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80 };
    std::vector<double> omega_to_test{ 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80 };

    // open the data
    auto data_source = Data_Source_Information::get_data_information(OSM_nodes);

    std::vector<mongo_types::box3d> regions_to_test;

    // randomly select several regions which we will test
    for (int i = 0; i < n; i++)
        regions_to_test.push_back(utilities::get_random_query_box(cover_distribution(generator), *data_source));

    // test the listed experiments
    perform_listed_experiments(data_source, regions_to_test, delta_to_test, omega_to_test);
}


void aggragate_experiments::perform_listed_experiments(std::shared_ptr<Data_Source_Information> data_source,
                                                       std::vector<mongo_types::box3d> &sample_ranges,
                                                       std::vector<double> &delta_values,
                                                       std::vector<double> &omega_values)
{
    // generate the exact queries for the experiments.
    std::vector<streaming_aggragate_query_time> exact_results;
    for (auto range : sample_ranges)
    {
        exact_results.emplace_back(range);
    }

    // scan through the data, gathering exact query data //
    // (also printing the progress of the scan, because it can take a very long time) //
    auto convert_function = data_source->get_convert_function();
    long elements_to_read = data_source->get_element_count();
    long elements_read = 0;
    // open input file
    std::fstream in_file(data_source->get_source_raw().c_str(), std::ios_base::in);
    std::string str;
    while (!in_file.eof())
    {
        std::getline(in_file, str);

        // empty string are considered null, so don't enter any data from a zero length string
        if (str.size() == 0)
            continue;
        mongo_types::sample_entry current_item(convert_function(str));

        for (int i = 0; i < exact_results.size(); i++)
            exact_results[i].submit(current_item);
        //for (auto exact_query : exact_results)
        //    exact_query.submit(current_item);

        ++elements_read;

        if (elements_read % 1000000 == 0)
            std::cout << ((0.0 + elements_read) / elements_to_read) << std::endl;
    }

    // open the sampling tree
    using rtree_t = rtree::rtree <
        mongo_types::entry,
        mongo_types::sample_entry,
        mongo_types::box3d,
        256,
        256
    >;
    std::unique_ptr<rtree_t> rtree_ptr;
    rtree_ptr.reset(new rtree_t(data_source->get_source_created()));

    // perform the experiments for each delta and omega values
    for (auto delta : delta_values) {
        for (auto omega : omega_values) {
            for (auto region : exact_results) {
                boost::posix_time::ptime start_time{ boost::posix_time::second_clock::local_time() };

                double spread = region.get_queryBox().max_corner().get<2>() - region.get_queryBox().min_corner().get<2>();
                double alpha = omega * spread / 2.0;
                int r = std::ceil(-(spread * spread / (2.0 * alpha)) * std::log(delta / 2.0));


                // setup query which we will be doing the aggregation over
                streaming_aggragate_query_time sampled_query(region.get_queryBox());
                auto cursor = rtree_ptr->sample_query(sampled_query.get_queryBox());

                 std::vector<mongo_types::sample_entry> samples;
                samples.reserve(r);

                // fulfill query and get submitted aggregation
                int count = cursor.estimate_count();
                // std::cerr << (1.0 * r / count) << std::endl;
                cursor.get_samples(r, std::back_inserter(samples));
                for (auto element : samples)
                    sampled_query.submit(element);

                write_experiment_entry(data_source->get_source_created(),
                    start_time,
                    delta,
                    spread,
                    r,
                    omega,
                    alpha,
                    region,
                    sampled_query);
            }
        }
    }

}





void aggragate_experiments::open_outputfile()
{
    m_output.reset(new std::fstream{ m_outputFilename, std::fstream::out | std::fstream::app });

    if (!m_output->tellp())
    {
        write_header();
    }
}

void aggragate_experiments::write_header()
{
    *m_output << "input file" << m_dataSeperator
        << "Start time"       << m_dataSeperator
        << "query box min"    << m_dataSeperator
        << "query box max"    << m_dataSeperator
        << "actual min"       << m_dataSeperator
        << "actual max"       << m_dataSeperator
        << "actual spread"    << m_dataSeperator
        << "delta"            << m_dataSeperator
        << "alpha"            << m_dataSeperator
        << "spread"           << m_dataSeperator
        << "r"                << m_dataSeperator
        << "omega"            << m_dataSeperator
        << "actual mean"      << m_dataSeperator
        << "est mean"         << m_dataSeperator
        << "within alpha"
        << std::endl;
}

void aggragate_experiments::write_experiment_entry(std::string inputFile,
                                                   boost::posix_time::ptime start_time,
                                                   double delta,
                                                   double spread,
                                                   int r,
                                                   double omega,
                                                   double alpha,
                                                   streaming_aggragate_query_time actual,
                                                   streaming_aggragate_query_time sampled)
{
    *m_output << start_time << m_dataSeperator
        << start_time << m_dataSeperator
         << '(' << actual.get_queryBox().min_corner().get<0>() << ',' << actual.get_queryBox().min_corner().get<1>() << ',' << actual.get_queryBox().min_corner().get<2>() << ')' << m_dataSeperator
         << '(' << actual.get_queryBox().max_corner().get<0>() << ',' << actual.get_queryBox().max_corner().get<1>() << ',' << actual.get_queryBox().max_corner().get<2>() << ')' << m_dataSeperator
        << actual.get_min()  << m_dataSeperator
        << actual.get_max()  << m_dataSeperator
        << (actual.get_max() - actual.get_min()) << m_dataSeperator
        << delta             << m_dataSeperator
        << alpha             << m_dataSeperator
        << spread            << m_dataSeperator
        << r                 << m_dataSeperator
        << omega             << m_dataSeperator
        << std::setprecision(10) << actual.get_avg()  << m_dataSeperator
        << std::setprecision(10) << sampled.get_avg() << m_dataSeperator
        << ((std::abs(actual.get_avg() - sampled.get_avg()) < alpha) ? "1" : "0")
        << std::endl;

}