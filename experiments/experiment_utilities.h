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
#ifndef EXPERIMENT_UTILITIES_H__
#define EXPERIMENT_UTILITIES_H__

#include <fstream>
#include <string>
#include <memory>
#include <vector>
#include <chrono>
#include <random>
#include <iterator>
#include <memory>

#include "boost/date_time/posix_time/posix_time.hpp"

#include <boost/geometry/geometry.hpp>

#include "mongo_types.h"
#include "Data_Source_Information.h"

#include "../rtree/rtree.h"

namespace utilities
{
    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;

    using rtree_t = rtree::rtree <
        entry,
        sample_entry
    >;


    double vec_min(const std::vector<double>& data);

    double vec_max(const std::vector<double>& data);

    double vec_mean(const std::vector<double>& data);

    double vec_stdDev(const std::vector<double>& data);

    double vec_sum(const std::vector<double>& data);

    mongo_types::box3d get_random_query_box(float percentage_cover, Data_Source_Information& data_info);

    mongo_types::box3d get_random_insertion_box(long seed, float percentage_cover, Data_Source_Information& data_info);

    mongo_types::box3d get_query_box_by_volume(mongo_types::point3d center, float volume_percent, Data_Source_Information& data_info);

    mongo_types::box3d get_query_region_with_element_count(std::shared_ptr<rtree_t> tree, Data_Source_Information& data_info, int64_t target_size, size_t tolerence, size_t &elements_in_region);

    std::vector<mongo_types::entry> get_random_entry_vector(const mongo_types::box3d &region, size_t count, long seed);

    mongo_types::entry get_random_entry(const mongo_types::box3d &region, std::default_random_engine &rd);

    long generate_seed();

    template<typename T>
    class null_iterator : public std::iterator < std::input_iterator_tag, T >
    {
    public:
        null_iterator() : count(new size_t(0)) { }
        null_iterator(const null_iterator& mit) : count(mit.count) {}
        null_iterator& operator++() { /*(*count)++;*/ return *this; }
        null_iterator operator++(int) { /*(*count)++;*/ return *this; }
        bool operator == (const null_iterator& rhs) { return this.count == rhs.count; }
        bool operator != (const null_iterator& rhs) { return !(this == rhs); }
        T& operator*() { (*count)++; return element; }

        size_t get_count() { return *count; }
    private:
        std::shared_ptr<size_t> count;
        T element;
    };
};



#endif
