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
#include "experiment_utilities.h"

#include <chrono>
#include <algorithm>
#include <random>
#include <limits>
#include <math.h>

using namespace utilities;

double utilities::vec_min(const std::vector<double>& data)
{
    return *std::min_element(data.begin(), data.end());
}

double utilities::vec_max(const std::vector<double>& data)
{
    return *std::max_element(data.begin(), data.end());
}

double utilities::vec_mean(const std::vector<double>& data)
{
    return vec_sum(data) / data.size();
}

double utilities::vec_stdDev(const std::vector<double>& data)
{
    if (data.size() <= 1)
        return 0.0;

    double mean = vec_mean(data);

    double dev_sum{ 0.0 };
    for (auto element : data)
        dev_sum += (mean - element) * (mean - element);
    
    return std::sqrt(dev_sum / (data.size() - 1));
}

double utilities::vec_sum(const std::vector<double>& data)
{
    double sum = 0.0f;
    for (auto element : data)
    {
        sum += element;
    }

    return sum;
}

mongo_types::box3d utilities::get_random_query_box(float percentage_cover, Data_Source_Information& data_info)
{
    mongo_types::box3d toret;

    if (percentage_cover < 1.0f && percentage_cover > 0.0f)
    {
        // region selection goes as follows:
        // 1 - get a random existing point from the data set (so the box will cover at least one element, no matter how small
        //       the selection is.
        // 2 - extend the box to the minimum corner.  If the bounds of the box reach past the minimum, only go to the minimum point
        // 3 - extend the box to the maximum corner.  If the bounds of the box reach past the max, only go to the max point


        auto orgin_point = data_info.get_randomPoint();

        float x_range = data_info.get_x_max() - data_info.get_x_min();
        float y_range = data_info.get_y_max() - data_info.get_y_min();
        float t_range = data_info.get_t_max() - data_info.get_t_min();

        float avg_x_range = (data_info.get_x_max() + data_info.get_x_min()) / 2.0f;
        float avg_y_range = (data_info.get_y_max() + data_info.get_y_min()) / 2.0f;
        float avg_t_range = (data_info.get_t_max() + data_info.get_t_min()) / 2.0f;

        if (orgin_point.get<0>() < avg_x_range)
        {
            toret.min_corner().set<0>(std::max(orgin_point.get<0>() - 0.5f * percentage_cover * x_range, data_info.get_x_min()));
            toret.max_corner().set<0>(toret.min_corner().get<0>() + x_range * percentage_cover);
        } else {
            toret.max_corner().set<0>(std::min(orgin_point.get<0>() + 0.5f * percentage_cover * x_range, data_info.get_x_max()));
            toret.min_corner().set<0>(toret.max_corner().get<0>() - x_range * percentage_cover);
        }

        if (orgin_point.get<1>() < avg_y_range)
        {
            toret.min_corner().set<1>(std::max(orgin_point.get<1>() - 0.5f * percentage_cover * y_range, data_info.get_y_min()));
            toret.max_corner().set<1>(toret.min_corner().get<1>() + y_range * percentage_cover);
        } else {
            toret.max_corner().set<1>(std::min(orgin_point.get<1>() + 0.5f * percentage_cover * y_range, data_info.get_y_max()));
            toret.min_corner().set<1>(toret.max_corner().get<1>() - y_range * percentage_cover);
        }

        if (orgin_point.get<2>() < avg_t_range)
        {
            toret.min_corner().set<2>(std::max(orgin_point.get<2>() - 0.5f * percentage_cover * t_range, static_cast<float>(data_info.get_t_min())));
            toret.max_corner().set<2>(toret.min_corner().get<2>() + y_range * percentage_cover);
        } else {
            toret.max_corner().set<2>(std::min(orgin_point.get<2>() + 0.5f * percentage_cover * t_range, static_cast<float>(data_info.get_t_max())));
            toret.min_corner().set<2>(toret.max_corner().get<2>() - t_range * percentage_cover);
        }
    }
    else if (percentage_cover >= 1.0f)
    {
        // the result is a box with area which covers the entire area of the data set
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_max());
        toret.max_corner().set<1>(data_info.get_y_max());
        toret.max_corner().set<2>(data_info.get_t_max());
    }
    else // (percentage_cover <= 0.0f)
    {
        // the result is a box with no area
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_min());
        toret.max_corner().set<1>(data_info.get_y_min());
        toret.max_corner().set<2>(data_info.get_t_min());
    }

    return toret;
}

mongo_types::box3d utilities::get_query_box_by_volume(mongo_types::point3d center, float volume_percent, Data_Source_Information& data_info)
{
    mongo_types::box3d toret;

    if (volume_percent < 1.0 && volume_percent > 0.0)
    {
        // region selection goes as follows:
        // 1 - get a random existing point from the data set (so the box will cover at least one element, no matter how small
        //       the selection is.
        // 2 - extend the box to the minimum corner.  If the bounds of the box reach past the minimum, only go to the minimum point
        // 3 - extend the box to the maximum corner.  If the bounds of the box reach past the max, only go to the max point


        //auto orgin_point = data_info.get_randomPoint();

        float x_range = data_info.get_x_max() - data_info.get_x_min();
        float y_range = data_info.get_y_max() - data_info.get_y_min();
        float t_range = data_info.get_t_max() - data_info.get_t_min();

        float avg_x_range = (data_info.get_x_max() + data_info.get_x_min()) / 2.0f;
        float avg_y_range = (data_info.get_y_max() + data_info.get_y_min()) / 2.0f;
        float avg_t_range = (data_info.get_t_max() + data_info.get_t_min()) / 2.0f;

        float percentage_cover = cbrtf(volume_percent);

        if (center.get<0>() < avg_x_range)
        {
            toret.min_corner().set<0>(std::max(center.get<0>() - 0.5f * percentage_cover * x_range, data_info.get_x_min()));
            toret.max_corner().set<0>(toret.min_corner().get<0>() + x_range * percentage_cover);
        }
        else {
            toret.max_corner().set<0>(std::min(center.get<0>() + 0.5f * percentage_cover * x_range, data_info.get_x_max()));
            toret.min_corner().set<0>(toret.max_corner().get<0>() - x_range * percentage_cover);
        }

        if (center.get<1>() < avg_y_range)
        {
            toret.min_corner().set<1>(std::max(center.get<1>() - 0.5f * percentage_cover * y_range, data_info.get_y_min()));
            toret.max_corner().set<1>(toret.min_corner().get<1>() + y_range * percentage_cover);
        }
        else {
            toret.max_corner().set<1>(std::min(center.get<1>() + 0.5f * percentage_cover * y_range, data_info.get_y_max()));
            toret.min_corner().set<1>(toret.max_corner().get<1>() - y_range * percentage_cover);
        }

        if (center.get<2>() < avg_t_range)
        {
            toret.min_corner().set<2>(std::max(center.get<2>() - 0.5f * percentage_cover * t_range, static_cast<float>(data_info.get_t_min())));
            toret.max_corner().set<2>(toret.min_corner().get<2>() + y_range * percentage_cover);
        }
        else {
            toret.max_corner().set<2>(std::min(center.get<2>() + 0.5f * percentage_cover * t_range, static_cast<float>(data_info.get_t_max())));
            toret.min_corner().set<2>(toret.max_corner().get<2>() - t_range * percentage_cover);
        }
    }
    else if (volume_percent >= 1.0f)
    {
        // the result is a box with area which covers the entire area of the data set
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_max());
        toret.max_corner().set<1>(data_info.get_y_max());
        toret.max_corner().set<2>(data_info.get_t_max());
    }
    else // (percentage_cover <= 0.0f)
    {
        // the result is a box with no area
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_min());
        toret.max_corner().set<1>(data_info.get_y_min());
        toret.max_corner().set<2>(data_info.get_t_min());
    }

    return toret;
}

mongo_types::box3d utilities::get_query_region_with_element_count(std::shared_ptr<rtree_t> tree, Data_Source_Information& data_info, int64_t target_size, size_t tolerence, size_t &elements_in_region)
{
    auto center_point = data_info.get_randomPoint();
    float min_volume = 0.0f;
    float max_volume = 1.0f;

    int iteration_count = 0;
    const int MAX_ITERATION_COUNT = 50000;
    int64_t count;

    mongo_types::box3d query_region;

    do{
        float mid_volume = (max_volume + min_volume) / 2.0f;
        query_region = get_query_box_by_volume(center_point, mid_volume, data_info);
        auto cursor = tree->naive_sample_query(query_region);
        count = (int64_t) cursor.get_count();

        if (std::abs(target_size - count) < tolerence)
        {
            elements_in_region = count;
            return query_region;
        }
        if (count < target_size)
            min_volume = mid_volume;
        else
            max_volume = mid_volume;

    } while (iteration_count++ < MAX_ITERATION_COUNT);

    // if it fails, how about we return empty query
    std::cerr << "WARNING: Unable to find region for request" << std::endl;
    std::cerr << "The closest we got was " << count << " which is " << (std::abs(target_size - count)) << " different" << std::endl;
    elements_in_region = count;
    return query_region;
}


mongo_types::box3d utilities::get_random_insertion_box(long seed, float percentage_cover, Data_Source_Information& data_info)
{
    mongo_types::box3d toret;

    if (percentage_cover < 1.0f && percentage_cover > 0.0f)
    {
        std::default_random_engine generator;
        generator.seed(seed);

        // use = because the t_range performs conversation
        float x_range = data_info.get_x_max() - data_info.get_x_min();
        float y_range = data_info.get_y_max() - data_info.get_y_min();
        float t_range = data_info.get_t_max() - data_info.get_t_min();

        float half_cover{ percentage_cover * 0.5f };

        // for some reason, the mongo type box3d contains floats for each of the dimensions
        std::uniform_real_distribution<float> x_dist(data_info.get_x_min() + (half_cover * x_range), data_info.get_x_max() - (half_cover * x_range));
        std::uniform_real_distribution<float> y_dist(data_info.get_y_min() + (half_cover * y_range), data_info.get_y_max() - (half_cover * y_range));
        std::uniform_real_distribution<float> t_dist(data_info.get_t_min() + (half_cover * t_range), data_info.get_t_max() - (half_cover * t_range));

        float rand_x{ x_dist(generator) };
        float rand_y{ y_dist(generator) };
        float rand_t{ t_dist(generator) };

        toret.min_corner().set<0>(rand_x - half_cover);
        toret.min_corner().set<1>(rand_y - half_cover);
        toret.min_corner().set<2>(rand_t - half_cover);

        toret.max_corner().set<0>(rand_x + half_cover);
        toret.max_corner().set<1>(rand_y + half_cover);
        toret.max_corner().set<2>(rand_t + half_cover);
}
    else if (percentage_cover >= 1.0f)
    {
        // the result is a box with area which covers the entire area of the data set
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_max());
        toret.max_corner().set<1>(data_info.get_y_max());
        toret.max_corner().set<2>(data_info.get_t_max());
    }
    else // (percentage_cover <= 0.0f)
    {
        // the result is a box with no area
        toret.min_corner().set<0>(data_info.get_x_min());
        toret.min_corner().set<1>(data_info.get_y_min());
        toret.min_corner().set<2>(data_info.get_t_min());

        toret.max_corner().set<0>(data_info.get_x_min());
        toret.max_corner().set<1>(data_info.get_y_min());
        toret.max_corner().set<2>(data_info.get_t_min());
    }

    return toret;
}

long utilities::generate_seed()
{
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
    std::chrono::system_clock::duration dtn = tp.time_since_epoch();

    return dtn.count();
}

std::vector<mongo_types::entry> utilities::get_random_entry_vector(const mongo_types::box3d &region, size_t count, long seed)
{
    std::default_random_engine generator;
    generator.seed(seed);
    std::vector<mongo_types::entry> to_ret;
    to_ret.reserve(count);

    for (int i = 0; i < count; i++)
        to_ret.push_back(get_random_entry(region, generator));
 
    // this uses the move constructor, so the time to return this data element should not be excessive
    return to_ret;
}


mongo_types::entry utilities::get_random_entry(const mongo_types::box3d &region, std::default_random_engine &rd)
{
    // the code for the rtree does some crazy conversion between types (float <-> int)
    // because of this, the bounties will be of type float, but the time is actually
    // an integer.  To fix this, we will convert back to integer when generating the data elements

    static long oid_gen_temp = 1L;

    std::uniform_real_distribution<float> x_dist(region.min_corner().get<0>(), region.max_corner().get<0>());
    std::uniform_real_distribution<float> y_dist(region.min_corner().get<1>(), region.max_corner().get<1>());
    std::uniform_int_distribution<int>    t_dist(std::floor(region.min_corner().get<2>()), std::ceil(region.max_corner().get<2>()));
    //std::uniform_int_distribution<unsigned long> oid_gen(std::numeric_limits<unsigned long>::min(), std::numeric_limits<unsigned long>::max());

    float rand_x{ x_dist(rd) };
    float rand_y{ y_dist(rd) };
    int   rand_t{ t_dist(rd) };
    char rand_oid[32];
    //memset(rand_oid, '0', 24);
    //snprintf(rand_oid, 25, "%lx%lx", oid_gen(rd), oid_gen(rd));
    snprintf(rand_oid, 25, "%012lx%012lx", oid_gen_temp, oid_gen_temp);

    oid_gen_temp++;

    return mongo_types::entry(rand_x, rand_y, rand_t, rand_oid);
}