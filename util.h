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

//#include <gsl/gsl_rng.h>
//#include <gsl/gsl_randist.h>
#include <random>
#include <cassert>

inline
size_t 
next_fanout(size_t size, size_t min_fanout, size_t max_fanout)
{
    if(size >= min_fanout + max_fanout)
        return max_fanout;

    if(size < max_fanout)
    {
        return size;
    }

    return size - min_fanout;
}

inline
size_t 
calc_node_count(size_t size, size_t min_fanout, size_t max_fanout)
{
    return (size % max_fanout) 
        ? (size / max_fanout + 1)
        : (size / max_fanout) 
        ;
}

template<typename Point>
void dump_point(std::ostream & out, Point const& point)
{
    out << boost::geometry::wkt(point);
}

template<typename Box>
void dump_box(std::ostream & out, Box const& box)
{
    out << boost::geometry::wkt(box.min_corner()) 
        << ' ' << boost::geometry::wkt(box.max_corner());
}

/*
template<typename RNG>
size_t alternate_binomial(size_t n, double p, RNG & rng)
{
    if(p > 0.5)
        return n - alternate_binomial(n, 1.0-p, rng);

    std::uniform_real_distribution<float> coin_dist(0,1);
    double log_q = std::log(1.0 - p);
    size_t x = 0;
    double sum = 0;
    for(;;) {
        sum += std::log(coin_dist(rng)) / (n - x);
        if(sum < log_q)
            return x;
        ++x;
    }
}
*/

template<typename RNG>
inline
size_t
next_sample_size(size_t total_sample_size, size_t cur_subtree_size, size_t total_subtree_size, RNG & rng)
{
    if(cur_subtree_size == 0) return 0;
    if(cur_subtree_size == total_sample_size) return total_sample_size;
    if(total_sample_size < 10)
    {
        size_t s = 0;
        std::uniform_real_distribution<float> dist(0,1);
        float prob = ((float)cur_subtree_size) / total_sample_size;
        for(size_t i = 0; i < total_sample_size; ++i)
            if(dist(rng) < prob)
                ++s;
        return s;
    }

    //std::binomial_distribution<int> distribution(((double)cur_subtree_size) / total_subtree_size, total_sample_size);
    std::binomial_distribution<int> distribution(total_sample_size, ((double)cur_subtree_size) / total_subtree_size);
    return distribution(rng);

    //static gsl_rng *_gsl_rng = nullptr;
    //if(!_gsl_rng){ _gsl_rng = gsl_rng_alloc (gsl_rng_taus); }
    //return gsl_ran_binomial(_gsl_rng, ((double)cur_subtree_size)/total_subtree_size, total_sample_size);

/*
    return alternate_binomial(
        total_sample_size,
        ((double)cur_subtree_size) / total_subtree_size,
        rng
    );
*/
/*
    return std::binomial_distribution<size_t>(
        total_sample_size,
        ((double)cur_subtree_size) / total_subtree_size
    )(rng);
*/
}

template<typename RNG>
inline
void
next_sample_size_bulk(int trials, const std::vector<int64_t> &prefix_weights, std::vector<int> &output_counts, RNG & rng)
{
    assert(prefix_weights.size() == output_counts.size());

    // special case
    if (prefix_weights.size() == 1)
    {
        output_counts[0] = trials;
        return;
    }

    int64_t accum = 0;
    for (int i = 0; i < prefix_weights.size(); ++i)
    {
        output_counts[i] = next_sample_size(trials, prefix_weights[i] - accum, prefix_weights.back(), rng);
        accum = prefix_weights[i];
    }

    //std::vector<int64_t> each_trial;
    //each_trial.resize(trials);

    //std::uniform_int_distribution<int64_t> dist(0, prefix_weights.back());

    //std::generate(each_trial.begin(), each_trial.end(), [&rng, &dist]() {return dist(rng);});
    //std::sort(each_trial.begin(), each_trial.end());
    //auto last_max = each_trial.begin();
    //auto output_itr = output_counts.begin();
    //for (auto weight : prefix_weights)
    //{
    //    auto next_max = std::lower_bound(last_max, each_trial.end(), weight);
    //    *output_itr = std::distance(last_max, next_max);
    //    ++output_itr;
    //    last_max = next_max;
    //}

    //if (last_max != each_trial.end())
    //    output_counts.back() += std::distance(last_max, each_trial.end());

    //for (int64_t samples_inserted = 0; samples_inserted < trials; ++samples_inserted)
    //{
    //    int64_t r_val = dist(rng);
    //    auto i_itr = std::lower_bound(prefix_weights.begin(), prefix_weights.end(), r_val);
    //    int index = i_itr - prefix_weights.begin();
    //    ++output_counts[index];
    //}


    //std::uniform_int_distribution<int64_t> dist(0, prefix_weights.back());
    //for (int64_t samples_inserted = 0; samples_inserted < trials; ++samples_inserted)
    //{
    //    int64_t r_val = dist(rng);
    //    auto i_itr = std::lower_bound(prefix_weights.begin(), prefix_weights.end(), r_val);
    //    int index = i_itr - prefix_weights.begin();
    //    ++output_counts[index];
    //}
}
