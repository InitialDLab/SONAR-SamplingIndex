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

#include <string>
#include <unordered_set>
#include <random>

#include <boost/geometry/geometry.hpp>

#include "rtree/rtree.h"

//#include <tpie/tpie.h>
//#include <tpie/memory.h>
//#include <tpie/file_stream.h>
//#include <tpie/sort.h>
//#include <tpie/progress_indicator_null.h>

namespace level_sampling {

namespace bg = boost::geometry;

using rtree::IOLayersParameters;
using rtree::Stats;

template<
    typename Value,
    typename SampleValue = Value,
    typename Box = bg::model::box<bg::model::point<float, 3, bg::cs::cartesian>>,
    size_t MaxFanout = 16,
    size_t MinFanout = MaxFanout / 4,
    typename HilbertValueComputer = hilbert::HilbertValueComputer<unsigned int, 3, 8>
>
struct level_sampling 
{
    // raw data
    using rtree_type = ::rtree::rtree<Value, Value, Box, 0, MaxFanout, MinFanout, HilbertValueComputer>;
    using io_layers_type = typename rtree_type::io_layers_type;
    // samples
    using sample_rtree_type = ::rtree::rtree<SampleValue, SampleValue, Box, 0, MaxFanout, MinFanout, HilbertValueComputer>;
    using sample_io_layers_type = typename sample_rtree_type::io_layers_type;

    static constexpr size_t MIN_TREE_SIZE = 1024;

    template <typename Iterator>
    static
    void
    build(Iterator first, Iterator last, std::string const& filename, IOLayersParameters const& parameters = IOLayersParameters())
    {
        IOLayersParameters cur_parameters = parameters;
        // this value is for those "disk-based" trees, don't set it too large
        cur_parameters.max_top_layer_io_node_count = 16;

        int cur_level = 0;
        int min_mem_level = -1;
        std::vector<size_t> tree_total_sizes;
        rtree_type::build_io_layers(first, last, filename + "-" + std::to_string(cur_level), cur_parameters);
        tree_total_sizes.push_back(rtree_type::estimate_total_size(std::distance(first, last), cur_parameters));

        std::vector<SampleValue> samples;
        std::default_random_engine rng;
        std::uniform_int_distribution<> coin_dist(0, 1);
        while(true)
        {
            ++cur_level;
            if(cur_level == 1)
            {
                // sample each raw value 
                for(auto iter = first; iter != last; ++iter)
                {
                    if(coin_dist(rng))
                        samples.push_back(*iter);
                }
            }
            else
            {
                // sample existing samples
                auto out_iter = samples.begin();
                for(auto iter = samples.begin(); iter != samples.end(); ++iter)
                {
                    if(coin_dist(rng))
                    {
                        *out_iter = std::move(*iter);
                        ++out_iter;
                    }
                }
                samples.erase(out_iter, samples.end()); 
            }

            // build rtree
            sample_rtree_type::build_io_layers(
                samples.begin(), 
                samples.end(), 
                filename + "-" + std::to_string(cur_level), 
                cur_parameters);

            tree_total_sizes.push_back(sample_rtree_type::estimate_total_size(samples.size(), cur_parameters));

            if(samples.size() < parameters.max_top_layer_io_node_count / 2)
            {
                if(min_mem_level == -1)
                    min_mem_level = cur_level;
            }

            if(samples.size() < MIN_TREE_SIZE)
            {
                // save metadata
                std::ofstream fout(filename + ".metadata");
                dump_value(fout, cur_level);
                dump_value(fout, min_mem_level);
                dump_array(fout, tree_total_sizes);
                break;
            }
        }
    }

    static
    void
    build(std::string const& in_filename, std::string const& out_filename,
            std::function<Value(const std::string&)> ReadConverter,
            size_t tpie_allowed_memory,
            IOLayersParameters const& parameters = IOLayersParameters())
    {
        IOLayersParameters cur_parameters = parameters;
        cur_parameters.max_top_layer_io_node_count = 16;

        int cur_level = 0;
        int min_mem_level = -1;
        std::vector<size_t> tree_total_sizes;

        std::string tmp_filename1, tmp_filename2;
        {
            // generate random file names
            std::uniform_int_distribution<unsigned long> dist(std::numeric_limits<unsigned long>::min(), std::numeric_limits<unsigned long>::max());
            std::default_random_engine rnd(std::chrono::system_clock::now().time_since_epoch().count());
            tmp_filename1 = std::string("./staging") + std::to_string(dist(rnd)) + ".tmp";
            do {
                tmp_filename2 = std::string("./staging") + std::to_string(dist(rnd)) + ".tmp";
            } while(tmp_filename2 == tmp_filename1);
        }

        // build the first rtree
        // tpie should be initialzed here, could be ugly!
        rtree_type::build_io_layers(in_filename, out_filename + "-" + std::to_string(cur_level), 
                ReadConverter, tpie_allowed_memory, nullptr, cur_parameters);

        // convert the data file into samples
        // we don't actually store this io_layers, it's just used to store and sample the data
        auto tmp_io_layers = sample_io_layers_type::create("", cur_parameters);
        size_t element_count = tmp_io_layers->convert_data_file(in_filename, tmp_filename1, ReadConverter);

        tree_total_sizes.push_back(rtree_type::estimate_total_size(element_count, cur_parameters));

        while(true)
        {
            ++cur_level;
            // sample the data file with probability 0.5
            element_count = tmp_io_layers->sample_data_file(tmp_filename1, tmp_filename2);
            std::swap(tmp_filename1, tmp_filename2);

            // build rtree
            sample_io_layers_type::create(out_filename + "-" + std::to_string(cur_level), cur_parameters)
                ->build_from_sorted(tmp_filename1, element_count);
            tree_total_sizes.push_back(sample_rtree_type::estimate_total_size(element_count, cur_parameters));

            //std::cerr << "level_sampling: built level... elements: " << element_count << " max " << parameters.max_top_layer_io_node_count << std::endl;

            if(element_count < parameters.max_top_layer_io_node_count / 2)
            {
                if(min_mem_level == -1)
                    min_mem_level = cur_level;
            }

            if(element_count < MIN_TREE_SIZE)
            {
                // save metadata
                std::ofstream fout(out_filename + ".metadata");
                dump_value(fout, cur_level);
                dump_value(fout, min_mem_level);
                dump_array(fout, tree_total_sizes);
                break;
            }
        }

        remove(tmp_filename1.c_str());
        remove(tmp_filename2.c_str());
    }

    level_sampling(std::string const& filename, int user_min_mem_level = -1, size_t memory_limit = 0)
        :filename(filename)
        , hilbert_value_computer(new HilbertValueComputer())
    {
        // load metadata
        {
            std::vector<size_t> tree_total_sizes;

            std::ifstream fin(filename + ".metadata");
            load_value(fin, max_level);
            load_value(fin, min_mem_level);
            load_array(fin, tree_total_sizes);
            if(user_min_mem_level != -1 && memory_limit > 0)
            {
                std::cerr << "both user_min_mem_level and memory_limit are set; useing memory_limit" << std::endl;
                user_min_mem_level = -1;
            }
            if(user_min_mem_level != -1)
            {
                assert((user_min_mem_level >= 1) && (user_min_mem_level <= max_level));
                min_mem_level = user_min_mem_level;
                // load last rtree and cache blocks
                tree_ptrs.resize(max_level+1);
                for(int level = min_mem_level; level <= max_level; ++level)
                {
                    tree_ptrs[level].reset(
                        new sample_rtree_type(filename + "-" + std::to_string(level), 
                                              true, // load in memory
                                              false,
                                              0,
                                              hilbert_value_computer

                    ));
                }
            }
            else if (memory_limit > 0)
            {
                tree_ptrs.resize(max_level+1);
                int cur_level;
                for(cur_level = max_level; cur_level >= 1; --cur_level)
                {
                    size_t s = std::min<size_t>(memory_limit, tree_total_sizes[cur_level]);
                    tree_ptrs[cur_level].reset(
                        new sample_rtree_type(filename + "-" + std::to_string(cur_level), 
                            false, // load everything in memory
                            false, // load mem_nodes
                            s,
                            hilbert_value_computer
                    ));
                    memory_limit -= s;
                    if(memory_limit == 0) break;
                }
                if(cur_level == 0)
                {
                    assert(memory_limit > 0);
                    tree0_ptr.reset(
                        new rtree_type(filename + "-" + std::to_string(cur_level),
                            false,
                            false,
                            memory_limit,
                            hilbert_value_computer
                        ));
                }
                min_mem_level = cur_level;
            }
        }

    }

    void ensure_tree(int level) { 
        assert(level <= max_level);
        if(level < 0) return;
        if(level >= min_mem_level) return;
        if(level == 0) 
        {
            if(tree0_ptr) return;
            tree0_ptr.reset(new rtree_type(
                filename + "-" + std::to_string(level),
                false, false, 0, hilbert_value_computer
            ));
        }
        else
        {
            if(tree_ptrs[level]) return;
            tree_ptrs[level].reset(new sample_rtree_type(
                filename + "-" + std::to_string(level),
                false, false, 0, hilbert_value_computer
            ));
        }
    }

    template<typename Geometry>
    struct sample_query_cursor
    {
        sample_query_cursor(Geometry const& query, level_sampling & ls)
            :query(query)
            ,ls(ls)
            ,cur_level(ls.max_level)
        { }


        // return the actual number of samples returned
        template<typename OutIter>
        size_t 
        get_samples(size_t sample_size, OutIter out_iter) {
            size_t returned_sample_count = 0;
            while(sample_size > 0)
            {
                if(buffered_samples.empty())
                {
                    ls.ensure_tree(cur_level);

                    // load next tree
                    if(cur_level == -1)
                    {
                        // fail
                        break;
                    }
                    else if(cur_level == 0)
                    {
                        // load raw tree
                        auto & ptr = ls.tree0_ptr;
                        auto cost0 = ptr->get_block_manager().get_stats().cost();
                        stats += ptr->range_report(query, std::back_inserter(buffered_samples));
                        ptr->flush_cache();
                        io_cost += ptr->get_block_manager().get_stats().cost() - cost0;
                    }
                    else if(cur_level >= ls.min_mem_level)
                    {
                        // load last rtree
                        auto & cur_tree = ls.tree_ptrs[cur_level];

                        auto cost0 = cur_tree->get_block_manager().get_stats().cost();
                        auto query_stats = cur_tree->range_report(query, std::back_inserter(buffered_samples));
                        io_cost += cur_tree->get_block_manager().get_stats().cost() - cost0;

                        // for memory tree, all the nodes are in memory
                        query_stats.internal_nodes += query_stats.leaf_nodes;
                        query_stats.leaf_nodes = query_stats.io_internal_nodes + query_stats.io_leaf_nodes;
                        query_stats.io_internal_nodes = 0;
                        query_stats.io_leaf_nodes = 0;
                        stats += query_stats;
                    }
                    else
                    {
                        // load next tree
                        auto & ptr = ls.tree_ptrs[cur_level];
                        auto cost0 = ptr->get_block_manager().get_stats().cost();
                        stats += ptr->range_report(query, std::back_inserter(buffered_samples));
                        ptr->flush_cache();
                        io_cost += ptr->get_block_manager().get_stats().cost() - cost0;
                    }
                    //std::cerr << "debug: level " << cur_level << " count " << buffered_samples.size() << std::endl;
                    -- cur_level;
                    last_range_report_count = buffered_samples.size();
                    std::shuffle(buffered_samples.begin(), buffered_samples.end(), rng);
                    //std::cerr << "samples: " << buffered_samples.size() << std::endl;
                    continue;
                }
                else
                {
                    auto cur_sample = buffered_samples.back();
                    buffered_samples.pop_back();

                    if(used_samples.count(cur_sample))
                        continue;

                    *out_iter = cur_sample;
                    ++out_iter;
                    used_samples.insert(cur_sample);
                    -- sample_size;
                    ++ returned_sample_count;
                }
            }

            return returned_sample_count;
        }

        size_t estimate_count(double *sd_ptr = nullptr) {
            size_t estimate = last_range_report_count;
            double p = 1.0;
            for(int l = cur_level; l != -1; --l) 
            {
                p /= 2.0;
                estimate *= 2;
            }
            if(sd_ptr) {
                *sd_ptr = std::sqrt(estimate * (1.0 / p / p - 1.0/p));
            }
            return estimate;
        }

        Stats get_stats(void) const { return stats; }
        void reset_stats(void) { stats = Stats(); }
        size_t get_io_cost(void) const { return io_cost; }

        Geometry query;
        level_sampling & ls;
        int cur_level;
        std::vector<SampleValue> buffered_samples;
        std::unordered_set<SampleValue> used_samples;
        size_t last_range_report_count = 0;
        Stats stats;
        size_t io_cost = 0;
        std::default_random_engine rng;
    };

    template<typename Geometry>
    sample_query_cursor<Geometry>
    sample_query(Geometry const& query) {
        return sample_query_cursor<Geometry>(query, *this);
    }


    void flush_cache(void) {
    }

    void insert(Value const& value) {
        int cur_level = 0;
        std::random_device rd;
        std::default_random_engine rng(rd());
        std::uniform_int_distribution<> coin_dist(0, 1);

        {
            ensure_tree(0);
            auto cost0 = tree0_ptr->get_block_manager().get_stats().cost(); 
            tree0_ptr->insert(value);
            tree0_ptr->flush_cache();
            io_cost += tree0_ptr->get_block_manager().get_stats().cost() - cost0;
        }

        for(++cur_level; cur_level <= max_level; ++cur_level)
        {
            if(coin_dist(rng))
                break;

            ensure_tree(cur_level);
            auto & ptr = tree_ptrs[cur_level];
            auto cost0 = ptr->get_block_manager().get_stats().cost(); 
            ptr->insert(value);
            if(cur_level < min_mem_level)
            {
                ptr->flush_cache();
                io_cost += ptr->get_block_manager().get_stats().cost() - cost0;
            }
        }

        // no checking for mem usage
        // so tree in mem may grow too much
    }

    bool erase(Value const& value) {
        bool erased = false;
        int cur_level = 0;
        {
            ensure_tree(0);
            auto cost0 = tree0_ptr->get_block_manager().get_stats().cost(); 
            erased = tree0_ptr->erase(value);
            io_cost += tree0_ptr->get_block_manager().get_stats().cost() - cost0;
        }

        if(!erased) return false;

        for(++cur_level; cur_level <= max_level; ++cur_level)
        {
            ensure_tree(cur_level);
            auto & ptr = tree_ptrs[cur_level];
            auto cost0 = ptr->get_block_manager().get_stats().cost(); 
            erased = ptr->erase(value);
            if(cur_level < min_mem_level)
            {
                ptr->flush_cache();
                io_cost += ptr->get_block_manager().get_stats().cost() - cost0;
            }

            // no need to go deeper
            if(!erased) return true;
        }

        return true;
    }

    std::string filename;
    std::shared_ptr<HilbertValueComputer> hilbert_value_computer;

    int min_mem_level;
    int max_level;
    std::unique_ptr<rtree_type> tree0_ptr;
    std::vector<std::unique_ptr<sample_rtree_type>> tree_ptrs;
    // for insertion & deletion
    size_t io_cost = 0;
};

} // namespace level_sampling