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

#define TDECL template <typename Box, typename Key, typename Value, typename SampleValue>
#define TARGS <Box, Key, Value, SampleValue>

namespace rtree {

template <typename Geometry, typename Box, typename Key, typename Value, typename SampleValue>
struct sample_query_cursor
{
    using node_type = node TARGS;
    using internal_node_type = internal_node TARGS;
    using io_internal_node_type = io_internal_node TARGS;
    using io_leaf_node_type = io_leaf_node TARGS;
    using entry_t = typename node_type::entry_t;

    sample_query_cursor(Geometry const& query, entry_t & root_entry, BlockManager & block_manager, RNG_DEV & rng_dev)
        : block_manager( block_manager )
        , query(query)
        , rng(rng_dev())
    {
        nodes.emplace_back(root_entry, 0);
        count = root_entry.subtree_size;
    }

    sample_query_cursor(sample_query_cursor const& sample_query_cursor) = delete;
    sample_query_cursor(sample_query_cursor && sample_query_cursor) = default;

    template<typename OutIter>
    void
    get_samples(size_t sample_size, OutIter out_iter) {
        size_t cost0 = block_manager.get_stats().cost();
        auto sample_buffer_inserter = std::back_inserter(sample_buffer);
        sampler<decltype(sample_buffer_inserter)> s(*this, sample_buffer_inserter, rng);
        while(sample_size > 0) 
        {
            if(sample_buffer.empty())
            {
                while(sample_buffer.empty())
                {
                    if(count == 0) return;
                    size_t ss = std::max<size_t>(sample_size, nodes.size() * 4);
                    s.get_samples(ss);
                }
                std::shuffle(sample_buffer.begin(), sample_buffer.end(), rng);
            }
            while((sample_size > 0) && (!sample_buffer.empty()))
            {
                *out_iter = sample_buffer.back();
                ++out_iter;
                sample_buffer.pop_back();
                --sample_size;
            }
        }
        io_cost += block_manager.get_stats().cost() - cost0;
    }

    // estimates the number of elements in the query range
    // the value should be equal to or smaller than `count`
    // return the estimated value
    // `sd_ptr` is to optionally receive an upper bound of the standard deviation
    size_t estimate_count(double * sd_ptr = nullptr) {
        count_estimator ce(*this);
        if(sd_ptr)
            *sd_ptr = sqrt(ce.variance);
        return round(ce.count);
    }

    Stats get_stats(void) const { return stats; }
    void reset_stats(void) { stats = Stats(); }
    size_t get_io_cost(void) const { return io_cost; }


private:
    struct sample_node_entry {
        entry_t node_entry;
        size_t sample_used = 0;

        sample_node_entry(entry_t const& node_entry, size_t sample_used)
            :node_entry(node_entry), sample_used(sample_used)
        { }
        sample_node_entry(entry_t && node_entry, size_t sample_used)
            :node_entry(node_entry), sample_used(sample_used)
        { }
    };

    template<typename OutIter>
    struct sampler
        : visitor TARGS
    {
        using base_t = visitor TARGS;
        using node_type = typename base_t::node_type;
        using internal_node_type = typename base_t::internal_node_type;
        using leaf_node_type = typename base_t::leaf_node_type;
        using io_internal_node_type = typename base_t::io_internal_node_type;
        using io_leaf_node_type = typename base_t::io_leaf_node_type;
        using entry_t = typename base_t::entry_t;
        using base_t::block_manager;

        using cursor_type = sample_query_cursor;

        sampler(cursor_type & cursor, OutIter out_iter, RNG & rng)
            : base_t(cursor.block_manager)
            , cursor(cursor)
            , out_iter(out_iter)
            , rng(rng)
        { }

        void get_samples(size_t sample_size)
        {
            sample_size_wanted = sample_size;
            
            // now we are filling sample buffer, and we don't need to go too deep
            //while(sample_size_wanted > 0)
            {
                if(cursor.count == 0) return;

                // the size of ground set for sampling, may contain some values out of the query range
                // cursor.count may be changed in sample_from_entries, as we eliminate out-of-range values
                // cache the value for safety
                size_t cur_count = cursor.count;

                size_t sample_size_this_round = sample_size_wanted;

                size_t samples_from_values = next_sample_size(sample_size_this_round, cursor.values.size(), cur_count, rng);
                if(samples_from_values > 0)
                {
                    sample_from_values(cursor.values.begin(), cursor.values.end(), samples_from_values);
                }

                sample_size_this_round -= samples_from_values;
                if(sample_size_this_round > 0)
                {
                    sample_from_entries(cursor.nodes.begin(), cursor.nodes.end(), 
                            cur_count - cursor.values.size(), 
                            sample_size_this_round);
                }

            }
        }

    private:
        // all the values must be in the query range
        template<typename ValueIter>
        void 
        sample_from_values(ValueIter first, ValueIter last, size_t sample_size)
        {
            //std::uniform_int_distribution<size_t> 
            boost::random::uniform_smallint<size_t> dist(0, (size_t)std::distance(first, last) - 1);
            for(size_t i = 0; i < sample_size; ++i)
            {
                *out_iter = first[dist(rng)];
                ++out_iter; 
#ifdef RSTREE_PROFILING
                ++cursor.stats.values_reported;
#endif 
            } 
            assert(sample_size_wanted >= sample_size);
            sample_size_wanted -= sample_size;
        }

        // iterators must be from `cursor.nodes`
        template<typename Iterator>
        void
        sample_from_entries(Iterator first, Iterator last, size_t subtree_size, size_t sample_size)
        {
            auto iter = first;
            while(iter != last)
            {
                if(sample_size == 0) return;

                bool do_increment = true;

                // sample size on this node
                size_t s = next_sample_size(sample_size, iter->node_entry.subtree_size, subtree_size, rng);

                if(s > 0)
                {
                    apply_arg.sample_size = s;
                    apply_arg.cur_sample_node_entry = &(*iter);
                    iter->node_entry.apply_visitor(*this);

                    if(apply_ret.sample_size_from_children > 0) 
                    {
                        assert(iter->node_entry.type != entry_t::IO_LEAF_TYPE);
                        assert(iter->node_entry.type != entry_t::LOADED_IO_LEAF_TYPE);
                        // there are not enough samples at this node
                        // remove it
                        iter = cursor.nodes.erase(iter);
                        do_increment = false;

                        // go deeper

                        // it's possible that there is no children in the query range
                        // in which case nothing will be outputted
                        // and we will need to get more samples in the next round
                        // with more accurate information
                        if(!apply_ret.children_list.empty())
                        {
                            // make a node of the first child
                            auto first_child_iter = apply_ret.children_list.begin();
                            cursor.nodes.splice(iter, apply_ret.children_list);

                            // now `first_child_iter` marks the first child node
                            // `iter` marks the next sibling, which just follows the last child node

                            sample_from_entries(first_child_iter, iter,
                                    subtree_size, apply_ret.sample_size_from_children);
                        }
                    }
                    else if(iter->node_entry.type == entry_t::IO_LEAF_TYPE || iter->node_entry.type == entry_t::LOADED_IO_LEAF_TYPE)
                    {
                        // the values must have been copied to `cursor.values`
                        // just remove this node 
                        iter = cursor.nodes.erase(iter);
                        do_increment = false;
                    }

                    sample_size -= s;
                }

                // subtree_size is the number of elements that are to be sampled (within this function)
                subtree_size -= iter->node_entry.subtree_size;
                if(do_increment)
                    ++iter;
            }

            // [first, last) is the valid range, and `subtree_size` is the size of ground set
            // it's not necessary that the sum of subtree sizes over [first, last) equals to `subtree_size`
            // so it's possible that `sample_size > 0` at this point
            // this means that those samples are out of the query range
            // so we just ignore them
        }

        void apply (internal_node_type & node, entry_t & entry) {
            get_samples_from_node(node, entry);
            if(apply_ret.sample_size_from_children > 0) 
            {
                prepare_children_list(node, entry);
                ++cursor.stats.internal_nodes;
            }
        }
        // same as above
        void apply (leaf_node_type & node, entry_t & entry) {
            get_samples_from_node(node, entry);
            if(apply_ret.sample_size_from_children > 0)
            {
                prepare_children_list(node, entry);
                ++cursor.stats.leaf_nodes;
            }
        }
        // same as above except for loading samples and children when necessary
        void apply (io_internal_node_type & node, entry_t & entry) {
            node.load_samples_from_blocks(entry, block_manager);
            get_samples_from_node(node, entry);
            if(!entry.is_loaded_io_node())
                ++cursor.stats.io_sample_nodes;
            else
                ++cursor.stats.internal_nodes;
            if(apply_ret.sample_size_from_children > 0)
            {
                node.load_children_and_buffer_from_blocks(entry, block_manager);
                prepare_children_list(node, entry);
                if(!entry.is_loaded_io_node())
                    ++cursor.stats.io_internal_nodes;
            }
        }

        void apply (io_leaf_node_type & node, entry_t & entry) {
            assert(apply_arg.sample_size > 0);
            node.load_from_blocks(entry, cursor.block_manager);
            if(!entry.is_loaded_io_node())
                ++cursor.stats.io_leaf_nodes;
            else
                ++cursor.stats.leaf_nodes;

            // filter values and save those in the query range
            size_t old_values_size = cursor.values.size();
            for(auto const& v : node.values)
            {
                if(bg::covered_by(v.get_point(), cursor.query))
                {
                    cursor.values.push_back(v);
#ifdef RSTREE_PROFILING
                    // will be counted in sample_from_values()
                    //++cursor.stats.values_reported;
#endif 
                }
#ifdef RSTREE_PROFILING
                else
                {
                    ++cursor.stats.values_rejected;
                }
                ++cursor.stats.leaf_values_scanned;
#endif 
            }

            size_t count_in_range = cursor.values.size() - old_values_size;
            // update `cursor.count` since we might have removed some elements
            cursor.count -= entry.subtree_size;
            cursor.count += count_in_range;

            // now take samples
            // this apply() function should get random sample from the whole leaf node
            // but only report those in the query range,
            // others are just ignored
            // here we directly calculate the number of samples that will be reported
            size_t sample_size = next_sample_size(apply_arg.sample_size, count_in_range, entry.subtree_size, rng);
            sample_from_values(cursor.values.begin() + old_values_size, cursor.values.end(), sample_size);

            apply_ret.sample_size_from_children = 0;
        }


        // used in apply()
        // try to get `sample_size` samples from the node
        void
        get_samples_from_node(internal_node_type & node, entry_t const& entry) {
            size_t & sample_used = apply_arg.cur_sample_node_entry->sample_used;

            size_t s = std::min<size_t>(
                node.samples.size() - sample_used,
                apply_arg.sample_size
            );

            auto iter1 = node.samples.begin() + sample_used;
            auto iter2 = iter1 + s;
            if(bg::covered_by(entry.bbox, cursor.query))
            {
                while(iter1 != iter2)
                {
                    *out_iter = *iter1;
                    assert(sample_size_wanted > 0);
                    --sample_size_wanted;
                    ++iter1;
#ifdef RSTREE_PROFILING
                    ++cursor.stats.values_reported;
#endif 
                }
            }
            else
            {
                while(iter1 != iter2)
                {
                    if(bg::covered_by(iter1->get_point(), cursor.query))
                    {
                        *out_iter = *iter1;
                        assert(sample_size_wanted > 0);
                        --sample_size_wanted;
#ifdef RSTREE_PROFILING
                        ++cursor.stats.values_reported;
#endif 
                    }
#ifdef RSTREE_PROFILING
                    else
                    {
                        ++cursor.stats.values_rejected;
                    }
                    ++cursor.stats.leaf_values_scanned;
#endif 
                    ++iter1;
                }
            }

            sample_used += s;
            apply_ret.sample_size_from_children = apply_arg.sample_size - s;
        }

        // put into `children_list `those children that might contain elements in the query range
        void
        prepare_children_list(internal_node_type & node, entry_t const& entry) {
            cursor.count -= entry.subtree_size;
            assert(apply_ret.children_list.empty());
            for(auto & child_entry : node.children)
            {
                if(bg::intersects(child_entry.bbox, cursor.query))
                {
                    apply_ret.children_list.emplace_back(child_entry, 0);
                    cursor.count += child_entry.subtree_size;
                }
            }
        }

        // transfer information for apply()
        struct {
            size_t sample_size; 
            sample_node_entry * cur_sample_node_entry;
        } apply_arg;

        // information returned by apply()
        struct {
            // for internal nodes, if the samples associated are not enough for the query
            // we need to go down to its children for more samples
            // `sample_size_from_children` is the number of samples to get from children
            size_t sample_size_from_children;

            // when `sample_size_from_children > 0`
            // we need to go on level deeper
            // `children_list` marks the child nodes that may contains elements in the query range
            std::list<sample_node_entry> children_list;
        } apply_ret;

        cursor_type & cursor;
        OutIter out_iter;

        size_t sample_size_wanted;
        RNG & rng;
    };

    struct count_estimator 
    {
        using cursor_type = sample_query_cursor;

        count_estimator(cursor_type & cursor)
            : cursor(cursor)
        { 
            count = cursor.values.size();
            variance = 0.0;

            for(auto const& sample_entry : cursor.nodes)
            {
                auto const& entry = sample_entry.node_entry;
                if(bg::covered_by(entry.bbox, cursor.query))
                {
                    // all elements are in the range
                    count += entry.subtree_size;
                    // no variance incurred
                }
                else
                {
                    // calculate the best estimation based on all the information we have
                    switch(entry.type)
                    {
                        case entry_t::INTERNAL_TYPE:
                        case entry_t::LEAF_TYPE:
                        case entry_t::LOADED_IO_INTERNAL_TYPE:
                            {
                                assert(dynamic_cast<internal_node_type*>(entry.node_ptr));
                                internal_node_type const& node = (internal_node_type&)(*entry.node_ptr);
                                if(node.samples.empty())
                                {
                                    wild_guess(entry.subtree_size);
                                }
                                else
                                {
                                    // estimate using samples
                                    size_t c = 0;
                                    for(auto const& s : node.samples)
                                    {
                                        if(bg::covered_by(s.get_point(), cursor.query))
                                            ++c;
                                    }
                                    
                                    double ss = entry.subtree_size;
                                    count += ss / node.samples.size() * c;
                                    variance += ss * ss / node.samples.size();
                                }
                            }
                            break;
                        case entry_t::IO_INTERNAL_TYPE:
                        case entry_t::IO_LEAF_TYPE:
                            // we don't want to waste IO for this
                            wild_guess(entry.subtree_size);
                            break;
                        case entry_t::LOADED_IO_LEAF_TYPE:
                            {
                                io_leaf_node_type const& node = (io_leaf_node_type&)(*entry.node_ptr);
                                for(auto const& v : node.values)
                                {
                                    if(bg::covered_by(v.get_point(), cursor.query))
                                        ++ count;
                                }
                            }
                            break;
                        default:
                            assert(false);
                            break;
                    }
                }
            }
        }

        // just estimate as size/2, without any other information given
        void wild_guess(double size) {
            count += size / 2.0;
            variance += size * size / 4.0;
        }

        double count;
        double variance;
    private:
        cursor_type & cursor;
    };

    std::list<sample_node_entry> nodes; // nodes that might have elements in the query range
    std::vector<SampleValue> values; // (some) values in the query range, which are not covered by nodes
    size_t count; // the number of values covered by nodes and values

public:
    std::vector<SampleValue> sample_buffer; // samples not returned

public:
    size_t node_count (void) const { return nodes.size(); }
    size_t value_count (void) const { return values.size(); }
    size_t ground_set_size (void) const { return count; }
    size_t sample_buffer_size (void) const { return sample_buffer.size(); }

private:

    BlockManager & block_manager;
    Geometry query;
    RNG rng;

    Stats stats;
    size_t io_cost = 0;
};

} // namespace rtree

#undef TDECL
#undef TARGS
