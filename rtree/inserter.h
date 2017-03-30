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
/*
 * Insert a value into the rtree
 *
 * by Lu Wang 2014
 */
#pragma once

namespace rtree {

template <
    size_t MinMemFanout, 
    size_t MaxMemFanout, 
    size_t MemNodeSampleSize,
    typename HilbertValueComputer, typename Box, typename Key, typename Value, typename SampleValue>
struct inserter
    : visitor<Box, Key, Value, SampleValue>
{
    using base_t = visitor<Box, Key, Value, SampleValue>;
    using node_type = typename base_t::node_type;
    using internal_node_type = typename base_t::internal_node_type;
    using leaf_node_type = typename base_t::leaf_node_type;
    using io_internal_node_type = typename base_t::io_internal_node_type;
    using io_leaf_node_type = typename base_t::io_leaf_node_type;
    using entry_t = typename base_t::entry_t;
    using base_t::block_manager;

    static constexpr bool NEED_SAMPLE = MemNodeSampleSize > 0;


    inserter(Value const& value, BlockManager & block_manager, HilbertValueComputer * hvc)
        : base_t(block_manager)
        , hvc(hvc)
        , value(value)
        , key((*hvc)(value.convert_for_hilbert()))
        , value_cmp(hvc)
        , coin_dist(0.0, 1.0)
    { }

    /*
     * For Internal Node
     * Just find the correct child to insert, and update the entries accordingly
     */
    void apply (internal_node_type & node, entry_t & entry) {
        assert(!buffer_flushing);
        assert(!node.children.empty());

        auto compare = [](entry_t const& k, entry_t const& entry) -> bool {
            return k.min_key < entry.min_key;
        };

        assert(std::is_sorted(node.children.begin(), node.children.end(), compare));

        // find a child to insert
        auto iter = std::upper_bound(node.children.begin(), node.children.end(), key,
            [](Key const& k, entry_t const& entry) -> bool {
            return k < entry.min_key;
        });

        if(iter != node.children.begin())
            -- iter;
        
        bg::expand(entry.bbox, value.get_point());
        ++entry.subtree_size;
        if(NEED_SAMPLE)
            update_samples(node, entry);

        assert(apply_ret.new_entries.empty());
        iter->apply_visitor(*this);

        // merge new nodes if the child has been split
        if(!apply_ret.new_entries.empty())
        {
            node.children.insert(std::next(iter), std::make_move_iterator(apply_ret.new_entries.begin()), std::make_move_iterator(apply_ret.new_entries.end()));
            apply_ret.new_entries.clear();
        }

        // check overflow
        if(node.children.size() > MaxMemFanout) 
        {
            split_node(node, entry, MinMemFanout, MaxMemFanout);
        }
    }

    /*
     * For Leaf Node
     *
     * Just add the value into the buffer
     * The buffer is flushed when overflowed:
     * We sort the buffer and insert them into the child nodes (with a linear scan on the children list)
     */
    void apply (leaf_node_type & node, entry_t & entry) {
        assert(!buffer_flushing);

        bg::expand(entry.bbox, value.get_point());
        ++entry.subtree_size;
        if(NEED_SAMPLE)
            update_samples(node, entry);

        node.buffer.push_back(value);
        if(node.buffer_overflow(block_manager.get_block_size())) {
            std::sort(node.buffer.begin(), node.buffer.end(), value_cmp);

            flush_buffer(node);

            // check overflow
            if(node.children.size() > MaxMemFanout) 
            {
                split_node(node, entry, MinMemFanout, MaxMemFanout);
            }
        }
    }

    /*
     * For IO Internal Node
     *
     * It's always buffer flushing instead of inserting a single value,
     * (but it is possible that only one value from the buffer of the parent node is to be inserted here)
     *
     * It's similar as in Leaf Nodes except for:
     * - buffer, samples and children list are stored into separated blocks, only load on demand
     * - our buffer and the incoming buffer are both pre-sorted, so a linear merge is sufficient
     */
    void apply (io_internal_node_type & node, entry_t & entry) {
        assert(buffer_flushing);

        node.load_children_and_buffer_from_blocks(entry, block_manager);

        // parameters from caller 
        // indicating the incoming values
        auto flush_first = apply_arg.first;
        auto flush_last = apply_arg.last;
        size_t flush_size = std::distance(flush_first, flush_last);
        assert(flush_size > 0);

        // bbox & subtree_size
        for(auto iter = flush_first; iter != flush_last; ++iter)
            bg::expand(entry.bbox, iter->get_point());
        entry.subtree_size += flush_size;

        // update samples
        // similar with update_samples() but more complicated
        // as there are multiple incoming values
        if(NEED_SAMPLE)
        {
            node.load_samples_from_blocks(entry, block_manager);
            double update_prob = ((double)flush_size) / entry.subtree_size;
            std::uniform_int_distribution<size_t> dist(0, flush_size - 1);
            bool updated = false;
            for(auto & v : node.samples)
            {
                if(toss(update_prob))
                {
                    v = flush_first[dist(rng)];
                    updated = true;
                }
            }
            if(updated)
                node.save_samples_to_blocks(entry, block_manager);
        }

        // merge buffer
        {
            value_list_t tmp_buffer;
            tmp_buffer.reserve(node.buffer.size() + flush_size);
            std::merge(flush_first, flush_last,
                node.buffer.begin(), node.buffer.end(),
                std::back_inserter(tmp_buffer),
                value_cmp);
            node.buffer.swap(tmp_buffer);
        }

        if(node.buffer_overflow(block_manager.get_block_size())) {
            flush_buffer(node);

            if(node.overflow(block_manager.get_block_size())) 
            {
                split_node(node, entry, 1, node.capacity(block_manager.get_block_size()));
            }
        }

        node.save_children_and_buffer_to_blocks(entry, block_manager);
    }

    /*
     * IO Leaf Node
     * 
     * Simply merge the incoming values with existing ones
     */
    void apply (io_leaf_node_type & node, entry_t & entry) {
        assert(buffer_flushing);

        // parameters from caller 
        auto flush_first = apply_arg.first;
        auto flush_last = apply_arg.last;
        size_t flush_size = std::distance(flush_first, flush_last);
        assert(flush_size > 0);

        node.load_from_blocks(entry, block_manager);

        // bbox & subtree_size
        for(auto iter = flush_first; iter != flush_last; ++iter)
            bg::expand(entry.bbox, iter->get_point());
        entry.subtree_size += flush_size;

        // merge values
        {
            value_list_t tmp_values;
            tmp_values.reserve(node.values.size() + flush_size);
            std::merge(flush_first, flush_last,
                node.values.begin(), node.values.end(),
                std::back_inserter(tmp_values),
                value_cmp);
            node.values.swap(tmp_values);
        }

        assert(apply_ret.new_entries.empty());
        if(node.overflow(block_manager.get_block_size()))
        {
            size_t capacity = node.capacity(block_manager.get_block_size());

            size_t step = node.values.size();
            while(step > capacity)
                step /= 2;

            std::vector<Value> tmp_values;
            tmp_values.swap(node.values);

            size_t values_left = tmp_values.size();
            auto iter1 = tmp_values.begin();
            auto iter2 = iter1 + step;
            values_left -= step;

            // original node
            node.values.assign(std::make_move_iterator(iter1), std::make_move_iterator(iter2));
            node.build_entry(entry);
            // keep node.min_key, don't change it

            // will be saved later

            while(values_left > 0)
            {
                iter1 = iter2;
                if(values_left > step)
                {
                    iter2 = iter1 + step;
                    values_left -= step;
                }
                else
                {
                    iter2 = tmp_values.end();
                    values_left = 0;
                }

                entry_t new_entry;
                io_leaf_node_type new_node;
                new_node.values.assign(std::make_move_iterator(iter1), std::make_move_iterator(iter2));

                new_node.build_entry(new_entry);
                new_entry.min_key = (*hvc)(iter1->convert_for_hilbert());

                new_node.allocate_blocks(new_entry, block_manager);
                new_node.save_to_blocks(new_entry, block_manager);

                apply_ret.new_entries.push_back(new_entry);
            }
        }

        node.save_to_blocks(entry, block_manager);
    }

private:
    /*
     * Distribute the values in the buffer to the child nodes
     * assuming that the buffer has been sorted
     */
#if 0
    void flush_buffer(leaf_node_type & node) {
        assert(!node.children.empty());

        ++buffer_flushing;
        
        auto child_iter = node.children.begin();

        // the result after potentially splitting
        // saving all the children that have been processed.
        std::vector<entry_t> next_children;
        next_children.reserve(node.children.size());

        auto buffer_iter = node.buffer.begin();
        while(buffer_iter != node.buffer.end())
        {
            auto cur_key = (*hvc)(buffer_iter->convert_for_hilbert());
            auto child_iter2 = std::next(child_iter);
            /*
             * save old children that are less then cur_key
             */
            while((child_iter2 != node.children.end()) && (!(cur_key < child_iter2->min_key)))
            {
                next_children.push_back(std::move(*child_iter));
                ++child_iter;
                ++child_iter2;
            }
            auto buffer_iter2 = buffer_iter;
            if(child_iter2 == node.children.end())
            {
                // everything in the buffer will be append to the end of node.children
                buffer_iter2 = node.buffer.end();
            }
            else
            {
                // locate the end of the range that will be inserted right after child_iter
                while((buffer_iter2 != node.buffer.end()) 
                    && ((*hvc)(buffer_iter2->convert_for_hilbert()) < child_iter2->min_key))
                    ++buffer_iter2;
            }
            // now insert [buffer_iter1, buffer_iter2) right after child_iter
            apply_arg.first = buffer_iter;
            apply_arg.last = buffer_iter2;
            
            assert(apply_ret.new_entries.empty());
            child_iter->apply_visitor(*this);
            next_children.push_back(std::move(*child_iter));
            ++child_iter;
            
            if(!apply_ret.new_entries.empty())
            {
                next_children.insert(next_children.end(), 
                    std::make_move_iterator(apply_ret.new_entries.begin()), 
                    std::make_move_iterator(apply_ret.new_entries.end()));
                apply_ret.new_entries.clear();
            }

            buffer_iter = buffer_iter2;
        }
        // copy the rest children
        next_children.insert(next_children.end(),
            std::make_move_iterator(child_iter),
            std::make_move_iterator(node.children.end()));

        node.buffer.clear();
        node.children.swap(next_children);

        --buffer_flushing;
    }
#endif

    void flush_buffer(leaf_node_type & node) {
        assert(!node.children.empty());
        assert(!node.buffer.empty());

        ++buffer_flushing;

        auto buffer_iter = node.buffer.begin();
        auto cur_buffer_key = (*hvc)(buffer_iter->convert_for_hilbert());

        auto child_iter = node.children.begin();

        std::vector<entry_t> next_children;
        next_children.reserve(node.children.size());

        while(true)
        {
            assert(child_iter != node.children.end());
            assert((buffer_iter == node.buffer.end()) 
                    || (child_iter == node.children.begin())
                    || (!(cur_buffer_key < child_iter->min_key)));

            auto buffer_iter2 = buffer_iter;
            if(buffer_iter != node.buffer.end())
            {
                // still buffered elements pending

                assert(child_iter != node.children.end());
                auto child_iter2 = std::next(child_iter);
                if(child_iter2 == node.children.end())
                    buffer_iter2 = node.buffer.end();
                else if (cur_buffer_key < child_iter2->min_key)
                {
                    buffer_iter2 = std::next(buffer_iter);
                    while(true)
                    {
                        if(buffer_iter2 == node.buffer.end())
                            break;
                        cur_buffer_key = (*hvc)(buffer_iter2->convert_for_hilbert());
                        if(cur_buffer_key < child_iter2->min_key)
                            ++buffer_iter2;
                        else
                            break;
                    }
                }

                if(buffer_iter != buffer_iter2)
                {
                    apply_arg.first = buffer_iter;
                    apply_arg.last = buffer_iter2;
                    
                    assert(apply_ret.new_entries.empty());
                    child_iter->apply_visitor(*this);
                    buffer_iter = buffer_iter2;
                }
            }
            next_children.push_back(std::move(*child_iter));
            if(!apply_ret.new_entries.empty())
            {
                next_children.insert(next_children.end(), 
                    std::make_move_iterator(apply_ret.new_entries.begin()), 
                    std::make_move_iterator(apply_ret.new_entries.end()));
                apply_ret.new_entries.clear();
            }

            ++child_iter;
            if(child_iter == node.children.end())
                break;
        }

        node.buffer.clear();
        node.children.swap(next_children);

        --buffer_flushing;
    }

    /*
     * Split a node
     * for all nodes except for io_leaf_node
     *
     * just split the child node list into small pieces, and rebuild corresponding entries
     * Note that since splitting is triggered by buffer flushing instead of single value insertion
     * it is possible that one node is split into more than two nodes
     *
     * information of new nodes are stored in apply_ret, to be handled by the caller
     */
    template<typename Node>
    void split_node(Node & node, entry_t & entry, size_t min_fanout, size_t max_fanout) {
        assert((entry.type == entry_t::INTERNAL_TYPE) || (((leaf_node_type&)node).buffer.empty()));

        size_t step = node.children.size();
        while(step > max_fanout)
            step /= 2;

        assert(step >= min_fanout);

        std::vector<entry_t> tmp_children;
        tmp_children.swap(node.children);

        size_t children_left = tmp_children.size();
        auto iter1 = tmp_children.begin();
        auto iter2 = iter1 + step;
        assert(children_left >= step);
        children_left -= step;

        // original node
        node.children.assign(std::make_move_iterator(iter1), std::make_move_iterator(iter2));
        node.build_entry(entry);
        if(NEED_SAMPLE)
        {
            node.samples.clear();
            sample_builder<MemNodeSampleSize, Box, Key, Value, SampleValue> sb(block_manager);
            node.apply_visitor(sb, entry);
        }
        // will be saved outside this function

        while(children_left > 0)
        {
            iter1 = iter2;
            if(children_left > step)
            {
                iter2 = iter1 + step;
                children_left -= step;
            }
            else
            {
                iter2 = tmp_children.end();
                children_left = 0;
            }

            entry_t new_entry;
            Node *new_node = new Node();

            new_node->children.assign(std::make_move_iterator(iter1), std::make_move_iterator(iter2));
            new_node->build_entry(new_entry);

            new_node->samples.clear();

            // save 
            if(new_entry.is_mem_node())
            {
                new_entry.node_ptr = new_node;
            }
            else
            {
                assert(new_entry.type != entry_t::IO_LEAF_TYPE);
                auto * p = (io_internal_node_type*)new_node;
                p->allocate_blocks(new_entry, block_manager);
                p->save_to_blocks(new_entry, block_manager);
            }

            if(NEED_SAMPLE)
            {
                sample_builder<MemNodeSampleSize, Box, Key, Value, SampleValue> sb(block_manager);
                new_node->apply_visitor(sb, new_entry);
            }

            if(new_entry.is_io_node())
            {
                if(NEED_SAMPLE)
                {
                    // save samples
                    auto p = (io_internal_node_type *)new_node;
                    p->save_samples_to_blocks(new_entry, block_manager);
                }
                delete new_node;
            }

            apply_ret.new_entries.push_back(new_entry);
        }
    }

    bool toss (double prob) {
        return coin_dist(rng) < prob;
    }

    /*
     * For mem nodes only
     *
     * In order keep node.samples a set of fair random samples of the subtree
     * some of them may be replaced with the newly inserted (single) value
     *
     * entry.subtree_size must include the newly inserted value
     */
    void update_samples(internal_node_type & node, entry_t & entry) {
        if(!NEED_SAMPLE)
            return;

        // TODO: optimize with binomial distribution
        double update_prob = 1.0 / entry.subtree_size;
        for(auto & v : node.samples)
        {
            if(toss(update_prob))
                v = value;
        }
    }

    using value_list_t = typename leaf_node_type::value_list_t;
    using value_iter_t = typename value_list_t::iterator;

    struct {
        // for buffer flushing
        value_iter_t first, last;
    } apply_arg;

public:
    struct {
        // if a node has been split
        // new entries are kept here
        std::vector<entry_t> new_entries;
    } apply_ret;


private:


    /*
     * Actually this class involves two part, insert a single value and flush the buffers
     * which should happen on different nodes (mem / io)
     * this flag is to make sure that wo don't mess things up
     */
    int buffer_flushing = 0;

    HilbertValueComputer * hvc;
    Value const& value;
    Key key;

    struct ValueCmp {
        ValueCmp(HilbertValueComputer * hvc):hvc(hvc) { }
        bool operator () (Value const& v1, Value const& v2) const {
            return (*hvc)(v1.convert_for_hilbert()) < (*hvc)(v2.convert_for_hilbert());
        }
        HilbertValueComputer * hvc;
    } value_cmp;

    std::default_random_engine rng;
    std::uniform_real_distribution<double> coin_dist;
};

} // namespace rtree
