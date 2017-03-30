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

namespace rtree {

template <
    size_t MinMemFanout, 
    size_t MaxMemFanout, 
    size_t MemNodeSampleSize,
    typename HilbertValueComputer, typename Box, typename Key, typename Value, typename SampleValue>
struct eraser
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

    eraser(Value const& value, BlockManager & block_manager, HilbertValueComputer * hvc)
        : base_t(block_manager)
        , value(value)
        , key((*hvc)(value.convert_for_hilbert()))
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        bool erased = erase_from_children(node, entry); 

        if(erased)
            post_erase(node, entry);

        apply_ret.erased = erased;
    }

    void apply (leaf_node_type & node, entry_t & entry) {
        bool erased = erase_from_buffer(node, entry) || erase_from_children(node, entry);

        if(erased)
            post_erase(node, entry);

        apply_ret.erased = erased;
    }

    void apply (io_internal_node_type & node, entry_t & entry) {
        node.load_children_and_buffer_from_blocks(entry, block_manager);

        bool erased = erase_from_buffer(node, entry) || erase_from_children(node, entry);
        if(erased)
        {
            node.save_children_and_buffer_to_blocks(entry, block_manager);

            if(NEED_SAMPLE)
                node.load_samples_from_blocks(entry, block_manager);
            post_erase(node, entry);
            if(NEED_SAMPLE)
                node.save_samples_to_blocks(entry, block_manager);
        }

        apply_ret.erased = erased;
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        node.load_from_blocks(entry, block_manager);

        for(auto iter = node.values.begin(); iter != node.values.end(); ++iter)
        {
            if(*iter == value)
            {
                if(node.values.size() > 1)
                {
                    node.values.erase(iter);
                    node.build_entry(entry);
                    node.save_to_blocks(entry, block_manager);
                }
                apply_ret.erased = true;
                return;
            }
        }

        // naive erase: no underflow checking

        apply_ret.erased = false;
    }

private:
    bool erase_from_buffer(leaf_node_type & node, entry_t entry) {
        for(auto & v : node.buffer)
        {
            if(v == value)
            {
                std::swap(v, node.buffer.back());
                node.buffer.pop_back();
                return true;
            }
        }

        return false;
    }

    bool erase_from_children(internal_node_type & node, entry_t entry) {
        assert(!node.children.empty());

        // find a child to insert
        auto iter = std::upper_bound(node.children.begin(), node.children.end(), key,
            [](Key const& k, entry_t const& entry) -> bool {
                return k < entry.min_key;
            });

        while(iter != node.children.begin())
        {
            -- iter;

            iter->apply_visitor(*this);

            if(apply_ret.erased) 
            {
                if(iter->subtree_size == 0)
                {
                    // release node pointer or blocks
                    node_type::free(*iter, block_manager);
                    node.children.erase(iter);
                }
                return true;
            }

            if(iter->min_key < key)
                break;
        }

        return false;
    }

    /*
     * correct entry
     * update samples
     */
    template<typename Node>
    void post_erase(Node & node, entry_t & entry) {
        node.build_entry(entry);

        if(NEED_SAMPLE)
        {
            for(size_t i = 0; i < node.samples.size(); ++i)
            {
                auto & v = node.samples[i];
                if(v == value)
                {
                    std::swap(v, node.samples.back());
                    node.samples.pop_back();
                }
            }

            // rebuild samples if necessary
            {
                sample_builder<MemNodeSampleSize, Box, Key, Value, SampleValue> sb(block_manager);
                node.apply_visitor(sb, entry);
            }
        }
        // naive erase: no underflow checking
    }

    using value_list_t = typename leaf_node_type::value_list_t;
    using value_iter_t = typename value_list_t::iterator;

    struct {
        // for buffer flushing
        value_iter_t first, last;
    } apply_arg;

public:
    struct {
        bool erased;
    } apply_ret;

private:
    Value const& value;
    Key key;
};



} // namespace rtree
