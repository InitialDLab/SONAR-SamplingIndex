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
struct finder
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

    finder(Value const& value, BlockManager & block_manager, HilbertValueComputer * hvc)
        : base_t(block_manager)
        , hvc(hvc)
        , value(value)
        , key((*hvc)(value.convert_for_hilbert()))
        , value_cmp(hvc)
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        apply_ret.found = find_from_children(node, entry); 
    }

    void apply (leaf_node_type & node, entry_t & entry) {
        apply_ret.found = find_from_buffer(node, entry) || find_from_children(node, entry);
    }

    void apply (io_internal_node_type & node, entry_t & entry) {
        node.load_buffer_from_blocks(entry, block_manager);
        apply_ret.found = find_from_buffer(node, entry);

        if(apply_ret.found)
            return;

        node.load_children_from_blocks(entry, block_manager);
        apply_ret.found = find_from_children(node, entry); 
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        node.load_from_blocks(entry, block_manager);

        for(auto iter = node.values.begin(); iter != node.values.end(); ++iter)
        {
            if(*iter == value)
            {
                apply_ret.found = true;
                return;
            }
        }
        apply_ret.found = false;
    }

private:
    bool find_from_buffer(leaf_node_type & node, entry_t entry) {
        for(auto & v : node.buffer)
            if(v == value)
                return true;

        return false;
    }

    bool find_from_children(internal_node_type & node, entry_t entry) {
        assert(!node.children.empty());

        // find a child to search 
        auto iter = std::upper_bound(node.children.begin(), node.children.end(), key,
            [](Key const& k, entry_t const& entry) -> bool {
                return k < entry.min_key;
            });

        while(iter != node.children.begin())
        {
            -- iter;
            iter->apply_visitor(*this);

            if(apply_ret.found) 
                return true;

            if(iter->min_key < key)
                break;
        }
        return false;
    }

public:
    struct {
        bool found;
    } apply_ret;

private:
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
};



} // namespace rtree
