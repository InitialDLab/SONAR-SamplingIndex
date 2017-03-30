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

/*
 * walk and load all the nodes
 */
template<
    typename Box, typename Key, typename Value, typename SampleValue
    >
struct node_loader
    : visitor TARGS
{
    using base_t = visitor<Box, Key, Value, SampleValue>;
    using node_type = typename base_t::node_type;
    using internal_node_type = typename base_t::internal_node_type;
    using leaf_node_type = typename base_t::leaf_node_type;
    using io_internal_node_type = typename base_t::io_internal_node_type;
    using io_leaf_node_type = typename base_t::io_leaf_node_type;
    using entry_t = typename base_t::entry_t;
    using base_t::block_manager;

    node_loader(entry_t & root_entry, BlockManager & block_manager, bool load_all, size_t memory_limit)
        : base_t(block_manager)
        , load_all(load_all)
        , memory_limit(memory_limit)
    { 
        cur_layer.push_back(&root_entry);

        while(!cur_layer.empty() && (load_all || this->memory_limit > 0))
        {
            std::random_shuffle(cur_layer.begin(), cur_layer.end());
            for(auto * p : cur_layer)
            {
                if(load_all || this->memory_limit > 0)
                {
                    p->apply_visitor(*this);
                }
                else
                    break;
            }
            if(load_all || this->memory_limit > 0)
            {
                std::swap(cur_layer, next_layer);
                next_layer.clear();
            }
            else
                break;
        }
    }

    void apply (internal_node_type & node, entry_t & entry) {
        ++ stats.internal_nodes;
        size_t node_size = sizeof(entry_t) * node.children.size() + sizeof(SampleValue) * node.samples.size();
        if(check_size(node_size))
            add_children(node, entry);
    }
    void apply (leaf_node_type & node, entry_t & entry) {
        ++ stats.leaf_nodes;
        size_t node_size = sizeof(entry_t) * node.children.size() + sizeof(SampleValue) * node.samples.size() + sizeof(Value) * node.buffer.size();
        if(check_size(node_size))
            add_children(node, entry);
    }
    void apply (io_internal_node_type & node, entry_t & entry) {
        ++ stats.io_internal_nodes;
        node.load_children_and_buffer_from_blocks(entry, block_manager);
        node.load_samples_from_blocks(entry, block_manager);

        size_t node_size = sizeof(entry_t) * node.children.size() + sizeof(SampleValue) * node.samples.size() + sizeof(Value) * node.buffer.size();

        if(check_size(node_size))
        {
            add_children(node, entry);
            node.mem_resident = true;
            entry.type = entry_t::LOADED_IO_INTERNAL_TYPE;
            entry.node_ptr = &node;
        }
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        ++ stats.io_leaf_nodes;
        node.load_from_blocks(entry, block_manager);

        size_t node_size = sizeof(Value) * node.values.size();

        if(check_size(node_size))
        {
            node.mem_resident = true;
            entry.type = entry_t::LOADED_IO_LEAF_TYPE;
            entry.node_ptr = &node;
        }
    }

    // return if this node should be loaded
    bool check_size(size_t node_size)
    {
        if(load_all || memory_limit > node_size)
        {
            memory_limit -= node_size;
            return true;
        }
        else
        {
            memory_limit = 0;
            return false;
        }
    }

    template <typename NodeType>
    void add_children(NodeType & node, entry_t const& entry) {
        for(auto & child_entry : node.children) 
            next_layer.push_back(&child_entry);
    }

    Stats stats;
    bool load_all;
    size_t memory_limit;

    std::vector<entry_t*> cur_layer, next_layer;
};

} // namespace rtree 

#undef TDECL
#undef TARGS
