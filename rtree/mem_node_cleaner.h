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
struct mem_node_cleaner
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

    mem_node_cleaner(BlockManager & block_manager)
        : base_t(block_manager)
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        visit_node(node, entry);
    }
    void apply (leaf_node_type & node, entry_t & entry) {
        visit_node(node, entry);
    }
    void apply (io_internal_node_type & node, entry_t & entry) {
        assert(entry.type == entry_t::LOADED_IO_INTERNAL_TYPE);
        visit_node(node, entry);
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        assert(entry.type == entry_t::LOADED_IO_LEAF_TYPE);
        // do nothing
    }
    

    void visit_node(internal_node_type & node, entry_t & entry) {
        for(auto & child_entry : node.children) 
        {
            if(child_entry.is_mem_node() || child_entry.is_loaded_io_node())
            {
                child_entry.apply_visitor(*this);
                delete child_entry.node_ptr;
            }
        }
    }
};

} // namespace rtree 

#undef TDECL
#undef TARGS
