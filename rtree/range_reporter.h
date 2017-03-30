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

template<
    typename Geometry, 
    typename Iterator,
    typename Box, typename Key, typename Value, typename SampleValue
    >
struct range_reporter
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

    range_reporter(Geometry const& query, BlockManager & block_manager, Iterator out_iter)
        : query(query)
        , base_t(block_manager)
        , out_iter(out_iter)
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        visit_node(node, entry);
        ++stats.internal_nodes;
    }
    void apply (leaf_node_type & node, entry_t & entry) {
        visit_node(node, entry);
        ++stats.leaf_nodes;
    }
    void apply (io_internal_node_type & node, entry_t & entry) {
        node.load_children_and_buffer_from_blocks(entry, block_manager);
        visit_node(node, entry);
        ++stats.io_internal_nodes;
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        node.load_from_blocks(entry, block_manager);
        for(auto const& v : node.values)
        {
            if(bg::covered_by(v.get_point(), query)) 
            {
                *out_iter = v;
                ++ out_iter;
            }
        }
        ++stats.io_leaf_nodes;
    }

    template <typename NodeType>
    void visit_node(NodeType & node, entry_t const& entry) {
        for(auto & child_entry : node.children) 
        {
            if (bg::intersects(child_entry.bbox, query))
            {
                child_entry.apply_visitor(*this);
            }
        }
    }

    Geometry const query;
    Iterator out_iter;
    Stats stats;
};

} // namespace rtree 

#undef TDECL
#undef TARGS
