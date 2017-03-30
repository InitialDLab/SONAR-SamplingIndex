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
struct mem_node_saver
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

    mem_node_saver(BlockManager & block_manager, std::string const& filename)
        : base_t(block_manager)
        , outf(filename.c_str(), std::ofstream::binary)
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        dump_value(outf, entry);
        dump_array(outf, node.samples);
        dump_value(outf, (size_t)node.children.size());
        for(auto & child_entry : node.children) 
            child_entry.apply_visitor(*this);
    }
    void apply (leaf_node_type & node, entry_t & entry) {
        dump_value(outf, entry);
        dump_array(outf, node.samples);
        dump_array(outf, node.buffer);
        dump_array(outf, node.children);
    }
    void apply (io_internal_node_type & node, entry_t & entry) {
        assert(false);
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        assert(false);
    }

    static
    entry_t load(std::string const& filename) {
        std::ifstream inf(filename.c_str(), std::ifstream::binary);
        assert(inf);
        return load_entry(inf);
    }

private:
    static
    entry_t load_entry(std::ifstream & inf) {
        assert(inf);
        entry_t entry;
        load_value(inf, entry);
        assert(entry.is_mem_node());
        if(entry.is_internal_node())
        {
            auto * node = new internal_node_type();
            entry.node_ptr = node;
            load_array(inf, node->samples);

            size_t children_count;
            load_value(inf, children_count);
            node->children.reserve(children_count);
            for(size_t i = 0; i < children_count; ++i)
            {
                node->children.push_back(load_entry(inf));
            }
        }
        else
        {
            // leaf
            auto * node = new leaf_node_type();
            entry.node_ptr = node;
            load_array(inf, node->samples);
            load_array(inf, node->buffer);
            load_array(inf, node->children);
        }
        return entry;
    }

    std::ofstream outf;
};

} // namespace rtree 

#undef TDECL
#undef TARGS
