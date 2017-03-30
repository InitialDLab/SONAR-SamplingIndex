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
namespace rtree {

template <typename Box, typename Key, typename Value, typename SampleValue, size_t MemNodeSampleSize>
struct integrity_checker
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

    integrity_checker(BlockManager & block_manager)
        : base_t(block_manager)
    { }

    void apply (internal_node_type & node, entry_t & entry) {
        visit_internal_node(node, entry);
        ++ internal_node_count;
    }
    void apply (leaf_node_type & node, entry_t & entry) {
        visit_internal_node(node, entry);
        ++ leaf_node_count;
    }
    void apply (io_internal_node_type & node, entry_t & entry) {
        node.load_children_from_blocks(entry, block_manager);
        node.load_samples_from_blocks(entry, block_manager);
        visit_internal_node(node, entry);
        ++ io_internal_node_count;
    }

    void apply (io_leaf_node_type & node, entry_t & entry) {
        node.load_from_blocks(entry, block_manager);
        bool ok = true;
        for(auto const& v : node.values)
            ok = ok && check_bbox(v.get_point(), entry.bbox, bad_values);

        if(!ok)
            ++bad_node_count;

        ++ io_leaf_node_count;
    }

    void visit_internal_node(internal_node_type & node, entry_t const& entry)
    {
        bool ok = true;
        size_t s = 0;
        for (auto & child_entry : node.children)
        {
            ok = ok && check_bbox(child_entry.bbox, entry.bbox, bad_child_entries);
            s += child_entry.subtree_size;
        }
        for(auto & s : node.samples)
        {
            ok = ok && check_bbox(s.get_point(), entry.bbox, bad_samples);
        }

        if(s != entry.subtree_size)
        {
            ok = false;
            ++ bad_subtree_sizes;
            std::cerr << s << ' ' << entry.subtree_size << std::endl;
        }


        if(!ok)
            ++bad_node_count;

        // go through children
        for (auto & child_entry : node.children)
        {
            auto * child_node = node_type::create(child_entry);
            child_node->apply_visitor(*this, child_entry);
            child_node->free_from_entry();
        }
    }

    template<typename Geometry1, typename Geometry2>
    bool check_bbox(Geometry1 const& box1, Geometry2 const& box2, size_t & count) {
        bool ok = bg::covered_by(box1, box2);
        if(!ok)
            ++ count;
        return ok;
    }

    void summary(void) const {
        std::cerr 
            << "internal_nodes: " << internal_node_count << std::endl
            << "leaf_nodes: " << leaf_node_count << std::endl
            << "io_internal_nodes: " << io_internal_node_count << std::endl
            << "io_leaf_nodes: " << io_leaf_node_count << std::endl
            << "bad_nodes: " << bad_node_count << std::endl
            << "bad_values: " << bad_values << std::endl
            << "bad_child_entries: " << bad_child_entries << std::endl
            << "bad_samples: " << bad_samples << std::endl
            << "bad_subtree_sizes: " << bad_subtree_sizes << std::endl
            ;
    }

    size_t bad_node_count = 0;
    size_t bad_values = 0;
    size_t bad_child_entries = 0;
    size_t bad_samples = 0;
    size_t bad_subtree_sizes = 0;

    size_t internal_node_count = 0;
    size_t leaf_node_count = 0;
    size_t io_internal_node_count = 0;
    size_t io_leaf_node_count = 0;
};

} // namespace rtree
