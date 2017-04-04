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

TDECL
struct naive_sample_query_cursor
{
    using node_type = node TARGS;
    using entry_t = typename node_type::entry_t;

    template<typename Geometry>
    naive_sample_query_cursor(Geometry const& query, entry_t & root_entry, BlockManager & block_manager, RNG_DEV & rng_dev)
        : block_manager(block_manager)
        , rng(rng_dev())
    {
        query_decomposer<Geometry> qd(query, *this);
        root_entry.apply_visitor(qd);

        count = values.size();
        for(auto const& n : nodes)
            count += n.subtree_size;
    }

    template<typename OutIter>
    void
    get_samples(size_t sample_size, OutIter out_iter) {
        sampler<OutIter, decltype(rng)> s(*this, out_iter, rng);
        s.get_samples(sample_size);
    }

    size_t get_count(void) const { return count; }
    size_t get_node_count(void) const { return std::distance(nodes.begin(), nodes.end()); }
    size_t get_value_count(void) const { return values.size(); }

    Stats get_stats(void) const { return stats; }
    void reset_stats(void) { stats = Stats(); }
    size_t get_io_cost(void) const { return io_cost; }

private:

    template<typename Geometry>
    struct query_decomposer
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

        using cursor_type = naive_sample_query_cursor;

        query_decomposer(Geometry const& query, cursor_type & cursor)
            : base_t(cursor.block_manager)
            , cursor(cursor)
            , query(query)
        { }

        void apply (internal_node_type & node, entry_t & entry) {
            visit_node(node, entry);
            ++ cursor.stats.internal_nodes;
        }
        void apply (leaf_node_type & node, entry_t & entry) {
            visit_node(node, entry);
            ++ cursor.stats.leaf_nodes;
        }
        void apply (io_internal_node_type & node, entry_t & entry) {
            node.load_children_and_buffer_from_blocks(entry, block_manager);
            visit_node(node, entry);
            ++ cursor.stats.io_internal_nodes;
        }

        void apply (io_leaf_node_type & node, entry_t & entry) {
            node.load_from_blocks(entry, cursor.block_manager);
            for(auto const& v : node.values)
            {
                if(bg::covered_by(v.get_point(), query))
                    cursor.values.push_back(v);
            }
            ++ cursor.stats.io_leaf_nodes;
        }

        template <typename NodeType>
        void visit_node(NodeType & node, entry_t const& entry) {
            for(auto & child_entry : node.children) 
            {
                if(bg::covered_by(child_entry.bbox, query))
                {
                    cursor.nodes.push_back(child_entry);
                }
                else if (bg::intersects(child_entry.bbox, query))
                {
                    child_entry.apply_visitor(*this);
                }
            }
        }

        cursor_type & cursor;
        Geometry const query;
    };

    template<typename OutIter, typename URNG>
    struct sampler
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

        using cursor_type = naive_sample_query_cursor;

        sampler(cursor_type & cursor, OutIter out_iter, URNG & rng)
            : base_t(cursor.block_manager)
            , cursor(cursor)
            , out_iter(out_iter)
            , rng(rng)
        { }

        void get_samples(size_t sample_size)
        {
            if(sample_size == 0) return;
            if(cursor.count == 0) return;

            size_t cost0 = block_manager.get_stats().cost();

            size_t samples_from_values = next_sample_size(sample_size, cursor.values.size(), cursor.count, rng);
            if(samples_from_values > 0)
            {
                sample_from_values(cursor.values.begin(), cursor.values.end(), samples_from_values);
            }

            if(sample_size > samples_from_values)
            {
                sample_from_entries(cursor.nodes.begin(), cursor.nodes.end(), 
                    cursor.count - cursor.values.size(), 
                    sample_size - samples_from_values);
            }

            cursor.io_cost += block_manager.get_stats().cost() - cost0;
        }

        template<typename ValueIter>
        void sample_from_values(ValueIter first, ValueIter last, size_t sample_size)
        {
            std::uniform_int_distribution<size_t> dist(0, (size_t)std::distance(first, last) - 1);
            for(size_t i = 0; i < sample_size; ++i)
            {
                *out_iter = first[dist(rng)];
                ++out_iter; 
            } 
        }

        template<typename Iter>
        void sample_from_entries(Iter first, Iter last, size_t subtree_size, size_t sample_size)
        {
            auto iter = first;
            while(iter != last)
            {
                if(sample_size == 0) return;

                // sample size on this node
                size_t s = next_sample_size(sample_size, iter->subtree_size, subtree_size, rng);

                if(s > 0)
                {
                    apply_arg.sample_size = s;
                    iter->apply_visitor(*this);
                    sample_size -= s;
                }

                subtree_size -= iter->subtree_size;
                ++iter;
            }

            assert(sample_size == 0);
        }

        void apply (internal_node_type & node, entry_t & entry) {
            visit_node(node, entry);
            ++ cursor.stats.internal_nodes;
        }
        void apply (leaf_node_type & node, entry_t & entry) {
            visit_node(node, entry);
            ++ cursor.stats.leaf_nodes;
        }
        void apply (io_internal_node_type & node, entry_t & entry) {
            node.load_children_and_buffer_from_blocks(entry, block_manager);
            visit_node(node, entry);
            ++ cursor.stats.io_internal_nodes;
        }

        void apply (io_leaf_node_type & node, entry_t & entry) {
            assert(apply_arg.sample_size > 0);
            node.load_from_blocks(entry, cursor.block_manager);
            sample_from_values(node.values.begin(), node.values.end(), apply_arg.sample_size);
            ++ cursor.stats.io_leaf_nodes;
        }

        template <typename NodeType>
        void visit_node(NodeType & node, entry_t const& entry) {
            assert(apply_arg.sample_size > 0);
            sample_from_entries(node.children.begin(), node.children.end(), entry.subtree_size, apply_arg.sample_size);
        }

        // transfer information for apply
        struct {
            size_t sample_size; 
        } apply_arg;

        cursor_type & cursor;
        OutIter out_iter;
        URNG & rng;
    };

    std::list<entry_t> nodes; // nodes that completely fall into the query range
    std::vector<Value> values; // (some) values in the query range, which are not covered by nodes
    size_t count;
    BlockManager & block_manager;

    Stats stats;
    size_t io_cost = 0;
    RNG rng;
};

} // namespace rtree

#undef TDECL
#undef TARGS
