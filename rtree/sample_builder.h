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
 * build samples in the rtree(with replacement)
 *
 * by Lu Wang 2014
 */
#pragma once

namespace rtree {
    /*
     * To build node.samples for the node being visited
     * existing values are used
     *
     * node.samples should be a set of fair random samples of all the values in the subtree
     *
     * to achieve this, we calculate how much samples we need from each children, and get samples from them recursively
     * samples at all the child nodes visited will also be filled (if underflowed)
     */
    template <size_t MemNodeSampleSize, typename Box, typename Key, typename Value, typename SampleValue>
    struct sample_builder
        : visitor < Box, Key, Value, SampleValue >
    {
        using base_t = visitor < Box, Key, Value, SampleValue > ;
        using node_type = typename base_t::node_type;
        using internal_node_type = typename base_t::internal_node_type;
        using leaf_node_type = typename base_t::leaf_node_type;
        using io_internal_node_type = typename base_t::io_internal_node_type;
        using io_leaf_node_type = typename base_t::io_leaf_node_type;
        using entry_t = typename base_t::entry_t;
        using base_t::block_manager;

        /*
         * visit_all:
         * if true, all the children will be visited, used in initial rtree building
         * if false, children are only visited on demand, used in normal sample querying/insertion
         */
        sample_builder(BlockManager & block_manager, bool visit_all = false)
            : base_t(block_manager)
            , cur_sample_size(0)
            , visit_all(visit_all)
        { }

        void apply(internal_node_type & node, entry_t & entry) {
            build_samples(node, entry, MemNodeSampleSize);
        }
        void apply(leaf_node_type & node, entry_t & entry) {
            build_samples(node, entry, MemNodeSampleSize);
        }
        void apply(io_internal_node_type & node, entry_t & entry) {
            node.load_children_and_buffer_from_blocks(entry, block_manager);
            node.load_samples_from_blocks(entry, block_manager);
            build_samples(node, entry, io_internal_node_type::sample_capacity(block_manager.get_block_size()));
            node.save_samples_to_blocks(entry, block_manager);
        }

        void apply(io_leaf_node_type & node, entry_t & entry) {
            assert(visit_all || cur_sample_size > 0);
            /*
             * We might reach this node with visit_all == true, and cur_sample_size == 0
             * in this case we don't need to do anything
             */
            if (cur_sample_size == 0)
                return;

            node.load_from_blocks(entry, block_manager);

            assert(entry.subtree_size == node.values.size());
            sample_vector(node.values, sample_buffer, cur_sample_size);
        }

        void build_samples(internal_node_type & node, entry_t & entry, size_t full_sample_size)
        {
            size_t subtree_size_left = entry.subtree_size;

            // how many samples for ancestors
            size_t ancestor_sample_size_left = cur_sample_size;
            assert(node.samples.size() <= full_sample_size);
            size_t my_sample_size_left = (node.samples.size() >= full_sample_size / 2)
                ? 0
                : (full_sample_size - node.samples.size())
                ;

            if(ancestor_sample_size_left + my_sample_size_left == 0)
                return;

            // visit children and fill in sample_buffer
            for (auto & child_entry : node.children)
            {
                size_t subtree_size = child_entry.subtree_size;
                if(subtree_size == 0)
                    continue;

                // sample size for ancestors
                size_t ss1 = next_sample_size(
                    ancestor_sample_size_left,
                    subtree_size,
                    subtree_size_left,
                    rng
                    );

                ancestor_sample_size_left -= ss1;

                // sample size for current node
                size_t ss2 = next_sample_size(
                    my_sample_size_left,
                    subtree_size,
                    subtree_size_left,
                    rng
                    );
                my_sample_size_left -= ss2;

                subtree_size_left -= subtree_size;

                cur_sample_size = ss1 + ss2;
                if (visit_all || (cur_sample_size > 0))
                {
                    // build samples for children
                    child_entry.apply_visitor(*this);

                    // get samples from sample_buffer
                    assert(sample_buffer.size() >= ss2);
                    auto sample_iter = sample_buffer.end() - ss2;
                    std::copy(std::make_move_iterator(sample_iter), std::make_move_iterator(sample_buffer.end()), std::back_inserter(node.samples));
                    sample_buffer.erase(sample_iter, sample_buffer.end());
                }
            }

            // sample from buffer
            if(entry.type != entry_t::INTERNAL_TYPE)
            {
                auto & n = (leaf_node_type&)node;
                assert(subtree_size_left == n.buffer.size());

                sample_vector(n.buffer, sample_buffer, ancestor_sample_size_left);
                sample_vector(n.buffer, node.samples, my_sample_size_left);

                ancestor_sample_size_left = 0;
                my_sample_size_left = 0;
            }

            assert(ancestor_sample_size_left == 0);
            assert(my_sample_size_left == 0);

            std::random_shuffle(node.samples.begin(), node.samples.end());
        }

        /*
         * Get `sample_size` random samples from src
         * and put then into dest
         */
        template<typename Vector1, typename Vector2>
        void sample_vector(Vector1 const& src, Vector2 & dest, size_t sample_size)
        {
            if(sample_size == 0)
                return;

            assert(!src.empty());
            std::uniform_int_distribution<size_t> dist(0, src.size() - 1);
            for (size_t i = 0; i < sample_size; ++i)
            {
                dest.emplace_back(src[dist(rng)]);
            }
        }


        std::default_random_engine rng;

        // used as an argument for apply()
        size_t cur_sample_size;

        std::vector<SampleValue> sample_buffer;

        bool visit_all;
    };
} // namespace rtree
