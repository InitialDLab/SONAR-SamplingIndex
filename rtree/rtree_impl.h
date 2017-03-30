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

#define TDECL \
template<typename Value, typename SampleValue, typename Box,\
    size_t NodeSampleSize, \
    size_t MaxFanout,\
    size_t MinFanout,\
    typename HilbertValueComputer\
>

#define TARGS \
<\
    Value, SampleValue, Box, \
    NodeSampleSize, \
    MaxFanout, MinFanout, \
    HilbertValueComputer\
>

namespace rtree {

    TDECL
    rtree TARGS::rtree(std::string const& filename, bool in_memory, bool load_mem_nodes, size_t memory_limit, std::shared_ptr<HilbertValueComputer> hvc)
        : io_layers(io_layers_type::load(filename))
        , hilbert_value_computer(((bool)hvc) ? hvc : std::make_shared<HilbertValueComputer>())
        , filename(filename)
        , rng(time(nullptr))
    {
        if(in_memory)
            io_layers->get_block_manager().set_cache_capacity(0);

        if(load_mem_nodes)
        {
            root_node_entry = mem_node_saver<Box, hilbert_value_type, Value, SampleValue>
                ::load(filename + ".memnodes");
        }
        else
        {
            std::vector<entry_t> cur_level = build_layer(
                    io_layers->get_top_layer().begin(),
                    io_layers->get_top_layer().end(),
                    MinFanout,
                    MaxFanout
                    );

            stats.leaf_nodes += cur_level.size();

            // build internal nodes
            while (cur_level.size() > 1)
            {
                cur_level = build_layer(
                        cur_level.begin(),
                        cur_level.end(),
                        MinFanout,
                        MaxFanout
                        );

                stats.internal_nodes += cur_level.size();
            }

            root_node_entry = cur_level.front();

            // build samples
            if (NodeSampleSize > 0)
            {
                sample_builder<NodeSampleSize, Box, hilbert_value_type, Value, SampleValue>
                    sb(io_layers->get_block_manager(), true);
                apply_visitor(sb);
            }
        }

        if(in_memory || memory_limit > 0)
        {
            if(in_memory && memory_limit != 0)
            {
                std::cerr << "rtree: in_memory and memory_limit both set, using memory_limit" << std::endl;
                in_memory = false;
            }
            // preload blocks
            node_loader<Box, hilbert_value_type, Value, SampleValue> nl(root_node_entry, io_layers->get_block_manager(), in_memory, memory_limit);
        }
    }

    TDECL
    rtree TARGS::~rtree()
    {
        mem_node_cleaner<Box, hilbert_value_type, Value, SampleValue> mnc(io_layers->get_block_manager());
        apply_visitor(mnc);
        delete root_node_entry.node_ptr;
    }

    TDECL
    void
    rtree TARGS::
    save_mem_nodes(void)
    {
        mem_node_saver<Box, hilbert_value_type, Value, SampleValue> mds(io_layers->get_block_manager(), 
                filename + ".memnodes");
        apply_visitor(mds);
    }


    TDECL
    template<bool UPDATE_SAMPLE>
    void
    rtree TARGS::
    insert(Value const& value)
    {
        inserter <MinFanout, MaxFanout, 
                 bm::if_<
                    bm::bool_<UPDATE_SAMPLE>,
                    bm::int_<NodeSampleSize>,
                    bm::int_<0>
                 >::type::value,
                 HilbertValueComputer, Box, hilbert_value_type, Value, SampleValue>
            ins(value, io_layers->get_block_manager(), hilbert_value_computer.get());
        root_node_entry.apply_visitor(ins);
        if (!ins.apply_ret.new_entries.empty())
        {
            // create new root
            assert(root_node_entry.is_mem_node());
            // TODO: any change that a single insertion would lead to many siblings of the root?
            assert(ins.apply_ret.new_entries.size() <= MaxFanout);

            auto * new_node = new mem_internal_node_type();

            new_node->children.swap(ins.apply_ret.new_entries);
            new_node->build_entry(root_node_entry);
            root_node_entry.node_ptr = new_node;

            sample_builder<NodeSampleSize, Box, hilbert_value_type, Value, SampleValue>
                sb(io_layers->get_block_manager());
            apply_visitor(sb);
        }
    }

    TDECL
    template<bool UPDATE_SAMPLE>
    bool
    rtree TARGS::
    erase(Value const& value)
    {
        eraser <MinFanout, MaxFanout, 
                 bm::if_<
                    bm::bool_<UPDATE_SAMPLE>,
                    bm::int_<NodeSampleSize>,
                    bm::int_<0>
                 >::type::value,
                HilbertValueComputer, Box, hilbert_value_type, Value, SampleValue>
            era(value, io_layers->get_block_manager(), hilbert_value_computer.get());
        root_node_entry.apply_visitor(era);
        return era.apply_ret.erased;
    }

    TDECL
    template<typename Iterator>
    std::vector<typename rtree TARGS::entry_t>
    rtree TARGS::
    build_layer(Iterator first, Iterator last, size_t min_fanout, size_t max_fanout)
    {
        // similiar as IOLayers::build_internal
        // but we are building mem_leaf nodes
        size_t element_left = std::distance(first, last);

        std::vector<entry_t> next_layer;
        next_layer.reserve(calc_node_count(element_left, min_fanout, max_fanout));

        auto iter = first;
        while (element_left > 0)
        {
            // determine the number of children for current node
            size_t children_count = next_fanout(element_left, min_fanout, max_fanout);
            element_left -= children_count;

#ifndef NDEBUG
            // all the child nodes must have the same type
            for (size_t i = 1; i < children_count; ++i)
            {
                assert(iter[i].type == iter->type);
            }
#endif

            entry_t cur_entry;
            mem_internal_node_type * cur_node = nullptr;
            if (iter->is_io_node())
            {
                cur_entry.type = entry_t::LEAF_TYPE;
                cur_node = new mem_leaf_node_type();
            }
            else
            {
                cur_entry.type = entry_t::INTERNAL_TYPE;
                cur_node = new mem_internal_node_type();
            }
            cur_entry.type = (iter->is_io_node() ? entry_t::LEAF_TYPE : entry_t::INTERNAL_TYPE);

            // go through the children
            size_t size = 0;
            bg::assign_inverse(cur_entry.bbox);
            for (size_t i = 0; i < children_count; ++i)
            {
                cur_node->children.push_back(*iter);
                if (i == 0)
                {
                    cur_entry.min_key = iter->min_key;
                }
                bg::expand(cur_entry.bbox, iter->bbox);
                size += iter->subtree_size;

                ++iter;
            }

            cur_entry.subtree_size = size;
            cur_entry.node_ptr = cur_node;
            next_layer.push_back(cur_entry);
        }

        return next_layer;
    }


} // namespace rtree

#undef TDECL
#undef TARGS
