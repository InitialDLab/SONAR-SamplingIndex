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
        void
        node_entry TARGS
        ::apply_visitor(visitor_type & visitor)
    {
        auto * node = node_type::create(*this);
        node->apply_visitor(visitor, *this);
        node->free_from_entry();
    }

    // need to be consistent with serialization_size
    TDECL
        void
        node_entry TARGS
        ::dump_to(std::ostream & out) const
    {
        // NOTE: the following assert has been removed because
        // we can dump memory entries to save memnodes
        // we cannot dump memory entry
        //assert(is_io_node());

        dump_value(out, type);
        dump_value(out, subtree_size);
        dump_value(out, bbox);
        dump_value(out, bid);
        dump_value(out, min_key);
    }

    // need to be consistent with serialization_size
    TDECL
        void
        node_entry TARGS
        ::load_from(std::istream & in)
    {
        load_value(in, type);

        load_value(in, subtree_size);
        load_value(in, bbox);
        load_value(in, bid);
        load_value(in, min_key);
    }
    
    TDECL
        node TARGS *
        node TARGS ::
        create(entry_t const& entry)
    {
        if (entry.is_mem_node() || entry.is_loaded_io_node())
            return entry.node_ptr;

        if (entry.type == entry_t::IO_INTERNAL_TYPE)
        {
            return new io_internal_node TARGS();
        }

        if (entry.type == entry_t::IO_LEAF_TYPE)
        {
            return new io_leaf_node TARGS();
        }

        assert(false);
        return nullptr;
    }

    TDECL
        void
        node TARGS
        ::free(entry_t const& entry, BlockManager & block_manager)
    {
        if(entry.is_mem_node())
            delete entry.node_ptr;
        else
        {
            if (entry.type == entry_t::IO_INTERNAL_TYPE)
                io_internal_node TARGS::free_blocks(entry, block_manager);

            if (entry.type == entry_t::IO_LEAF_TYPE)
                io_leaf_node TARGS::free_blocks(entry, block_manager);
        }
    }


    TDECL
        void
        internal_node TARGS
        ::build_entry(entry_t & entry) const
    {
        entry.type = entry_t::INTERNAL_TYPE;
        if(!children.empty())
            entry.min_key = children.front().min_key;
        // go through the children
        entry.subtree_size = 0;
        bg::assign_inverse(entry.bbox);
        for (auto const& child : children)
        {
            bg::expand(entry.bbox, child.bbox);
            entry.subtree_size += child.subtree_size;
        }
    }

    TDECL
        void
        leaf_node TARGS
        ::build_entry(entry_t & entry) const
    {
        base_t::build_entry(entry);
        entry.type = entry_t::LEAF_TYPE;

        // check buffer
        entry.subtree_size += buffer.size();
        for (auto const& v : buffer)
            bg::expand(entry.bbox, v.get_point());
    }

    TDECL
        void
        io_internal_node TARGS
        ::build_entry(entry_t & entry) const
    {
        base_t::build_entry(entry);
        entry.type = entry_t::IO_INTERNAL_TYPE;
    }

    TDECL
        void
        io_internal_node TARGS
        ::allocate_blocks(entry_t & entry, BlockManager & block_manager)
    {
        // 1 block for samples
        // 1 block for children & buffer
        entry.bid = block_manager.allocate_blocks(2);
    }

    TDECL
        void
        io_internal_node TARGS
        ::free_blocks(entry_t const& entry, BlockManager & block_manager)
    {
        block_manager.free_blocks(entry.bid, 3);
    }

    TDECL
        void
        io_internal_node TARGS
        ::save_to_blocks(entry_t const& entry, BlockManager & block_manager) const
    {
        save_children_and_buffer_to_blocks(entry, block_manager);
        save_samples_to_blocks(entry, block_manager);
    }

    TDECL
        void
        io_internal_node TARGS
        ::save_children_and_buffer_to_blocks(entry_t const& entry, BlockManager & block_manager) const
    {
        auto block = block_manager.get_block(children_and_buffer_bid(entry), Block::WRITE);
        auto & stream = block->get_stream();
        dump_array(stream, children);
        stream.seekp(buffer_offset());
        dump_array(stream, buffer);
    }

    TDECL
        void
        io_internal_node TARGS
        ::load_children_and_buffer_from_blocks(entry_t const& entry, BlockManager & block_manager)
    {
        if(mem_resident) return;
        auto block = block_manager.get_block(children_and_buffer_bid(entry), Block::READ);
        auto & stream = block->get_stream();
        load_array(stream, children);
        stream.seekg(buffer_offset());
        load_array(stream, buffer);
    }

    TDECL
        void
        io_internal_node TARGS
        ::save_samples_to_blocks(entry_t const& entry, BlockManager & block_manager) const
    {
        auto block = block_manager.get_block(sample_bid(entry), Block::WRITE);
        auto & stream = block->get_stream();
        dump_array(stream, samples);
    }

    TDECL
        void
        io_internal_node TARGS
        ::load_samples_from_blocks(entry_t const& entry, BlockManager & block_manager)
    {
        if(mem_resident) return;
        auto block = block_manager.get_block(sample_bid(entry), Block::READ);
        auto & stream = block->get_stream();
        load_array(stream, samples);
    }

    TDECL
        void
        io_leaf_node TARGS
        ::build_entry(entry_t & entry) const
    {
        entry.type = entry_t::IO_LEAF_TYPE;
        entry.subtree_size = values.size();

        // go through the values
        bg::assign_inverse(entry.bbox);
        for (auto const& v : values)
            bg::expand(entry.bbox, v.get_point());
    }

    TDECL
        void
        io_leaf_node TARGS
        ::allocate_blocks(entry_t & entry, BlockManager & block_manager)
    {
        entry.bid = block_manager.allocate_blocks(1);
    }

    TDECL
        void
        io_leaf_node TARGS
        ::free_blocks(entry_t const& entry, BlockManager & block_manager)
    {
        block_manager.free_blocks(entry.bid, 1);
    }

    TDECL
        void
        io_leaf_node TARGS
        ::save_to_blocks(entry_t const& entry, BlockManager & block_manager) const
    {
        auto block = block_manager.get_block(entry.bid, Block::WRITE);
        auto & stream = block->get_stream();
        // no need to dump size
        // the value is saved in the entry
        for (auto const& v : values)
            dump_value(stream, v);
    }

    TDECL
        void
        io_leaf_node TARGS
        ::load_from_blocks(entry_t const& entry, BlockManager & block_manager)
    {
        if(mem_resident) return;
        auto block = block_manager.get_block(entry.bid, Block::READ);
        auto & stream = block->get_stream();
        values.resize(entry.subtree_size);
        for (auto & v : values)
            load_value(stream, v);
    }
} // namespace rtree

#undef TDECL
#undef TARGS
