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
 * node types for rtree
 *
 * by Lu Wang 2014
 */
#pragma once

#define TDECL template <typename Box, typename Key, typename Value, typename SampleValue>
#define TARGS <Box, Key, Value, SampleValue>

namespace rtree {

constexpr size_t MAX_IO_FANOUT = 16;
constexpr size_t MIN_IO_FANOUT = 4;


using bid_t = size_t;
constexpr bid_t INVALID_BID = 0;

struct BlockManager;

TDECL struct node;
TDECL struct internal_node;
TDECL struct leaf_node;
TDECL struct io_internal_node;
TDECL struct io_leaf_node;

/*
 * A `pointer` to a node, either in memory or disk
 * sometimes we need only subtree_size or other info from the node,
 * and we don't want to read the whole node (samples, children, buffer etc) for only these info
 * therefore the info are stored separately, at the parent node of this node
 *
 * Note that we always access a node via the pointer from its parent, 
 * so there is no sync issues
 *
 * almost all methods of the nodes have a reference to the corresponding `node_entry` as an argument
 */
TDECL
struct node_entry
{
    using node_type = node TARGS;
    using visitor_type = typename node_type::visitor_type;

    char type;

    static constexpr 
    char INTERNAL_TYPE = 0;
    static constexpr 
    char LEAF_TYPE = 1;
    static constexpr 
    char IO_INTERNAL_TYPE = 2;
    static constexpr 
    char IO_LEAF_TYPE = 3;
    static constexpr 
    char LOADED_IO_INTERNAL_TYPE = 4;
    static constexpr 
    char LOADED_IO_LEAF_TYPE = 5;


    inline bool 
    is_leaf_node(void) const { return (type & 1); }
    inline bool 
    is_internal_node(void) const { return !is_leaf_node(); }
    inline bool 
    is_io_node(void) const { return (type & 2); }
    inline bool 
    is_mem_node(void) const { return !is_io_node(); }
    inline bool
    is_loaded_io_node(void) const { return (type & 4); }

    size_t subtree_size = 0;
    Box bbox;

    // bid for disk nodes
    // pointer for mem nodes
    union {
        bid_t bid;
        node_type * node_ptr;
    };
    Key min_key;

    void apply_visitor(visitor_type & visitor);

    void dump_to(std::ostream & out) const;
    void load_from(std::istream & in);

    // need to be consistent with dump_to & load_from
    static constexpr size_t serialization_size = 
        serializer<char>::size +
        serializer<size_t>::size +
        serializer<Box>::size +
        serializer<bid_t>::size +
        serializer<Key>::size;
};

} // namespace rtree

TDECL
struct has_dump_load_method<rtree::node_entry TARGS>
{
    static constexpr bool value = true;
};


namespace rtree {

/*
 * General interface to access different types of nodes
 */
TDECL
struct visitor
{
    using node_type = node TARGS;
    using internal_node_type = internal_node TARGS;
    using leaf_node_type = leaf_node TARGS;
    using io_internal_node_type = io_internal_node TARGS;
    using io_leaf_node_type = io_leaf_node TARGS;

    using entry_t = typename node_type::entry_t;

    visitor(BlockManager & block_manager) : block_manager(block_manager) { }
    virtual ~visitor() {}
    virtual void apply(internal_node_type &, entry_t &) = 0;
    virtual void apply(leaf_node_type &, entry_t &) = 0;
    virtual void apply(io_internal_node_type &, entry_t &) = 0;
    virtual void apply(io_leaf_node_type &, entry_t &) = 0;

    BlockManager & block_manager;
};

/*
 * Overall there are 4 types of different nodes in the structure, from top to bottom:
 * 1) Internal Node (mem) 
 * 2) Leaf Node (mem)
 * 3) IO Internal Node (disk)
 * 4) IO Leaf Node (disk)
 *
 * All the nodes in the same level must be of the same type
 */

/*
 * Common interface
 */
TDECL
struct node
{
    using visitor_type = visitor TARGS;
    using entry_t = node_entry TARGS;

    virtual ~node() {}
    virtual void apply_visitor(visitor_type &, entry_t &) = 0;

    /*
     * mostly used for io nodes
     * doesn't do much things for mem nodes
     */
    static node * create(entry_t const&);
    static void free(entry_t const&, BlockManager & block_manager);

    // remove things that are not required by entry
    virtual void free_from_entry(void) { }
};

/*
 * Internal Nodes 
 * children of Internal Nodes must be Internal Nodes or Leaf Nodes
 *
 * Each Internal Node has a number of samples associated with it
 */
TDECL
struct internal_node 
    : node TARGS
{
    using base_t = node TARGS;
    using visitor_type = typename base_t::visitor_type;
    using entry_t = typename base_t::entry_t;

    virtual void apply_visitor (visitor_type & v, entry_t & e) { v.apply(*this, e); }
    
    // Build everything in entry
    void build_entry(entry_t & entry) const;

    std::vector<SampleValue> samples;
    std::vector<entry_t> children;
};

/*
 * Leaf Nodes
 * Leaf Nodes are the bottom layer of the in-memory structure
 * children of Leaf Nodes must be IO Internal Nodes or IO Leaf Nodes
 *
 * besides everything in Internal Node, each Leaf Node also has a insertion buffer
 * in order to support fast insertion
 * insertions are held until the buffer if overflowed
 */
TDECL
struct leaf_node
    : internal_node TARGS
{
    using base_t = internal_node TARGS;
    using visitor_type = typename base_t::visitor_type;
    using entry_t = typename base_t::entry_t;

    virtual void apply_visitor (visitor_type & v, entry_t & e) { v.apply(*this, e); }

    // Build everything in entry
    void build_entry(entry_t & entry) const;

    // TODO: use separate size of leaf_node and internal_node
    static size_t
    buffer_capacity(size_t block_size) {
        //return (block_size - sizeof(size_t)) / serializer<Value>::size;
        size_t offset = buffer_offset();
        assert(block_size >= offset + sizeof(size_t) + serializer<Value>::size);
        return (block_size - offset - sizeof(size_t)) / serializer<Value>::size;
    }

    // we are storing children and buffer togetther
    // offset indicates where we should store buffer inside a block
    static size_t
    buffer_offset() {
        return (sizeof(size_t) + serializer<entry_t>::size * MAX_IO_FANOUT);
    }

    bool
    buffer_overflow(size_t block_size) const {
        return buffer.size() > buffer_capacity(block_size);
    }

    using base_t::samples;
    using base_t::children;

    using value_list_t = std::vector<Value>;
    value_list_t buffer;
};

/*
 * IO Internal Nodes
 * children of IO Internal Nodes must be IO Internal Nodes or IO Leaf Nodes,
 * it's like Leaf Nodes except for all the information are stored on the disk
 *
 * Samples, buffer and children are stored into different blocks, 
 * which can be loaded into memory independently
 *
 * the size of samples/buffer/children are calculated in order to fill in a whole block
 * which is usually larger than those of mem nodes
 */
TDECL
struct io_internal_node
    : leaf_node TARGS
{
    using base_t = leaf_node TARGS;
    using visitor_type = typename base_t::visitor_type;
    using entry_t = typename base_t::entry_t;

    virtual void apply_visitor (visitor_type & v, entry_t & e) { v.apply(*this, e); }
    virtual void free_from_entry(void) { if(!mem_resident) delete this; }

    static size_t
    capacity(size_t block_size) { 
        //return (block_size - sizeof(size_t)) / serializer<entry_t>::size;
        return MAX_IO_FANOUT;
    }

    static size_t
    sample_capacity(size_t block_size) {
        return (block_size - sizeof(size_t)) / serializer<SampleValue>::size;
    }

    bool
    overflow(size_t block_size) const {
        return children.size() > capacity(block_size);
    }

    // Build everything in entry
    void build_entry(entry_t & entry) const;

    static void allocate_blocks(entry_t & entry, BlockManager & block_manager);
    static void free_blocks(entry_t const& entry, BlockManager & block_manager);
    void save_to_blocks(entry_t const& entry, BlockManager & block_manager) const;

    void save_children_and_buffer_to_blocks(entry_t const& entry, BlockManager & block_manager) const;
    void load_children_and_buffer_from_blocks(entry_t const& entry, BlockManager & block_manager);

    void save_samples_to_blocks(entry_t const& entry, BlockManager & block_manager) const;
    void load_samples_from_blocks(entry_t const& entry, BlockManager & block_manager);

    using base_t::samples;
    using base_t::buffer;
    using base_t::children;

    using base_t::buffer_offset;

private:
    static bid_t sample_bid (entry_t const& entry) { return entry.bid; }
    static bid_t children_and_buffer_bid (entry_t const& entry) { return entry.bid + 1; }

public:
    bool mem_resident = false;
};

/*
 * IO Leaf Nodes
 *
 * IO Leaf Nodes stored raw data on the disk, within one block
 */
TDECL
struct io_leaf_node
    : node TARGS
{
    using base_t = node TARGS;
    using visitor_type = typename base_t::visitor_type;
    using entry_t = typename base_t::entry_t;

    virtual void apply_visitor (visitor_type & v, entry_t & e) { v.apply(*this, e); }
    virtual void free_from_entry(void) { if(!mem_resident) delete this; }

        static size_t 
    capacity(size_t block_size) {
        return block_size / serializer<Value>::size;
    }

        bool
    overflow(size_t block_size) const {
        return values.size() > capacity(block_size);
    }

    // Build everything in entry
    // EXCEPT FOR min_key
    void build_entry(entry_t & entry) const;

    static void allocate_blocks(entry_t & entry, BlockManager & block_manager);
    static void free_blocks(entry_t const& entry, BlockManager & block_manager);

    void save_to_blocks(entry_t const& entry, BlockManager & block_manager) const;
    void load_from_blocks(entry_t const& entry, BlockManager & block_manager);

    bool mem_resident = false;
    std::vector<Value> values;
};


} // namespace rtree

#undef TDECL
#undef TARGS
