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

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/array.hpp>
#include <sys/mman.h>
#include <mutex>

//#include "block_cache.h"

namespace rtree {

namespace bi = boost::iostreams;

struct BlockManager;

struct Block
{
    using stream_t = bi::stream<bi::basic_array<char>>;

    Block(bid_t bid, int mode, size_t size, BlockManager & owner) 
        : bid(bid)
        , mode(mode)
        , data (new char[size])
        , stream(data, size)
        , owner(owner)
    { 
        stream.exceptions(stream_t::failbit | stream_t::badbit);
    }
    
    Block(Block && block) = default;

    ~Block();

    stream_t &
    get_stream(void) { 
        stream.seekp(0);
        stream.seekg(0);
        return stream; 
    }

    static constexpr
    int READ = 1;

    static constexpr
    int WRITE = 2;

    bid_t bid;
    int mode;
    char * data;
    stream_t stream;
    BlockManager & owner;
};

struct BlockManager
{
    static constexpr 
    size_t default_cache_size = 4096;

    static std::unique_ptr<BlockManager>
    create (std::string const& name, size_t block_size, size_t cache_size = default_cache_size);

    static std::unique_ptr<BlockManager>
    load (std::string const& name, size_t cache_size = default_cache_size);

    void save_meta_data(void);
    void load_meta_data(void);

    struct Stats {
        size_t read = 0;
        size_t write = 0;
        size_t cost (void) const { return read + write; }
    };

    Stats get_stats (void) const { return stats; }
    void reset_stats (void) { stats = Stats(); }
    
    size_t get_block_size (void) const { return block_size; }

    std::unique_ptr<Block>
    get_block(bid_t bid, int mode);

    bid_t
    allocate_blocks(size_t size);

    void 
    free_blocks(bid_t bid, size_t size);

    void
    flush_cache(void) {
        // if we are not using the memory mapped file we can't control
        // how data is flushed.
        if(mp_data_memory != nullptr)
        {
            msync(mp_data_memory, m_allocated_memory_size, MS_ASYNC);
        }
    }

    ~BlockManager() {
        save_meta_data();
        close(data_file_descriptor);
        if(mp_data_memory != nullptr)
        {
            msync(mp_data_memory, m_allocated_memory_size, MS_SYNC);
            munmap(mp_data_memory, m_allocated_memory_size);
            mp_data_memory = nullptr;
            m_allocated_memory_size = 0;
        }
    }

    // 0 for unlimited
    void set_cache_capacity (size_t c)
    {
        // since we are now using memory mapped files, the cache capacity is
        // set my the OS.  when needed the cache size will shrink and it will
        // grow if the OS thinks it can accomidate.
        // This is left in here for compatibility with older code.
    }

    // the OS manages the cache.  This function is left in for compatibility
    // with older code which expects it.
    // TODO: remove this function?
    size_t get_cache_size (void) const { return 0; }

private:
    friend struct Block;

    // arguments:
    // name - the prefix of the input files to read (will read meta file and data file)
    // static_size - true if the number of blocks will never increase.  If this is true
    //               the block manager can allow multiple threads.  Otherwise, it only
    //               supports a single thread.
    BlockManager(std::string const& name, bool static_size);

    void memory_map_data();

    void read_block(Block const&);
    void write_block(Block const&);

    std::string name;
    size_t block_size;

    std::mutex m_manager_lock;

    std::fstream metadata_file;
    //std::fstream data_file;

    // we are using linux specific code to enable us to have the OS handle caching
    int data_file_descriptor;

    // a memory mapped section of memory with all the data
    size_t m_allocated_memory_size;
    void *mp_data_memory;
    bool m_static_size;

    std::map<bid_t, size_t> free_block_map; // garbage collected
    bid_t next_free_block = 1;

    Stats stats;

    //BlockCache<bid_t, Block> block_cache;
};

} //namespace rtree 
