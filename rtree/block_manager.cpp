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
#include <stdexcept>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include "rtree/rtree.h"

namespace rtree {

Block::~Block()
{
    if(mode & WRITE)
        owner.write_block(*this);
    delete [] data;
}

std::unique_ptr<BlockManager>
BlockManager::create(std::string const& name, size_t block_size, size_t cache_size)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    // create files
    { std::ofstream _(name + ".metadata"); }
    { std::ofstream _(name + ".data"); }
   
    // since the block manager is new, we will allow it to increase the size
    // building is single threaded so this should be okay.
    BlockManager * p = new BlockManager(name, false);
    p->block_size = block_size;
    p->save_meta_data();
    return std::unique_ptr<BlockManager>(p);
}

std::unique_ptr<BlockManager>
BlockManager::load(std::string const& name, size_t cache_size)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    // block manager is created with static size.  We expect that
    // we will only be submitting queries, so we would like multi threaded
    // support but don't care about modifying the data structure at this time
    BlockManager * p = new BlockManager(name, true);
    p->load_meta_data();
    p->memory_map_data();
    return std::unique_ptr<BlockManager>(p);
}

void
BlockManager::save_meta_data (void)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    m_manager_lock.lock();

    metadata_file.seekp(0);
    dump_value(metadata_file, block_size);

    dump_value(metadata_file, free_block_map.size());
    for(auto const& p : free_block_map) 
    {
        dump_value(metadata_file, p.first);
        dump_value(metadata_file, p.second);
    }

    dump_value(metadata_file, next_free_block);

    m_manager_lock.unlock();
}

void
BlockManager::load_meta_data (void)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    m_manager_lock.lock();

    metadata_file.seekg(0);
    load_value(metadata_file, block_size);

    size_t s;
    load_value(metadata_file, s);
    free_block_map.clear();
    for(size_t i = 0; i < s; ++i)
    {
        bid_t bid;
        size_t size;
        load_value(metadata_file, bid);
        load_value(metadata_file, size);
        free_block_map.insert(std::make_pair(bid, size));
    }

    load_value(metadata_file, next_free_block);

    m_manager_lock.unlock();
}


std::unique_ptr<Block>
BlockManager::get_block(bid_t bid, int mode)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    assert(bid != INVALID_BID);
    assert(mode != 0);


    std::unique_ptr<Block> block(new Block(bid, mode, block_size, *this));

    assert(block);
    if(mode & Block::READ)
        read_block(*block);

    return block;
}

bid_t
BlockManager::allocate_blocks(size_t size)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    // this will automatically unlock when we return or thrown an exception
    std::lock_guard<std::mutex> lck(m_manager_lock);

    for(auto iter = free_block_map.begin(); iter != free_block_map.end(); ++iter)
    {
        if(iter->second >= size) 
        {
            auto bid = iter->first + iter->second - size;
            iter->second -= size;
            if(iter->second == 0)
                free_block_map.erase(iter);
            return bid;
        }
    }

    if(this->m_static_size == true)
        throw std::runtime_error("BlockManager::allocate_blocks attempted to increase the number of allocated blocks!");

    bid_t bid = next_free_block;
    next_free_block += size;
    return bid;
}

void
BlockManager::free_blocks(bid_t bid, size_t size)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    // this will automatically unlock when we return or thrown an exception
    std::lock_guard<std::mutex> lck(m_manager_lock);

    bool inserted = false;
    auto iter = free_block_map.lower_bound(bid);
    if(iter != free_block_map.begin())
    {
        auto iter2 = iter;
        --iter2;
        if(iter2->first + iter2->second == bid)
        {
            iter2->second += size;
            inserted = true;
            iter = iter2;
        }
    }

    if(!inserted)
        iter = free_block_map.insert(iter, std::make_pair(bid, size));

    // now iter points to the newly inserted entry
    // try to merge with the next entry
    auto iter2 = std::next(iter);

    if(iter2 != free_block_map.end())
    {
        if(iter2->first == bid)
        {
            throw std::runtime_error("BlockManager:: double free!");
        }

        if(iter2->first == iter->first + iter->second)
        {
            iter->second += iter2->second;
            free_block_map.erase(iter2);
        }
    }
}

BlockManager::BlockManager (std::string const& name, bool static_size)
    : name{name}
    , metadata_file(name + ".metadata", std::fstream::in | std::fstream::out | std::fstream::binary)
    , m_static_size{static_size}
    , m_allocated_memory_size{0}
    , mp_data_memory{nullptr}
    //,data_file(name + ".data", std::fstream::in | std::fstream::out | std::fstream::binary)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    std::string data_file_name = name + ".data";
    data_file_descriptor = open(data_file_name.c_str(), O_RDWR);
}

void
BlockManager::memory_map_data()
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    // we must have a static size because otherwise we have to to some more
    // complicated locking to make sure we remap the file with a large capacity
    assert(m_static_size);

    // get the size of the file
    m_allocated_memory_size = lseek(data_file_descriptor, 0, SEEK_END);

    // map the file
    mp_data_memory = mmap(NULL
                        , m_allocated_memory_size
                        , PROT_READ | PROT_WRITE
                        , MAP_SHARED
                        , data_file_descriptor
                        , 0);
}

void
BlockManager::read_block(Block const& block)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    ++ stats.read;
    if(mp_data_memory == nullptr)
    {
        m_manager_lock.lock();
        // TODO: check for errors.
        lseek(data_file_descriptor, block.bid * block_size, SEEK_SET);
        ssize_t rv = read(data_file_descriptor, block.data, block_size);

        // we should always read the correct number of bytes since the file should be aligned
        assert(rv == block_size);
        m_manager_lock.unlock();
    }
    else // if we have a memory mapped file
    {
        memcpy(block.data, &((char*)mp_data_memory)[block.bid * block_size], block_size);
    }
}

void
BlockManager::write_block(Block const& block)
{
    //std::cerr << "starting " << __func__ << " line: " << __LINE__ << std::endl;
    ++ stats.write;
    if(mp_data_memory == nullptr)
    {
        m_manager_lock.lock();
        // TODO: check for errors.
        lseek(data_file_descriptor, block.bid * block_size, SEEK_SET);
        ssize_t rv = write(data_file_descriptor, block.data, block_size);

        // we should always write the correct number of bytes.  If it is not the
        // same it is returning an error anyway
        assert(rv == block_size);
        m_manager_lock.unlock();
    }
    else // if we have a memory mapped file
    {
        memcpy(&((char*)mp_data_memory)[block.bid * block_size], block.data, block_size);
    }
}


} // namespace rtree
