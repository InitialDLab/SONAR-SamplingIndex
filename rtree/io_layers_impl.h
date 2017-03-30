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

#include <boost/timer/timer.hpp>

#include "stxxl/sorter"
#include "stxxl/stream"
#include "stxxl/stats"
#include "stxxl/timer"
#include "stxxl/io"
#include "stxxl/vector"

#include <fstream>
#include <cstdio>
#include <chrono>
#include <type_traits>

#include <stdlib.h>

#define TDECL template<typename Box, typename HilbertValueComputer, typename Value, typename SampleValue>
#define TARGS <Box, HilbertValueComputer, Value, SampleValue>

namespace rtree {

TDECL
IOLayers TARGS::~IOLayers()
{
    save_to_file();
}

TDECL
template<typename Iterator>
void
IOLayers TARGS::build(Iterator first, Iterator last)
{
    // dump information
    std::cerr << "Building IO layers..." << std::endl;
    std::cerr << "block size: " << parameters.block_size << std::endl;
    std::cerr << "leaf node capacity: " << leaf_node_type::capacity(parameters.block_size) << std::endl;
    std::cerr << "internal node capacity: " << internal_node_type::capacity(parameters.block_size) << std::endl;

    using element_type = typename std::iterator_traits<Iterator>::value_type;
    using cmp_entry_t = std::pair<Iterator, hilbert_value_type>;

    std::vector<cmp_entry_t> cmp_entrices;

    // sort raw data
    size_t element_count = std::distance(first, last);
    cmp_entrices.reserve(element_count);

    {
        // hvc might be very large, don't allocate in the stack
        std::unique_ptr<HilbertValueComputer> hvc(new HilbertValueComputer());
        while(first != last)
        {
            cmp_entrices.emplace_back(first, (*hvc)(first->convert_for_hilbert()));
            ++first;
        }
    }

    std::sort(cmp_entrices.begin(), cmp_entrices.end(), 
        [&](cmp_entry_t const& p1, cmp_entry_t const& p2)->bool{
        return p1.second < p2.second;
    });

    // build leaf nodes
    std::vector<entry_t> cur_layer = build_leaves(
        cmp_entrices.begin(),
        cmp_entrices.end(),
        leaf_node_type::capacity(parameters.block_size) * 0.5,
        leaf_node_type::capacity(parameters.block_size) * parameters.fill_ratio
    );

    // build internal nodes
    {
        size_t min_fanout = MIN_IO_FANOUT;
        size_t max_fanout = MAX_IO_FANOUT;

        // recursively build internal nodes until there are not much left
        while(cur_layer.size() > parameters.max_top_layer_io_node_count)
        {
            cur_layer = build_internal(
                cur_layer.begin(),
                cur_layer.end(),
                min_fanout,
                max_fanout
            );
        }
    }

    top_layer.swap(cur_layer);

    // will save in the deconstructor
}

TDECL
void
IOLayers TARGS::build(const std::string &inputFile, converter_t ReadConverter, size_t)
{
    // output statistical information
    std::cerr << "Building IO layers..." << std::endl;
    std::cerr << "block size: " << parameters.block_size << std::endl;
    std::cerr << "leaf node capacity: " << leaf_node_type::capacity(parameters.block_size) << std::endl;
    std::cerr << "internal node capacity: " << internal_node_type::capacity(parameters.block_size) << std::endl;

    // open temp file
    // NOTE: This file can be created in a temporary space (such as the /tmp directory)
    //  However, in order to prevent windows incompatibilities, we will create the temp file in the local
    //  directory.  creating the file in a temporary space would be needed to safely build multiple index
    //  structures at the same time, but I had issues with it being placed in a different partition without
    //  a lot of free space (<100GB of free space)
    std::string out_filename{ "./staging" };
    {
        char rand_extention[16];
        std::uniform_int_distribution<unsigned long> extention_gen(std::numeric_limits<unsigned long>::min(), std::numeric_limits<unsigned long>::max());
        std::default_random_engine rnd(std::chrono::system_clock::now().time_since_epoch().count());
        snprintf(rand_extention, 16, "%lx", extention_gen(rnd));
        out_filename += rand_extention;
        out_filename += ".tmp";
    }
    size_t element_count = convert_data_file(inputFile, out_filename, ReadConverter);
    std::cout << "done with convert data file" << std::endl;
    build_from_sorted(out_filename, element_count);
    std::cout << "done with leaf nodes" << std::endl;
    remove(out_filename.c_str());
    std::cout << "done with everything!" << std::endl;
}

TDECL
void
IOLayers TARGS::build_from_sorted(std::string const& in_filename, size_t element_count)
{
    using cmp_entry_t = builder_type;

    stxxl::syscall_file input(in_filename, stxxl::file::DIRECT | stxxl::file::RDONLY);

    // TODO: optimize vector commands
    typedef stxxl::vector<cmp_entry_t> vector_type;

    vector_type input_data(&input);

    size_t min_leaf_size = leaf_node_type::capacity(parameters.block_size) * 0.5;
    size_t max_leaf_size = leaf_node_type::capacity(parameters.block_size) * parameters.fill_ratio;

    size_t min_fanout = MIN_IO_FANOUT;
    size_t max_fanout = MAX_IO_FANOUT;

    std::cerr << "building leaf nodes" << std::endl;

    stxxl::timer leaf_construct_timer;
    leaf_construct_timer.start();

    std::vector<entry_t> cur_layer = build_leaves(
        input_data,
        element_count,
        min_leaf_size,
        max_leaf_size
        );

    leaf_construct_timer.stop();
    stxxl::timer nodes_construct_timer;
    nodes_construct_timer.start();

    std::cerr << "building internal nodes" << std::endl;

    // build internal nodes (same as before)
    // recursively build internal nodes until there are not much left
    while (cur_layer.size() > parameters.max_top_layer_io_node_count)
    {
        cur_layer = build_internal(
            cur_layer.begin(),
            cur_layer.end(),
            min_fanout,
            max_fanout
            );
    }
    top_layer.swap(cur_layer);

    nodes_construct_timer.stop();

    // the timers should be in nanosecond resolution.  we want second resolution
    last_build_statistics.node_construct_time_sec = nodes_construct_timer.seconds();
    last_build_statistics.leaf_construct_time_sec = leaf_construct_timer.seconds();
    last_build_statistics.construct_time_sec = last_build_statistics.node_construct_time_sec + last_build_statistics.leaf_construct_time_sec;
    last_build_statistics.total_time_sec = last_build_statistics.read_time_sec + last_build_statistics.sort_time_sec + last_build_statistics.construct_time_sec;
}

TDECL
size_t
IOLayers TARGS::get_top_layer_node_count(size_t element_count, IOLayersParameters const& parameters)
{
    {
        size_t max_leaf_node_size = leaf_node_type::capacity(parameters.block_size) * parameters.fill_ratio;
        element_count = (element_count - 1) / max_leaf_node_size + 1;
    }
    while(element_count > parameters.max_top_layer_io_node_count)
    {
        element_count = (element_count - 1) / MAX_IO_FANOUT + 1;
    }
    return element_count;
}

TDECL
size_t
IOLayers TARGS::convert_data_file(std::string const& in_filename, std::string const& out_filename, const converter_t& converter)
{
    // for read statistics
    stxxl::timer readtimer;
    stxxl::timer sorttimer;

    // open input file
    std::fstream in_file(in_filename.c_str(), std::ios_base::in);

    // check if the file opened correct, if not throw exception?
    std::cerr << "reading the file" << std::endl;

    std::shared_ptr<HilbertValueComputer> hvc(new HilbertValueComputer());
    using cmp_entry_t = builder_type;

    // read all data into sorting object
    struct my_comparator
    {
        my_comparator(std::shared_ptr<HilbertValueComputer> hvc)
            : m_hvc{ hvc }
        {}

        bool operator() (const cmp_entry_t& a, const cmp_entry_t& b) const
        {
            return a.second < b.second;
        }

        // HACK, since we are usually using the save hilbert value width I
        // set this value here.  This should be changed to make it more general.
        cmp_entry_t min_value() const
        {
            cmp_entry_t dummy;
            dummy.second = m_hvc->min_value();
            return dummy;
        }

        cmp_entry_t max_value() const
        {
            cmp_entry_t dummy;
            dummy.second = m_hvc->max_value();
            return dummy;
        }

        std::shared_ptr<HilbertValueComputer> m_hvc;
    };
    typedef stxxl::sorter<cmp_entry_t, my_comparator> sorter_type;

    readtimer.start();

    size_t element_count = 0;
    sorter_type data_sorter(my_comparator(hvc), /* max memory limit */ 1024 * 1024 * 1024 /* 1G? */);
    std::string str;
    while (!in_file.eof())
    {
        std::getline(in_file, str);

        // empty string are consitered null, so don't enter any data from zero length string
        if (str.size() == 0)
            continue;

        Value current_item = std::move(converter(str));
        data_sorter.push(builder_type{ current_item, (*hvc)(current_item.convert_for_hilbert()) });
        ++element_count;
    }

    readtimer.stop();

    std::cout << "sorting the file" << std::endl;
    // perform sort
    sorttimer.start();
    data_sorter.sort();
    sorttimer.stop();

    // write sorted results to file
    stxxl::syscall_file OutputFile(out_filename, stxxl::file::RDWR | stxxl::file::CREAT | stxxl::file::TRUNC );
    typedef stxxl::vector<builder_type> vector_type;

    // setup buffered writing for results
    stxxl::vector<builder_type> OutputVector(&OutputFile);
    OutputVector.resize(element_count);
    typename vector_type::bufwriter_type writer(OutputVector.begin());

    while (!data_sorter.empty())
    {
        cmp_entry_t data_item = *data_sorter;
        ++data_sorter;
        writer << data_item;
    }
    writer.finish();

    std::cout << "finished dump data" << std::endl;
    last_build_statistics.read_time_sec = readtimer.seconds();
    last_build_statistics.sort_time_sec = sorttimer.seconds();

    return element_count;
}

TDECL
size_t
IOLayers TARGS::sample_data_file(std::string const& in_filename, std::string const& out_filename)
{
    using cmp_entry_t = std::pair<Value, hilbert_value_type>;
    std::cout << "sample_data_file(std::string const& in_filename, std::string const& out_filename) called" << std::endl;
    throw "change to new file system";
    //tpie::file_stream<cmp_entry_t> in_file, out_file;
    //in_file.open(in_filename);
    //out_file.open(out_filename);
    //out_file.truncate(0);

    //std::default_random_engine rng;
    //std::uniform_int_distribution<> coin_dist(0, 1);
    //size_t element_count = 0;
    //while (in_file.can_read())
    //{
    //    if(coin_dist(rng)) 
    //    {
    //        out_file.write(in_file.read());
    //        ++element_count;
    //    }
    //    else
    //        in_file.read();
    //}
    //in_file.close();
    //out_file.close();
    //return element_count;
}



TDECL
std::unique_ptr<IOLayers TARGS>
IOLayers TARGS::create(std::string const& filename, IOLayersParameters const& parameters)
{
    // create files
    { std::ofstream _(filename + ".iolayers"); }

    IOLayers TARGS * p = new IOLayers TARGS(filename);
    p->parameters = parameters;
    p->block_manager = BlockManager::create(filename, parameters.block_size);
    return std::unique_ptr<IOLayers TARGS>(p);
}

TDECL
std::unique_ptr<IOLayers TARGS>
IOLayers TARGS::load(std::string const& filename)
{
    IOLayers TARGS * p = new IOLayers TARGS(filename);
    p->load_from_file();
    p->block_manager = BlockManager::load(filename);
    if(p->block_manager->get_block_size() != p->parameters.block_size) 
    {
        std::cerr << "Warning: block_size mismatch between IOLayers and BlockManager! " 
            << p->block_manager->get_block_size()
            << " " << p->parameters.block_size
            << std::endl;
    }

    return std::unique_ptr<IOLayers TARGS>(p);
}

TDECL
void
IOLayers TARGS::save_to_file(void) 
{
    iolayers_file.seekp(0);
    dump_value(iolayers_file, parameters);
    dump_array(iolayers_file, top_layer);
}

TDECL
void
IOLayers TARGS::load_from_file(void)
{
    iolayers_file.seekg(0);
    load_value(iolayers_file, parameters);
    load_array(iolayers_file, top_layer);
    std::cerr << "top_layer size: " << top_layer.size() << std::endl;
    std::cerr << "block size: " << parameters.block_size << std::endl;
}

TDECL
template<typename Iterator>
std::vector<typename IOLayers TARGS::entry_t>
IOLayers TARGS::build_leaves(Iterator first, Iterator last, size_t min_leaf_size, size_t max_leaf_size)
{
    size_t element_left = std::distance(first, last);

    std::vector<entry_t> cur_layer;
    cur_layer.reserve(calc_node_count(element_left, min_leaf_size, max_leaf_size));

    auto iter = first;

    while(element_left > 0)
    {
        // build the next leaf node
        leaf_node_type cur_leaf_node;

        // determine how many values in the leaf
        size_t size = next_fanout(element_left, min_leaf_size, max_leaf_size);
        element_left -= size;

        auto min_key = iter->second;

        cur_leaf_node.values.reserve(size);
        for(size_t i = 0; i < size; ++i)
        {
            cur_leaf_node.values.push_back(*(iter->first));
            ++iter;
        }

        entry_t cur_entry;
        cur_leaf_node.build_entry(cur_entry);
        cur_entry.min_key = min_key;

        // write to block
        cur_leaf_node.allocate_blocks(cur_entry, *block_manager);
        cur_leaf_node.save_to_blocks(cur_entry, *block_manager);

        cur_layer.push_back(cur_entry);
    }

    return cur_layer;
}

TDECL
std::vector<typename IOLayers TARGS::entry_t>
IOLayers TARGS::build_leaves(stxxl::vector<builder_type> &input_vec, size_t element_count, size_t min_leaf_size, size_t max_leaf_size)
{
    size_t element_left = element_count;

    std::vector<entry_t> cur_layer;
    cur_layer.reserve(calc_node_count(element_left, min_leaf_size, max_leaf_size));

    stxxl::stream::vector_iterator2stream<typename stxxl::vector<builder_type>::const_iterator>
        input(input_vec.begin(), input_vec.end());

    builder_type cur_item = *input;
    ++input;

    while (element_left > 0)
    {
        // build the next leaf node
        leaf_node_type cur_leaf_node;

        // determine how many values in the leaf
        size_t size = next_fanout(element_left, min_leaf_size, max_leaf_size);
        element_left -= size;

        // we want to assign a minimum min_key for the first leaf
        auto min_key = cur_item.second;

        cur_leaf_node.values.reserve(size);
        cur_leaf_node.values.push_back(cur_item.first);
        for (size_t i = 0; i < size-1; ++i)
        {
            cur_item = *input;
            ++input;
            
            cur_leaf_node.values.push_back(cur_item.first);
        }

        entry_t cur_entry;
        cur_leaf_node.build_entry(cur_entry);
        cur_entry.min_key = min_key;

        // write to block
        cur_leaf_node.allocate_blocks(cur_entry, *block_manager);
        cur_leaf_node.save_to_blocks(cur_entry, *block_manager);

        cur_layer.push_back(cur_entry);
    }

    return cur_layer;
}

TDECL
template<typename Iterator>
std::vector<typename IOLayers TARGS::entry_t>
IOLayers TARGS::build_internal(Iterator first, Iterator last, size_t min_fanout, size_t max_fanout)
{
    size_t element_left = std::distance(first, last);

    std::vector<entry_t> next_layer;
    next_layer.reserve(calc_node_count(element_left, min_fanout, max_fanout));

    auto iter = first;
    while(element_left > 0)
    {
        internal_node_type cur_internal_node;

        // determine the number of children for current node
        size_t children_count = next_fanout(element_left, min_fanout, max_fanout);
        element_left -= children_count;

        // go through the children
        for(size_t i = 0; i < children_count; ++i)
        {
            cur_internal_node.children.push_back(*iter);
            ++ iter;
        }

        entry_t cur_entry;
        cur_internal_node.build_entry(cur_entry);
        // write to block
        {
            cur_internal_node.allocate_blocks(cur_entry, *block_manager);
            // we don't have samples nor buffer yet
            // but save the count
            cur_internal_node.save_to_blocks(cur_entry, *block_manager);
        }
        next_layer.push_back(cur_entry);
    }

    return next_layer;
}

} // namespace rtree

#undef TDECL
#undef TARGS
