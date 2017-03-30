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

#include "stxxl/vector"

namespace rtree {

struct IOLayersParameters
{
    // usually it's not better to make every block full in the initial build
    // this value controls how much shall we fill in each block
    double fill_ratio = 0.7;
    // size of a block, in bytes
    size_t block_size = 8192;
    // the maximum number of nodes in the top layer
    // we will stop building more layers when there are few enough nodes
    size_t max_top_layer_io_node_count = 1024;
    // maximum number of nodes to cache in the memory (in the block manager)
    size_t cached_blocks = 4096;
};

struct IOLayerBuildStatistics
{
    // the time it took to read and prepare all the elements (for disk based building)
    double read_time_sec;
    // the time it took to sort the elements
    double sort_time_sec;
    // the time it took to construct the node elements
    double node_construct_time_sec;
    // the time it took to construct the leaf elements
    double leaf_construct_time_sec;
    // the time it took to put all the elements in the tree
    double construct_time_sec;
    // the sum of all the other times
    double total_time_sec;
};

/*
 * IOLayers means those nodes that are stored on the disk
 * These nodes are built once and then loaded from disk later
 * (But the in-memory part (mem nodes) are only rebuilt each time)
 */
template<typename Box, typename HilbertValueComputer, typename Value, typename SampleValue>
struct IOLayers
{
    using hilbert_value_type = typename HilbertValueComputer::value_type;
    using Key = hilbert_value_type;

    using node_type = node<Box, Key, Value, SampleValue>;
    using internal_node_type = io_internal_node<Box, Key, Value, SampleValue>;
    using leaf_node_type = io_leaf_node<Box, Key, Value, SampleValue>;
    using entry_t = typename node_type::entry_t;

    struct builder_type
    {
        Value first;

        hilbert_value_type second;

        // the dummy value is added to make sizeof(builder_type)==64.  Doing this makes it more
        // easy to use stxxl external vector files.  Otherwise the block size will have to change
        // in the vector template value.  Ultamatly it would probably be better to change
        // the value there, this is an easy fix for now.
        // TODO: remove dummy value and adjust vector template.
        Value dummy;
    };

    ~IOLayers();

    using converter_t = std::function<Value(const std::string&)>;

    template<typename Iterator>
    void 
    build(Iterator first, Iterator last);

    void
    build(const std::string &inputFile, converter_t ReadConverter, 
            size_t tpie_allowed_memory = (1024 * 1024));

    void 
    build_from_sorted(std::string const& in_filename, size_t element_count);

    static size_t
    get_top_layer_node_count(size_t element_count, IOLayersParameters const& parameters);

    // must be called when tpie has been initialized
    // return the number of elements
    size_t 
    convert_data_file(std::string const& in_filename, std::string const& out_filename,
    const converter_t&  converter);

    // sample each element with probability 0.5 and store them into a new file
    // returns the number of elements in the output
    // only used by level_sampilng
    size_t
    sample_data_file(std::string const& in_filename, std::string const& out_filename);

    static 
    std::unique_ptr<IOLayers>
    create(std::string const& filename, IOLayersParameters const& parameters);

    static 
    std::unique_ptr<IOLayers>
    load(std::string const& filename);

    std::vector<entry_t> const&
    get_top_layer(void) const { return top_layer; }

    BlockManager &
    get_block_manager(void) { return *block_manager; }

    const IOLayerBuildStatistics get_statistics() const
    {
        return last_build_statistics;
    }

private:
    IOLayers(std::string const& filename)
        : iolayers_file(filename + ".iolayers", std::fstream::in | std::fstream::out | std::fstream::binary)
    { }

    void save_to_file(void);
    void load_from_file(void);

    template<typename Iterator>
    std::vector<entry_t> 
    build_leaves(Iterator first, Iterator last, size_t min_leaf_size, size_t max_leaf_size);
 

    std::vector<entry_t>
        build_leaves(stxxl::vector<builder_type> &input, size_t element_count, size_t min_leaf_size, size_t max_leaf_size);

    template<typename Iterator>
    std::vector<entry_t>
    build_internal(Iterator first, Iterator last, size_t min_fanout, size_t max_fanout);

    IOLayersParameters parameters;
    std::vector<entry_t> top_layer;

    std::fstream iolayers_file;
    std::unique_ptr<BlockManager> block_manager;

    IOLayerBuildStatistics last_build_statistics;
};

} // namespace rtree
