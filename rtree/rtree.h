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
 * Sample Rtree
 * by Lu Wang 2014
 */
#pragma once

#include <memory>
#include <vector>
#include <fstream>
#include <list>
#include <iterator>
#include <string>
#include <iterator>
#include <algorithm>
#include <random>
#include <ctime>

#include <boost/geometry/geometry.hpp>
#include <boost/mpl/integral_c.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/if.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/random/taus88.hpp>
#include <boost/random/uniform_smallint.hpp>

#include "hilbert/hilbert.h"
#include "serialization/serializer.h"

#include "util.h"

namespace rtree {

namespace bg = boost::geometry;
namespace bm = boost::mpl;

//using RNG = std::default_random_engine;
using RNG = boost::random::taus88;
using RNG_DEV = std::random_device;

struct Stats {
    size_t internal_nodes = 0;
    size_t leaf_nodes = 0;
    size_t io_internal_nodes = 0;
    size_t io_leaf_nodes = 0;
    size_t io_sample_nodes = 0;

    Stats& operator += (Stats const& stats) {
        internal_nodes += stats.internal_nodes;
        leaf_nodes += stats.leaf_nodes;
        io_internal_nodes += stats.io_internal_nodes;
        io_leaf_nodes += stats.io_leaf_nodes;
        io_sample_nodes += stats.io_sample_nodes;
        return *this;
    }

#ifdef RSTREE_PROFILING
    size_t values_reported = 0;
    size_t values_rejected = 0;
    size_t leaf_values_scanned = 0;
#endif //RSTREE_PROFILING
};


} // namespace rtree

/*
 * these are internal files for the rtree implementation
 * do not use them out of this file
 */
#include "nodes.h"
#include "block_manager.h"
#include "io_layers.h"

#include "sample_builder.h"
#include "naive_sample_query.h"
#include "sample_query.h"
#include "range_reporter.h"
#include "node_loader.h"
#include "mem_node_cleaner.h"
#include "mem_node_saver.h"
#include "inserter.h"
#include "eraser.h"
#include "finder.h"

namespace rtree {
    /*
     * The main rtree structure
     *
     * Value: the data type for raw entries, should has a `get_point` method, whose return value is to be fed into HilbertValueComputer
     * SampleValue: the data type for samples, should contain interested data fields and an OID for identification (in deletion)
     *
     * NodeSampleSize: how many samples are stored in each mem node
     * Max/Min Fanout: the fanout parameters for the in-memory part
     *                 MinFanout should not be larger than MaxFanout / 2
     *
     * HilbertValueComputer: to convert a point in Value into the Hilbert space
     *
     */
    template<
        typename Value,
        typename SampleValue = Value,
        typename Box = bg::model::box<bg::model::point<float, 3, bg::cs::cartesian>>,
        size_t NodeSampleSize = 512,
        size_t MaxFanout = 16,
        size_t MinFanout = MaxFanout / 4,
        typename HilbertValueComputer = hilbert::HilbertValueComputer < unsigned int, 3, 8 >
    >
    struct rtree
    {
        using hilbert_value_type = typename HilbertValueComputer::value_type;

        using box_type = Box;
        using key_type = hilbert_value_type;
        using value_type = Value;
        using sample_value_type = SampleValue;
        static constexpr size_t mem_node_sample_size = NodeSampleSize;

#define TARGS <Box, hilbert_value_type, Value, SampleValue>

        using node_type = node TARGS;
        using mem_internal_node_type = internal_node TARGS;
        using mem_leaf_node_type = leaf_node TARGS;
        using io_internal_node_type = io_internal_node TARGS;
        using io_leaf_node_type = io_leaf_node TARGS;

        using entry_t = typename node TARGS::entry_t;
        using visitor_type = typename node TARGS::visitor_type;

        using io_layers_type = IOLayers < Box, HilbertValueComputer, Value, SampleValue > ;

        rtree(std::string const& filename, 
              bool in_memory = false, // if in_memory is true, block_cache is set to unlimited
              bool load_mem_nodes = false,
              size_t memory_limit = 0, 
              std::shared_ptr<HilbertValueComputer> hvc = nullptr
        );
        ~rtree();
        void save_mem_nodes(void);

        template<bool UPDATE_SAMPLE=true>
        void insert(Value const& value);

        template<bool UPDATE_SAMPLE=true>
        bool erase(Value const& value);

        bool find(Value const& value);

        size_t
        size(void) const {
            return root_node_entry.subtree_size;
        }

        key_type
        min_key(void) const { 
            return root_node_entry.min_key;
        }

        void
        apply_visitor(visitor_type & v) {
            root_node_entry.apply_visitor(v);
        }

        Box
        bbox(void) const {
            return root_node_entry.bbox;
        }

        BlockManager &
        get_block_manager(void) {
            return io_layers->get_block_manager();
        }

        /*
         * Flush the disk cache in block_manager
         */
        void
        flush_cache(void) {
            get_block_manager().flush_cache();
        }

        /*
         * Get samples without using the samples associated to the nodes
         * The baseline algorithm
         */
        template<typename Geometry>
        naive_sample_query_cursor TARGS
        naive_sample_query(Geometry const& query) {
            return naive_sample_query_cursor TARGS
                (query, root_node_entry, get_block_manager(), rng_dev);
        }

        /*
         * Get samples
         * The essential algorithm supported by this rtree structure
         */
        template<typename Geometry>
        sample_query_cursor<Geometry, Box, hilbert_value_type, Value, SampleValue>
        sample_query(Geometry const& query) {
            return sample_query_cursor<Geometry, Box, hilbert_value_type, Value, SampleValue> (query, root_node_entry, get_block_manager(), rng_dev);
        }

        /*
         * Get all items within the query range
         */
        template<typename Geometry, typename Iterator>
        Stats
        range_report(Geometry const& query, Iterator out_iter) {
            range_reporter<Geometry, Iterator, Box, hilbert_value_type, Value, SampleValue>
                rr(query, get_block_manager(), out_iter);
            apply_visitor(rr);
            return rr.stats;
        }


        /*
         * Build the disk/io layers from the raw data
         */
        template<typename Iterator>
        static void
        build_io_layers(Iterator first, Iterator last, std::string const& filename, IOLayersParameters const& parameters = IOLayersParameters())
        {
            io_layers_type::create(filename, parameters)->build(first, last);
        }

        /*
         * Build the disk/io layers from a file on disk.  Provide a function which can be used
         * to convert a line of text to the data type of interest for the rtree
         *
         * (Use this to construct an rstree from raw data)
         */
        static void
        build_io_layers(std::string const& input_file,
            std::string const& rtreeStorageFilename,
            std::function<Value(const std::string&)> ReadConverter,
            size_t allowed_memory_use = 1024 * 1024 * 1024, // 1 GB of memory allowed by default
            IOLayerBuildStatistics *build_stats = nullptr,
            IOLayersParameters const& parameters = IOLayersParameters())
        {
            auto iolayer = io_layers_type::create(rtreeStorageFilename, parameters);
            iolayer->build(input_file, ReadConverter, allowed_memory_use);
            if (build_stats)
                *build_stats = iolayer->get_statistics();
        }

        /*
         * estimate the memory usage
         */
        static size_t
        estimate_memory_usage(size_t element_count, IOLayersParameters const& parameters = IOLayersParameters())
        {
            size_t top_layer_node_count = io_layers_type::get_top_layer_node_count(element_count, parameters);
            return (top_layer_node_count / (MaxFanout-1)) * (sizeof(SampleValue) * NodeSampleSize + sizeof(entry_t) * MaxFanout);
        }

        /*
         * estimate total space (mem+disk)
         */
        static size_t
        estimate_total_size(size_t element_count, IOLayersParameters const& parameters = IOLayersParameters())
        {
            size_t leaf_node_size = element_count * sizeof(Value) / parameters.fill_ratio;
            size_t internal_node_size = element_count / (MaxFanout-1) * (sizeof(SampleValue) * NodeSampleSize + sizeof(entry_t) * MaxFanout);
            return leaf_node_size + internal_node_size;
        }

        /*
         * Only updated upon build
         * Will not be updated upon insertion/deletion
         */
        Stats
        get_stats(void) const { return stats; }

        /*
        // count the number of nodes by walking 
        Stats
        count_nodes(void) {
            walker<Box, hilbert_value_type, Value, SampleValue> w(io_layers->get_block_manager());
            apply_visitor(w);
            return w.stats;
        }
        */
    private:

        /*
         * Build one single layer in the structure
         */
        template<typename Iterator>
        std::vector<entry_t>
        build_layer(Iterator first, Iterator last, size_t min_fanout, size_t max_fanout);

        entry_t root_node_entry;
        std::unique_ptr<io_layers_type> io_layers;
        std::shared_ptr<HilbertValueComputer> hilbert_value_computer;

        Stats stats;
        
        std::string filename;

        //RNG rng;
        RNG_DEV rng_dev;

        bool clean_mem_resident_nodes = false;
    };

#undef TARGS
} // namespace rtree

/*
 * these are internal files for the rtree implementation
 * do not use them out of this file
 */
#include "nodes_impl.h"
#include "io_layers_impl.h"
#include "rtree_impl.h"
