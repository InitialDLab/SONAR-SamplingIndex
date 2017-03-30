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
#include <iostream>
#include <vector>
#include <fstream>
#include <random>
#include <limits>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <functional>

#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>

#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "mongo_types.h"
#include "rtree/rtree.h"
#include "level_sampling/level_sampling.h"

//using entry = mongo_types::entry;
//using sample_entry = mongo_types::sample_entry;
//using point = mongo_types::point3d;
//using box = mongo_types::box3d;
//using rtree_t = rtree::rtree<
//    entry,
//    sample_entry,
//    box,
//    256, // node sample count
//    256,  // leaf size
//    16   // internal size
//>;

template<typename Iter>
void generate_entries(size_t element_count, Iter iter)
{
    std::default_random_engine rng;

    std::uniform_int_distribution<unsigned int> lat_dist(0, 180L * 10000000);
    std::uniform_int_distribution<unsigned int> lon_dist(0, 360L * 10000000);
    std::uniform_int_distribution<int> timestamp_dist(0, std::numeric_limits<int>::max());

    static const char * id_list = "0123456789abcdef";
    std::uniform_int_distribution<int> id_dist(0, 15);
    char id_buffer[2 * mongo_types::OID::oid_len + 1];
    id_buffer[2*mongo_types::OID::oid_len] = 0;

    for(size_t i = 0; i < element_count; ++i)
    {
        for(int j = 0; j < 2 * mongo_types::OID::oid_len; ++j)
        {
            id_buffer[j] = id_list[id_dist(rng)];
        }

        *iter++ = mongo_types::entry(
            ((double)(lat_dist(rng))) / 10000000.0 - 90,
            ((double)(lon_dist(rng))) / 10000000.0 - 180,
            timestamp_dist(rng),
            id_buffer
        );
    }
}

void generate(size_t element_count, std::string const& filename)
{
    std::vector<mongo_types::entry> entries;
    entries.reserve(element_count);
    
    generate_entries(element_count, std::back_inserter(entries));

    {
        std::cerr << "bbox: ";
        mongo_types::box3d bbox;
        boost::geometry::assign_inverse(bbox);
        for(auto & v : entries)
            boost::geometry::expand(bbox, v.get_point());
        dump_box(std::cerr, bbox);
        std::cerr << std::endl;
    }

    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    rtree::IOLayersParameters parameters;

    parameters.max_top_layer_io_node_count = 1024;
    constexpr size_t NODE_SAMPLE_SIZE = 256;
    constexpr size_t MAX_FANOUT = 16;
    rtree::rtree<
        entry,
        sample_entry,
        box,
        NODE_SAMPLE_SIZE,
        MAX_FANOUT
    >::build_io_layers(entries.begin(), entries.end(), filename + "-rtree");

    parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
    parameters.max_top_layer_io_node_count /= MAX_FANOUT;

    level_sampling::level_sampling<
        entry,
        sample_entry,
        box,
        MAX_FANOUT
    >::build(entries.begin(), entries.end(), filename + "-ls", parameters);
}

void test()
{
    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    rtree::IOLayersParameters parameters;

    constexpr size_t NODE_SAMPLE_SIZE = 256;
    constexpr size_t MAX_FANOUT = 16;

    parameters.max_top_layer_io_node_count *= NODE_SAMPLE_SIZE;
    parameters.max_top_layer_io_node_count /= MAX_FANOUT;

    std::function<entry(const std::string&)> GeoLife_converter =
        [](const std::string& str)
    {
        float x_val;
        float y_val;
        int   time;
        char OIDvalue[32];

        int ret = sscanf(str.c_str(), "%24s %f %f %d", OIDvalue, &x_val, &y_val, &time);

        return mongo_types::entry(x_val, y_val, time, OIDvalue);
    };

    level_sampling::level_sampling<
        entry,
        sample_entry,
        box,
        MAX_FANOUT
    >::build("raw/GeoLifeDataNormalized.txt",
            "test/1",
            GeoLife_converter,
            1024*1024*1024,
            parameters);
}

int main(int argc, char ** argv)
{
    //// THIS IS A TEMP MAIN FUNCTION FOR TESTING PURPOSES FOR BULK LOADING WITHOUT NEED FOR THE ENTIRE 
    //// DATA SET TO FIT INTO MAIN MEMORY.
    //std::string inputfilename  = "/uusoc/scratch/magpie2/robertc/data/GeoLifeDataAdded.txt";
    //std::string outputfilename = "output";

    //using entry = mongo_types::entry;
    //using sample_entry = mongo_types::sample_entry;
    //using point = mongo_types::point3d;
    //using box = mongo_types::box3d;

    //boost::posix_time::ptime epoch = boost::posix_time::from_iso_string("1970-01-01T00:00:00Z");

    ////std::function<entry(const std::string&)> convert = 
    ////    [](const std::string& str){
    ////    float x_val = 0.0f;
    ////    float y_val = 0.0f;
    ////    int   time  = 0;
    ////    char OIDvalue[32];
    ////
    ////    int ret = sscanf(str.c_str(), "%24s %f %f %f", OIDvalue, &x_val, &y_val, &time);
    ////
    ////    return mongo_types::entry(x_val, y_val, time, OIDvalue); 
    ////};
    //std::function<entry(const std::string&)> convert =
    //    [&epoch](const std::string& str){
    //    float x_val = 0.0f;
    //    float y_val = 0.0f;
    //    int   user  = 0;
    //    char OIDvalue[32];
    //    char timestr[32];

    //    int ret = sscanf(str.c_str(), "%f %f %d %21s %24s", &x_val, &y_val, &time, &user, timestr, OIDvalue);

    //    boost::posix_time::ptime entry_time = boost::posix_time::from_iso_string(timestr);
    //    int time = boost::posix_time::time_period(epoch, entry_time).length().total_seconds();

    //    return mongo_types::entry(x_val, y_val, time, OIDvalue);
    //};
    //rtree::rtree<
    //    entry,
    //    sample_entry,
    //    box,
    //    256, // node sample count
    //    256,  // leaf size
    //    16   // internal size
    //>::build_io_layers(inputfilename, outputfilename,
    //                   convert,  1024 * 1024 * 1024);

    test();
    return 0;
    
    if(argc != 3)
    {
        std::cerr << "argument: <entry count> <output filename>" << std::endl;
        return -1;
    }

    size_t element_count = boost::lexical_cast<size_t>(argv[1]);
    const char * filename = argv[2];

    generate(element_count, filename);
    
    return 0;
}
