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
// Implementation of the data source information object with stores
// general information about the data source (such as size or tree location)

#include "Data_Source_Information.h"

#include <memory>
#include <string>
#include <iostream>
#include <limits>
#include <chrono>
#include <random>
#include <algorithm>
#include <fstream>

//#include <tpie/tpie.h>
//#include <tpie/memory.h>
//#include <tpie/file_stream.h>

#include <boost/timer/timer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace bt = boost::timer;

using entry        = mongo_types::entry;
using sample_entry = mongo_types::sample_entry;
using point        = mongo_types::point3d;
using box          = mongo_types::box3d;

std::shared_ptr<Data_Source_Information> Data_Source_Information::p_Geolife_source = nullptr;
std::shared_ptr<Data_Source_Information> Data_Source_Information::p_OSM_nodes_source = nullptr;

uninitalized_exception my_uninitalized_exception;
already_initalized_exception my_already_initalized_exception;

Data_Source_Information::Data_Source_Information()
    :
    x_set{ false },
    y_set{ false },
    t_set{ false },
    m_element_count_set{ false },
    m_source_raw_set{ false },
    m_source_created_set{ false },
    m_source_level_created_set{ false },
    m_element_node_size_set{ false },
    m_element_sample_size_set{ false },
    m_converter_set{ false },
    m_readTime_set{ false },
    m_isPartInit{ false },
    m_isFullInit{ false }
{};

void Data_Source_Information::part_initGeolife()
{
    if (!p_Geolife_source)
        p_Geolife_source.reset(new Data_Source_Information());

    if (p_Geolife_source->m_isPartInit)
        return;

    p_Geolife_source->m_thisSource = Geolife;

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
    p_Geolife_source->set_convert_function(GeoLife_converter);

    p_Geolife_source->set_source_raw("./raw/GeoLifeDataNormalized.txt");
    p_Geolife_source->set_source_created("./built/geo_tree");
    p_Geolife_source->set_source_level_created("./built/geo_level");
    p_Geolife_source->set_source_shuffle_created("./built/geo_shuffle.shf");

    p_Geolife_source->set_element_size(sizeof(entry), sizeof(sample_entry));

    // a value of 4 was suggested by Lu
    p_Geolife_source->set_min_level_sample(4);

    p_Geolife_source->m_isPartInit = true;
}

void Data_Source_Information::full_initGeolife()
{
    part_initGeolife();
    if (p_Geolife_source->m_isFullInit)
        return; // already fully initialized


    // take 6000 samples
    double sample_rate = 10000.0 / 25000000.0;
    p_Geolife_source->m_sample_rate = sample_rate;
    std::default_random_engine generator;
    std::uniform_real_distribution<double> random_selection(0.0, 1.0);

    // find the extremes of each of the dimensions
    float x_min = std::numeric_limits<float>::max();
    float x_max = std::numeric_limits<float>::min();
    float y_min = std::numeric_limits<float>::max();
    float y_max = std::numeric_limits<float>::min();
    int   t_min = std::numeric_limits<int>::max();
    int   t_max = std::numeric_limits<int>::min();
    long int element_count = 0;

    // open input file
    std::fstream in_file(p_Geolife_source->get_source_raw().c_str(), std::ios_base::in);

    // start timer to figure out how long it takes to read and convert the data
    bt::cpu_timer scan_timer;
    auto GeoLife_converter = p_Geolife_source->get_convert_function();

    std::string str;
    while (!in_file.eof())
    {
        std::getline(in_file, str);

        // empty string are considered null, so don't enter any data from a zero length string
        if (str.size() == 0)
            continue;

        entry current_item = std::move(GeoLife_converter(str));

        if (random_selection(generator) < sample_rate)
            p_Geolife_source->m_sampled_points.push_back(current_item.get_point());

        x_min = std::min(current_item.loc.get<0>(), x_min);
        x_max = std::max(current_item.loc.get<0>(), x_max);

        y_min = std::min(current_item.loc.get<1>(), y_min);
        y_max = std::max(current_item.loc.get<1>(), y_max);

        t_min = std::min(current_item.timestamp, t_min);
        t_max = std::max(current_item.timestamp, t_max);

        ++element_count;
    }

    scan_timer.stop();

    p_Geolife_source->set_x(x_min, x_max);
    p_Geolife_source->set_y(y_min, y_max);
    p_Geolife_source->set_t(t_min, t_max);
    p_Geolife_source->set_element_count(element_count);
    // the following cast will truncate, so the number of seconds will be integer.  Since we expect
    // the time to read the file to be large (on the order of minutes to hours) partial seconds might be
    // unnecessary
    p_Geolife_source->set_readTime(scan_timer.elapsed().wall / (1000.0 * 1000 * 1000));

    // we shuffle the elements we selected so the selection seems a little more random
    shuffle(p_Geolife_source->m_sampled_points.begin(), p_Geolife_source->m_sampled_points.end(), generator);

    p_Geolife_source->m_isFullInit = true;
};

void Data_Source_Information::part_initOSM_nodes()
{
    if (!p_OSM_nodes_source)
        p_OSM_nodes_source.reset(new Data_Source_Information());

    if (p_OSM_nodes_source->m_isPartInit)
        return;

    p_OSM_nodes_source->m_thisSource = OSM_nodes;

    std::function<entry(const std::string&)> OSM_data1_converter =
        [](const std::string& str)
    {
        float x_val;
        float y_val;
        char OIDvalue[32];
        int time;

        int ret = sscanf(str.c_str(), "%24s\t%f\t%f\t%d\t%*d", OIDvalue, &x_val, &y_val, &time);

        return mongo_types::entry(x_val, y_val, time, OIDvalue);
    };

    p_OSM_nodes_source->set_convert_function(OSM_data1_converter);

    //p_OSM_nodes_source->set_source_raw("./raw/osm_test.osm");
    p_OSM_nodes_source->set_source_raw("./raw/osm_data1_lat-lon-uid-date-id.osm");
    p_OSM_nodes_source->set_source_created("./built/osm_tree");
    p_OSM_nodes_source->set_source_level_created("./built/osm_level");
    p_OSM_nodes_source->set_source_shuffle_created("./built/osm_shuffle.shf");

    p_OSM_nodes_source->set_element_size(sizeof(entry), sizeof(sample_entry));

    // a value of 12 was suggested by Lu
    p_OSM_nodes_source->set_min_level_sample(12);

    p_OSM_nodes_source->m_isPartInit = true;
}

void Data_Source_Information::full_initOSM_nodes()
{
    part_initOSM_nodes();
    if (p_OSM_nodes_source->m_isFullInit)
        return; // already initialized

    p_OSM_nodes_source->m_thisSource = OSM_nodes;

    // take about 6000 samples
    double sample_rate = 20000.0 / 2210525000.0;
    p_OSM_nodes_source->m_sample_rate = sample_rate;
    std::default_random_engine generator;
    std::uniform_real_distribution<double> random_selection(0.0, 1.0);



    // find the extremes of each of the dimensions
    float x_min = std::numeric_limits<float>::max();
    float x_max = std::numeric_limits<float>::min();
    float y_min = std::numeric_limits<float>::max();
    float y_max = std::numeric_limits<float>::min();
    int   t_min = std::numeric_limits<int>::max();
    int   t_max = std::numeric_limits<int>::min();
    long int element_count = 0;

    // open input file
    std::fstream in_file(p_OSM_nodes_source->get_source_raw().c_str(), std::ios_base::in);

    // start timer to figure out how long it takes to read and convert the data
    bt::cpu_timer scan_timer;

    auto OSM_data1_converter = p_OSM_nodes_source->get_convert_function();

    std::string str;
    while (!in_file.eof())
    {
        std::getline(in_file, str);

        // empty string are considered null, so don't enter any data from a zero length string
        if (str.size() == 0)
            continue;

        entry current_item = std::move(OSM_data1_converter(str));

        if (random_selection(generator) < sample_rate)
            p_OSM_nodes_source->m_sampled_points.push_back(current_item.get_point());

        x_min = std::min(current_item.loc.get<0>(), x_min);
        x_max = std::max(current_item.loc.get<0>(), x_max);

        y_min = std::min(current_item.loc.get<1>(), y_min);
        y_max = std::max(current_item.loc.get<1>(), y_max);

        t_min = std::min(current_item.timestamp, t_min);
        t_max = std::max(current_item.timestamp, t_max);

        ++element_count;

        if (element_count % 1000000 == 0)
            std::cout << '\r' << element_count << std::flush;
    }

    scan_timer.stop();

    p_OSM_nodes_source->set_x(x_min, x_max);
    p_OSM_nodes_source->set_y(y_min, y_max);
    p_OSM_nodes_source->set_t(t_min, t_max);
    p_OSM_nodes_source->set_element_count(element_count);
    // the following cast will truncate, so the number of seconds will be integer.  Since we expect
    // the time to read the file to be large (on the order of minutes to hours) partial seconds might be
    // unnecessary
    p_OSM_nodes_source->set_readTime(scan_timer.elapsed().wall / (1000.0 * 1000 * 1000));

    // we shuffle the elements we selected so the selection seems a little more random
    shuffle(p_OSM_nodes_source->m_sampled_points.begin(), p_OSM_nodes_source->m_sampled_points.end(), generator);

    p_OSM_nodes_source->m_isFullInit = true;
};

void Data_Source_Information::set_x(float min, float max)
{
    if (!x_set)
    {
        m_x_min = min;
        m_x_max = max;
        x_set = true;
    }
    else
        throw already_initalized_exception("min and max for x");
};

void Data_Source_Information::set_y(float min, float max)
{
    if (!y_set)
    {
        m_y_min = min;
        m_y_max = max;
        y_set = true;
    }
    else
        throw already_initalized_exception("min and max for y");
};

void Data_Source_Information::set_t(int min, int max)
{
    if (!t_set)
    {
        m_t_min = min;
        m_t_max = max;
        t_set = true;
    }
    else
        throw already_initalized_exception("min and max for t");
};

void Data_Source_Information::set_element_count(long int count)
{
    if (!m_element_count_set)
    {
        m_element_count = count;
        m_element_count_set = true;
    }
    else
        throw already_initalized_exception("element count");
};

void Data_Source_Information::set_source_raw(std::string str)
{
    if (!m_source_raw_set)
    {
        m_source_raw = str;
        m_source_raw_set = true;
    }
    else
        throw already_initalized_exception("raw source location");
};

void Data_Source_Information::set_source_created(std::string str)
{
    if (!m_source_created_set)
    {
        m_source_created = str;
        m_source_created_set = true;
    }
    else
        throw already_initalized_exception("created source location");
};

void Data_Source_Information::set_source_level_created(std::string str)
{
    if (!m_source_level_created_set)
    {
        m_source_level_created = str;
        m_source_level_created_set = true;
    }
    else
        throw already_initalized_exception("created level-sampling source location");
};

void Data_Source_Information::set_source_shuffle_created(std::string str)
{
    m_source_shuffle_created = str;
}

void Data_Source_Information::set_element_size(size_t node_size, size_t sample_size)
{
    if (!m_element_node_size_set)
    {
        m_element_node_size = node_size;
        m_element_node_size_set = true;
    }
    else
        throw already_initalized_exception("element node size");

    if (!m_element_sample_size_set)
    {
        m_element_sample_size = sample_size;
        m_element_sample_size_set = true;
    }
    else
        throw already_initalized_exception("element sample size");
}

void Data_Source_Information::set_convert_function(std::function<entry(const std::string&)> fun)
{
    if (!m_converter_set)
    {
        m_converter = fun;
        m_converter_set = true;
    }
    else
        throw already_initalized_exception("convert function");
}

void Data_Source_Information::set_readTime(float time_sec)
{
    if (!m_readTime_set)
    {
        m_readTime = time_sec;
        m_readTime_set = true;
    }
    else
        throw already_initalized_exception("read time");
};

void Data_Source_Information::set_min_level_sample(int value)
{
    m_min_level_sample = value;
}

float Data_Source_Information::get_x_min()
{
    full_init();

    if (x_set)
        return m_x_min;
    else
        throw uninitalized_exception("min for x");
};

float Data_Source_Information::get_x_max()
{
    full_init();

    if (x_set)
        return m_x_max;
    else
        throw uninitalized_exception("max for x");
};

float Data_Source_Information::get_y_min()
{
    full_init();

    if (y_set)
        return m_y_min;
    else
        throw uninitalized_exception("min for y");
};

float Data_Source_Information::get_y_max()
{
    full_init();

    if (y_set)
        return m_y_max;
    else
        throw uninitalized_exception("max for y");
};

int Data_Source_Information::get_t_min()
{
    full_init();

    if (t_set)
        return m_t_min;
    else
        throw uninitalized_exception("min for t");
};

int Data_Source_Information::get_t_max()
{
    full_init();

    if (t_set)
        return m_t_max;
    else
        throw uninitalized_exception("max for t");
};

long int Data_Source_Information::get_element_count()
{
    full_init();

    if (m_element_count_set)
        return m_element_count;
    else
        throw uninitalized_exception("element count");
}

std::string Data_Source_Information::get_source_raw() const
{
    if (m_source_raw_set)
        return m_source_raw;
    else
        throw uninitalized_exception("raw source");
}

std::string Data_Source_Information::get_source_created() const
{
    if (m_source_created_set)
        return m_source_created;
    else
        throw uninitalized_exception("created source");
}

std::string Data_Source_Information::get_source_level_created() const
{
    if (m_source_level_created_set)
        return m_source_level_created;
    else
        throw uninitalized_exception("created source");
}

std::string Data_Source_Information::get_source_shuffle_created() const
{
    return m_source_shuffle_created;
}

size_t Data_Source_Information::get_element_node_size() const
{
    if (m_element_node_size_set)
        return m_element_node_size;
    else
        throw uninitalized_exception("element node size");
}

size_t Data_Source_Information::get_element_sample_size() const
{
    if (m_element_sample_size_set)
        return m_element_sample_size;
    else
        throw uninitalized_exception("element sample size");
}

std::function<entry(const std::string&)> Data_Source_Information::get_convert_function() const
{
    if (m_converter_set)
        return m_converter;
    else
        throw uninitalized_exception("convert function");
}

float Data_Source_Information::get_readTime()
{
    full_init();

    if (m_readTime_set)
        return m_readTime;
    else
        throw uninitalized_exception("read time");
};

int Data_Source_Information::get_min_level_sample()
{
    return m_min_level_sample;
}

Data_sources Data_Source_Information::get_dataSource()
{
    return m_thisSource;
}

mongo_types::point3d Data_Source_Information::get_randomPoint()
{
    full_init();

    mongo_types::point3d toreturn = m_sampled_points.front();
    m_sampled_points.pop_front();
    return toreturn;
}

size_t Data_Source_Information::get_remainingrandomPoints()
{
    full_init();
    return m_sampled_points.size();
}