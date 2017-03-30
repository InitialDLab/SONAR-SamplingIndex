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
// A small collection of useful information for the different data sources and
// information about the data source

#ifndef DATA_SOURCE_INFORMATION_H__
#define DATA_SOURCE_INFORMATION_H__

#include <string>
#include <exception>
#include <memory>
#include <functional>

#include "../mongo_types.h"

enum Data_sources {Data_source_start = 1, Geolife = 1, OSM_nodes, Data_source_end};

// exception thrown when an item has not been set but it tries to read it
// (this is an error because reading the value would return an undefined value
class uninitalized_exception : public std::exception
{
public:
    uninitalized_exception()
        : m_message("")
    { };
    uninitalized_exception(std::string message)
        : m_message(message)
    { };

    virtual const char* what() const throw()
    {
        if (m_message.empty())
            return "A value which has not been set is trying to be read";
        else
            return m_message.c_str();
    }

private:
    std::string m_message;
};

// exception thrown when an item has already been set but it tried to write
// another value to that value
class already_initalized_exception : public std::exception
{
public:
    already_initalized_exception()
        : m_message("")
    { };
    already_initalized_exception(std::string message)
        : m_message(message)
    { };

    virtual const char* what() const throw()
    {
        if (m_message.empty())
            return "An attempt was made to write a value which is already initialized";
        else
            return m_message.c_str();
    }

private:
    std::string m_message;
};



//
// This data structure provides data storage but only allows
// each element to be set once 
//
class Data_Source_Information
{
public:
    using entry = mongo_types::entry;
    using sample_entry = mongo_types::sample_entry;
    using point = mongo_types::point3d;
    using box = mongo_types::box3d;

    static const std::shared_ptr<Data_Source_Information> get_data_information(Data_sources source_request)
    {
        switch (source_request)
        {
        case Geolife:
            if (p_Geolife_source == nullptr)
                part_initGeolife();
            return p_Geolife_source;
            break;
        case OSM_nodes:
            if (p_OSM_nodes_source == nullptr)
                part_initOSM_nodes();
            return p_OSM_nodes_source;
            break;
        }
        return nullptr;
    };



    float get_x_min();
    float get_x_max();
    float get_y_min();
    float get_y_max();
    int get_t_min();
    int get_t_max();
    long int get_element_count();
    std::string get_source_raw() const;
    std::string get_source_created() const;
    std::string get_source_level_created() const; // the name of the level sampling file
    std::string get_source_shuffle_created() const;
    size_t get_element_node_size() const;
    size_t get_element_sample_size() const;
    std::function<entry(const std::string&)> get_convert_function() const;
    float get_readTime();
    Data_sources get_dataSource();

    int get_min_level_sample();

    mongo_types::point3d get_randomPoint();
    size_t get_remainingrandomPoints();

private:
    void set_x(float min, float max);
    void set_y(float min, float max);
    void set_t(int min, int max);
    void set_element_count(long int count);
    void set_source_raw(std::string str);
    void set_source_created(std::string str);
    void set_source_level_created(std::string str);
    void set_source_shuffle_created(std::string str);
    void set_element_size(size_t node_size, size_t sample_size);
    void set_convert_function(std::function<entry(const std::string&)> fun);
    void set_readTime(float time_sec);
    void set_min_level_sample(int value);

    void full_init()
    {
        switch (m_thisSource)
        {
        case Geolife:
            full_initGeolife();
            break;
        case OSM_nodes:
            full_initOSM_nodes();
            break;
        }
    }

    static std::shared_ptr<Data_Source_Information> p_Geolife_source;
    static std::shared_ptr<Data_Source_Information> p_OSM_nodes_source;

    std::deque<mongo_types::point3d> m_sampled_points;

    double m_sample_rate;
    int m_min_level_sample;

    Data_Source_Information();

    static void part_initGeolife();
    static void part_initOSM_nodes();

    static void full_initGeolife();
    static void full_initOSM_nodes();

    float m_x_min;
    float m_x_max;
    
    float m_y_min;
    float m_y_max;

    int m_t_min;
    int m_t_max;

    Data_sources m_thisSource;

    long int m_element_count;

    std::string m_source_raw;
    std::string m_source_created;
    std::string m_source_level_created;
    std::string m_source_shuffle_created;

    size_t m_element_node_size;
    size_t m_element_sample_size;

    std::function<entry(const std::string&)> m_converter;

    float m_readTime;

    bool x_set; //
    bool y_set; //
    bool t_set; //
    bool m_element_count_set; //
    bool m_source_raw_set; //
    bool m_source_created_set; //
    bool m_source_level_created_set;
    bool m_element_node_size_set; //
    bool m_element_sample_size_set; //
    bool m_converter_set; //
    bool m_readTime_set; //

    bool m_isPartInit; //
    bool m_isFullInit;
};

#endif //DATA_SOURCE_INFORMATION_H__