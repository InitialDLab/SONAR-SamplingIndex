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
// implementation of RStree_basic

#include <iostream>

#include <functional>
#include <cstdio>
#include <ctime>

#include <sys/stat.h>

#include <glog/logging.h>

#include "RStree_basic.h"

#include "basic_types.h"
#include "query_cursor_basic.h"

RStree_basic::RStree_basic(std::string input_file, std::string file_prefix, bool cleanup_input)
{
    // create the function we will be using to convert the input to data
    std::function<server_types::basic_entry(const std::string&)> basic_data_converter =
        [](const std::string& str)
    {
        char OIDvalue[26];
        double x_val;
        double y_val;
        int time;

        int ret = sscanf(str.c_str(), "%24s,%lf,%lf,%d", OIDvalue, &x_val, &y_val, &time);

        // x_val should be latitude (-90 -- 90)
        // y_val should be longitude (-180 -- 180)
        if (x_val < -90.0f || x_val > 90.0f || y_val < -180.0f || y_val > 180.0f) {
            LOG(INFO) << "Bad record found in inserted data set record = \'" << str << "\' Replaced with null record";

            return server_types::basic_entry::build(0.0f, 0.0f, 0, "000000000000000000000000");
        }

        return server_types::basic_entry::build((float)x_val, (float)y_val, time, OIDvalue);
    };

    // run the build function to build the RS tree

    // the name of the file will be formatted for the time it was created
    // we will mark the time is was constructed as when we started building
    time(&m_constructionTime);
    tm* current_time = gmtime(&m_constructionTime);

    char file_name_builder[64];
    snprintf(file_name_builder, 64, "%s_%d_%d_%d", file_prefix.c_str(), current_time->tm_year % 100, current_time->tm_yday, current_time->tm_hour);

    m_file_backend = file_name_builder;

    // build the tree
    LOG(INFO) << "starting to build an RStree_basic";
    basic_rtree::build_io_layers(input_file, m_file_backend, basic_data_converter);
    LOG(INFO) << "finished building tree" << std::endl;

    LOG(INFO) << "Opening RStree_basic from file";
    mp_data.reset(new basic_rtree(m_file_backend));
    LOG(INFO) << "Finished opening RStree_basic from file";

    mp_data->save_mem_nodes();

    m_size = mp_data->size();

    // cleanup if needed
    if (cleanup_input)
    {
        if (remove(input_file.c_str()) != 0)
            LOG(WARNING) << "Error deleting the file " << input_file;
    }

    LOG(INFO) << "Finished creating a basic RStree";
}

RStree_basic::RStree_basic(std::string input_file, time_t construction_time)
    : m_constructionTime(construction_time)
    , m_file_backend(input_file)
{
    LOG(INFO) << "Opening RStree_basic from file";
    std::string memnodes_file = input_file + ".memnodes";
    if (fexists(memnodes_file.c_str())) {
        mp_data.reset(new basic_rtree(input_file, false, true));
    }
    else {
        LOG(INFO) << "Memnodes file not found.  Rebuilding memnodes";
        mp_data.reset(new basic_rtree(input_file, false, false));
        mp_data->save_mem_nodes();
    }
    LOG(INFO) << "Finished opening RStree_basic from file";
}


std::shared_ptr<RStree_basic> RStree_basic::open_RStree_basic(std::string input_file, time_t construction_time)
{
    return std::shared_ptr<RStree_basic>(new RStree_basic(input_file, construction_time));
}


std::shared_ptr<RStree_basic> RStree_basic::build_RStree_basic(std::string input_file, std::string file_prefix, bool cleanup_input)
{
    // if the file does not exist, return an empty shared pointer
    struct stat buf;
    if (stat(input_file.c_str(), &buf) == -1)
        return std::shared_ptr<RStree_basic>(nullptr);

    return std::shared_ptr<RStree_basic>(new RStree_basic(input_file, file_prefix, cleanup_input));
}

RStree_basic::~RStree_basic()
{
    LOG(INFO) << "Cleaning up a basic RStree";
    mp_data.reset();
    if (this->m_cleanupAfterUse)
    {
        LOG(INFO) << "cleaning up the backend files for " << m_file_backend;
        std::string fdata = m_file_backend + ".data";
        std::string fio = m_file_backend + ".iolayers";
        std::string mdata = m_file_backend + ".metadata";
        std::string cache = m_file_backend + ".memnodes";

        if (remove(fdata.c_str()) != 0)
            LOG(WARNING) << "Error deleting the file " << fdata;
        if (remove(fio.c_str()) != 0)
            LOG(WARNING) << "Error deleting the file " << fio;
        if (remove(mdata.c_str()) != 0)
            LOG(WARNING) << "Error deleting the file " << mdata;
        if (remove(mdata.c_str()) != 0)
            LOG(WARNING) << "Error deleting the file " << cache;
    }
}

std::unique_ptr<query_cursor> RStree_basic::get_query_cursor(const serverProto::StartQueryRequest& request)
{
    
    std::unique_ptr<query_cursor_basic> to_ret(new query_cursor_basic(mp_data, request.query_region(),
                                                                request.return_oid(),
                                                                request.return_time(),
                                                                request.return_location(),
                                                                request.suggested_ttl()));

    return move(to_ret);
}

serverProto::SampleStructureType RStree_basic::getPayloadType()
{
    return serverProto::SampleStructureType::NO_PAYLOAD;
}

void RStree_basic::insert(const serverProto::element& elem)
{
    throw std::bad_function_call();

    //// check to see if we have the required data elements in the element
    //if (!elem.has_location() || elem.oid().size() == 0)
    //    return;

    //if (elem.location().lat() < -90.0f || elem.location().lat() > 90.0f || elem.location().lon() < -180.0f || elem.location().lon() > 180.0f)
    //    LOG(WARNING) << "Element being inserted outside regular bounds for location (lat,lon)=" << elem.location().lat() << ',' << elem.location().lon() << ')';

    //// insert value into the tree
    //server_types::basic_entry newValue(elem.location().lat(), elem.location().lon(), elem.location().time(), elem.oid());
    //mp_data->insert(newValue);

    //// increase m_size
    //++m_size;
}

long RStree_basic::get_size()
{
    return m_size;
}

time_t RStree_basic::get_construction_time()
{
    return m_constructionTime;
}

std::shared_ptr<basic_rtree> RStree_basic::get_rtree()
{
    return mp_data;
}

std::string RStree_basic::get_BackingFile()
{
    return m_file_backend;
}

bool RStree_basic::fexists(const char *filename)
{
    std::ifstream ifile(filename);
    return ifile;
}

bool RStree_basic::flush_buffers()
{
    mp_data->flush_cache();
}