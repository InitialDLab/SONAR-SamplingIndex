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

#include <memory>
#include <ctime>

#include "rtree/rtree.h"
#include "server_code/sampling_structure.h"
#include "server_code/basic_types.h"

#include "query_cursor.h"

typedef rtree::rtree<server_types::basic_entry, server_types::basic_entry, server_types::box3d> basic_rtree;

class RStree_basic : public sampling_structure
{
public:
    // input_file is the name of the file to read in to create the tree
    // file_prefix is a string to put in the string when it is saved to help to know what files do what
    // cleanup_input will specify if it should remove the input file after the data ins inserted.
    static std::shared_ptr<RStree_basic> build_RStree_basic(std::string input_file, std::string file_prefix, bool cleanup_input = false);

    // open an RStree which has already been constructed
    static std::shared_ptr<RStree_basic> open_RStree_basic(std::string input_file, time_t construction_time = 0);

    ~RStree_basic();

    std::unique_ptr<query_cursor> get_query_cursor(const serverProto::StartQueryRequest& request);

    serverProto::SampleStructureType getPayloadType();

    // insert the value into the RStree
    void insert(const serverProto::element&);

    bool flush_buffers();

    long get_size();

    time_t get_construction_time();

    std::shared_ptr<basic_rtree> get_rtree();

    std::string get_BackingFile();
private:
    RStree_basic(std::string input_file, std::string file_prefix, bool cleanup_input);

    RStree_basic(std::string input_file, time_t construction_time);

    std::string m_file_backend;
    //std::weak_ptr<Rstree_basic> mp_selfptr;

    time_t m_constructionTime;
    long m_size;

    std::shared_ptr<basic_rtree> mp_data;

    // check if a file exists
    bool fexists(const char *filename);
};
