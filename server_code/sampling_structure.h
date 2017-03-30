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

#include <ctime>
#include <memory>

#include "server_code/protobuf/sampling_api.pb.h"

#include "server_code/query_cursor.h"

class sampling_structure
{
public:
    sampling_structure()
        : m_cleanupAfterUse(false)
    { };

    virtual ~sampling_structure(){};

    // retrieves a cursor which is used to get samples from this data structure
    virtual std::unique_ptr<query_cursor> get_query_cursor(const serverProto::StartQueryRequest&) = 0;

    virtual serverProto::SampleStructureType getPayloadType() = 0;

    // insert the provided element into the sampling structure
    virtual void insert(const serverProto::element&) = 0;

    // get how many elements are in the sampling index
    virtual long get_size() = 0;

    // returns the time when the structure was built
    virtual time_t get_construction_time() = 0;
    
    // returns the time when the samples for the structure were rebuilt
    //virtual time_t get_samples_build_time() = 0;

    // this will return the file which backs this data structure.  This
    // is useful if you want to save the state of the database.mp_repository
    virtual std::string get_BackingFile() = 0;

    virtual bool flush_buffers() = 0;

    bool set_cleanupAfterUse(bool value)
    {
        m_cleanupAfterUse = value;
    }

protected:
    bool m_cleanupAfterUse;
};
