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

#include "server_code/protobuf/sampling_api.pb.h"

class query_cursor
{
public:
    virtual ~query_cursor() { };

    // get the settings requested for the cursor 
    // (what to return as part of the returned elements)
    virtual bool returning_OID() = 0;
    virtual bool returning_location() = 0;
    virtual bool returning_payload() = 0;
    virtual bool returning_time() = 0;

    // get the ttl for the object
    virtual int get_ttl() = 0;

    // return true if this cursor is expired (lived to the end of its ttl) and is ready to be clearned up
    virtual bool is_expired() = 0;

    // get the counted number of elements in the requested range for this query
    virtual long get_total_elements_in_query_range() = 0;

    virtual long get_elements_analyzed_count() = 0;

    // return the region this query is going to sample from
    virtual serverProto::box get_query_region() = 0;

    // perform the query and return a query response with the requested data
    virtual void perform_query(int count, serverProto::QueryResponse&) = 0;
};
