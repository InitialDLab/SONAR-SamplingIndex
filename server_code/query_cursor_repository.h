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
// This class is a repository for several queries running in the system.
// look up performed by giving the query id.

#include <memory>
#include <string>
#include <map>
#include <atomic>
#include <mutex>

#include "server_code/query_cursor.h"

#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/protobuf/sampling_api.grpc.pb.h"

typedef int QUERY_ID;

class query_cursor_repository
{
public:
    query_cursor_repository();

    virtual ~query_cursor_repository();

    // adds a particular query to the repository.  Returns the ID of that
    // query
    QUERY_ID add_new_query(std::unique_ptr<query_cursor> new_query);
    QUERY_ID add_new_query_proto(std::unique_ptr<query_cursor> new_query, serverProto::StartQueryResponse& response);


    // get data from a particular queryid
    grpc::Status perform_query(const serverProto::QueryRequest &request, serverProto::QueryResponse&);
    grpc::Status perform_query(QUERY_ID, int elements_requested, serverProto::QueryResponse&);

    // scan the table of queries and remove those which are expired
    int garbage_collect();

private:
    std::atomic<QUERY_ID> current_id;

    std::map<QUERY_ID, std::unique_ptr<query_cursor> > m_repository;

    std::recursive_mutex m_lock;
};
