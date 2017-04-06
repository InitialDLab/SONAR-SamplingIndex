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
// Implementation of the cursor storage system
// This is used as a lookup table to find a cursor based on an ID.
// the repository will also periodically cleanup unused cursors periodically

// NOTE: A map is being used as the data store for the running queries.
// according to cplusplus.com concurrently accessing elements is
// safe during erase and insert, but iterating ranges is not safe.
// Because of this statement, inserting elements will be
// done unprotected.  erasing elements will be done in batches,
// so it will be protected.  I read the words on the site, but
// I am not sure if I am interpreting them correctly.  Look at
// this is bugs happen.

#include <memory>

#include <glog/logging.h>

#include "server_code/protobuf/sampling_api.grpc.pb.h"
#include "server_code/protobuf/sampling_api.pb.h"

#include "query_cursor_repository.h"

query_cursor_repository::query_cursor_repository()
    : current_id(0)
{
    LOG(INFO) << "Creating a query repository";
}

query_cursor_repository::~query_cursor_repository()
{
    LOG(INFO) << "Cleaning up a query repository";
}

QUERY_ID query_cursor_repository::add_new_query(std::unique_ptr<query_cursor> new_query)
{
    // get the query ID of the new value
    QUERY_ID new_id = ++current_id;

    // insert the new query into the repository
    m_lock.lock();
    m_repository.insert(std::pair<QUERY_ID, std::unique_ptr<query_cursor> >(new_id, move(new_query)));
    m_lock.unlock();

    return new_id;
}

QUERY_ID query_cursor_repository::add_new_query_proto(std::unique_ptr<query_cursor> new_query, serverProto::StartQueryResponse& response)
{
    QUERY_ID toRet = add_new_query(move(new_query));

    response.set_query_id(toRet);

    return toRet;
}

grpc::Status query_cursor_repository::perform_query(QUERY_ID q_id, int elements_requested, serverProto::QueryResponse& toRet)
{
    grpc::Status status;
    // lookup the pointer for a query_id
    m_lock.lock();
    auto p_elm = m_repository.find(q_id);
    m_lock.unlock();

    if (p_elm == m_repository.end())
    {
        if (q_id < current_id)
            status = grpc::Status(grpc::StatusCode::NOT_FOUND, "This query was old and was cleaned up");
        else
            status = grpc::Status(grpc::StatusCode::NOT_FOUND, "This query does not exist");
        LOG(INFO) << "Query ID " << q_id << " was requested, but it is not in the database";
        return status;
    }

    // perform the query
    p_elm->second->perform_query(elements_requested, toRet);
    status = grpc::Status::OK;

    return status;
    
}

grpc::Status query_cursor_repository::perform_query(const serverProto::QueryRequest &request, serverProto::QueryResponse& toRet)
{
    QUERY_ID q_id_requested = request.query_id();
    int elements_requested = request.elements_to_return();

    return perform_query(q_id_requested, elements_requested, toRet);
}

int query_cursor_repository::garbage_collect()
{
    int count = 0;

    m_lock.lock();
    auto b_marker = m_repository.begin();
    auto i = m_repository.begin();
    while (i != m_repository.end() && !i->second->is_expired())
        ++i;

    b_marker = i;

    while (i != m_repository.end() && i->second->is_expired())
    {
        ++i;
        ++count;
    }

    // we have found a range of expired elements
    if (b_marker != m_repository.end() && b_marker->second->is_expired())
        i = m_repository.erase(b_marker, i);

    m_lock.unlock();

    LOG_IF(INFO, count > 0) << "cleaned up " << count << " old queries in the repository";
    return count;
}

