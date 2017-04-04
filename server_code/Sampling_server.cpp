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
#include "Sampling_server.h"

#include "stxxl/timer"

#include <glog/logging.h>

using grpc::Server;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using namespace serverProto;

Sampling_server::Sampling_server(bool attemptRestore, bool autoSave)
    : mp_structureRepo(new sampling_structure_repository(attemptRestore))
    , mp_queryRepo(new query_cursor_repository())
{
    mp_structureRepo->set_autosave(autoSave);
}


Sampling_server::~Sampling_server()
{
}


grpc::Status Sampling_server::ListSamplingStructure(grpc::ServerContext* context
    , const serverProto::ListStructuresRequest* request
    , serverProto::ListStructuresResponse* response)
{
    LOG(INFO) << "Request to list existing structures";

    stxxl::timer responseTime;
    stxxl::timer lockTime;

    responseTime.start();
    lockTime.start();

    // request the list from the database
    m_structureRepoLock.lock();
    lockTime.stop();
    mp_structureRepo->list_structures_proto(*response);
    m_structureRepoLock.unlock();

    responseTime.stop();

    LOG(INFO) << "Finished list existing structures (response time=" << responseTime.mseconds() << "ms)";

    return Status::OK;
}

grpc::Status Sampling_server::BuildStructure(grpc::ServerContext* context
    , const serverProto::BuildRequest* request
    , serverProto::BuildResponse* response)
{
    LOG(INFO) << "Received a request to build a structure name=" << request->name();
    // request a structure to be built using the provided request
    m_structureRepoLock.lock();
    LOG(INFO) << "recieved lock, next we will start building";
    Status status = mp_structureRepo->add_new_structure(*request, response);
    m_structureRepoLock.unlock();

    return status;
}

grpc::Status Sampling_server::DropStructure(grpc::ServerContext* context
    , const serverProto::DropRequest* request
    , serverProto::DropResponse* response)
{
    LOG(INFO) << "Received a request to drop a structure";

    m_structureRepoLock.lock();
    Status status = mp_structureRepo->drop_structure(*request, *response);
    m_structureRepoLock.unlock();

    return status;
}

grpc::Status Sampling_server::StartQuery(grpc::ServerContext* context
    , const serverProto::StartQueryRequest* request
    , serverProto::StartQueryResponse* response)
{
    Status status;

    stxxl::timer responseTime;
    stxxl::timer lockTime;

    lockTime.start();
    responseTime.start();

    // get the requested structure
    std::lock(m_structureRepoLock, m_queryRepoLock);
    lockTime.stop();
    auto structure = mp_structureRepo->get_structure(request->structure_name());

    if (request->query_region().min_point().lat() < -90.0f || request->query_region().max_point().lat() > 90.0f)
        LOG(WARNING) << "latitude value outside expected range -90<" << request->query_region().min_point().lat() << '<' << request->query_region().max_point().lat() << '<' << "90";
    if (request->query_region().min_point().lon() < -180.0f || request->query_region().max_point().lon() > 180.0f)
        LOG(WARNING) << "longitude value outside expected range -180<" << request->query_region().min_point().lon() << '<' << request->query_region().max_point().lon() << '<' << "180";

    // if it got a data structure back
    if (structure)
    {
        LOG(INFO) << "**Starting to generate new query";
        auto p_query = structure->get_query_cursor(*request);

        LOG(INFO) << "**Now adding to the manager";

        mp_queryRepo->add_new_query_proto(std::move(p_query), *response);

        LOG(INFO) << "Started q=" << response->query_id() << " on " << request->structure_name() 
            << " range (lat,lon) (" << request->query_region().min_point().lat() << ',' << request->query_region().min_point().lon() 
            << ")->("               << request->query_region().max_point().lat() << ',' << request->query_region().max_point().lon() << ')';
    }
    else
    {
        status = grpc::Status(grpc::StatusCode::NOT_FOUND, "Unable to find sampling structure requested (" + request->structure_name() + ")");
    }
    m_structureRepoLock.unlock();
    m_queryRepoLock.unlock();
    responseTime.stop();

    LOG(INFO) << "Finished Start Query (response time=" << responseTime.mseconds() << "ms)";

    return status;
}

grpc::Status Sampling_server::Query(grpc::ServerContext* context
    , const serverProto::QueryRequest* request
    , serverProto::QueryResponse* response)
{
    LOG(INFO) << "Received a request to return query samples for [q=" <<  request->query_id() << ",samples_requested=" << request->elements_to_return() << "]";

    stxxl::timer responseTime;
    stxxl::timer lockTime;

    lockTime.start();
    responseTime.start();

    std::lock(m_structureRepoLock, m_queryRepoLock);
    lockTime.stop();
    mp_queryRepo->perform_query(*request, *response);

    m_queryRepoLock.unlock();
    m_structureRepoLock.unlock();

    responseTime.stop();

    LOG(INFO) << "Finished Query [q=" << request->query_id() << "] (response time=" << responseTime.mseconds() << "ms,samples_collected=" << response->elements_size() << ")";

    return Status::OK;
}

grpc::Status Sampling_server::Insert(grpc::ServerContext* context
    , const serverProto::InsertItemsRequest* request
    , serverProto::InsertItemsResponse* response)
{
    Status status = grpc::Status(grpc::StatusCode::NOT_FOUND, "Unable to find sampling structure requested (" + request->name() + ")");
    LOG(INFO) << "Insert request (" << request->name() << ", n=" << request->elements_size() << ")";

    m_structureRepoLock.lock();
    auto structure = mp_structureRepo->get_structure(request->name());

    // if it got a data structure back
    if (structure)
    {   
        long pre_element_count = structure->get_size();
        for (int i = 0; i < request->elements_size(); ++i) {
            try {
                LOG(INFO) << "Inserting " << i << '\t' << request->elements(i).oid() << '\t'
                    << request->elements(i).location().lat() << '\t'
                    << request->elements(i).location().lon() << '\t'
                    << request->elements(i).location().time();

                structure->insert(request->elements(i));
            }
            catch (...)
            {
                LOG(ERROR) << "Insert failed on iteration " << i;
                LOG(FATAL) << "FAILED TO INSERT.  FATAL ERROR.";
            }
        }
        structure->flush_buffers();
        long post_element_count = structure->get_size();

        LOG(INFO) << "Inserted " << (post_element_count - pre_element_count) << " elements into " << request->name();
    }
    else
    {
        status = grpc::Status(grpc::StatusCode::NOT_FOUND, "Unable to find sampling structure requested (" + request->name() + ")");
    }


    m_structureRepoLock.unlock();

    return status;
}

int Sampling_server::garbageCollectQueries()
{
    stxxl::timer responseTime;
    stxxl::timer lockTime;

    responseTime.start();
    lockTime.start();

    m_queryRepoLock.lock();
    lockTime.stop();
    int count = mp_queryRepo->garbage_collect();
    m_queryRepoLock.unlock();

    responseTime.stop();

    if (count > 0)
        LOG(INFO) << "Garbage collected " << count << " old queries (response time=" << responseTime.mseconds() << "ms)";
}
