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

#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/query_cursor.h"
#include "server_code/query_cursor_repository.h"
#include "server_code/sampling_structure.h"
#include "server_code/sampling_structure_repository.h"

class Sampling_server final:
    public serverProto::SamplingDatabase::Service
{
public:
    Sampling_server(bool attemptRestore = false, bool autoSave = false);
    virtual ~Sampling_server();

    grpc::Status ListSamplingStructure(grpc::ServerContext* context
                                     , const serverProto::ListStructuresRequest* request
                                     , serverProto::ListStructuresResponse* response) override;

    grpc::Status BuildStructure(grpc::ServerContext* context
                              , const serverProto::BuildRequest* request
                              , serverProto::BuildResponse* response) override;

    grpc::Status DropStructure(grpc::ServerContext* context
                             , const serverProto::DropRequest* request
                             , serverProto::DropResponse* response) override;

    grpc::Status StartQuery(grpc::ServerContext* context
                          , const serverProto::StartQueryRequest* request
                          , serverProto::StartQueryResponse* response) override;

    grpc::Status Query(grpc::ServerContext* context
                     , const serverProto::QueryRequest* request
                     , serverProto::QueryResponse* response) override;

    grpc::Status Insert(grpc::ServerContext* context
                      , const serverProto::InsertItemsRequest* request
                      , serverProto::InsertItemsResponse* response) override;

    // cleanup old queries and return the number of queries deleted
    int garbageCollectQueries();
private:
    // data structure repository for this server
    std::unique_ptr<sampling_structure_repository> mp_structureRepo;
    //std::recursive_mutex m_structureRepoLock;

    // data structure to manage the running queries
    std::unique_ptr<query_cursor_repository> mp_queryRepo;
    //std::recursive_mutex m_queryRepoLock;
    
};
