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
#include <iostream>
#include <cassert>

#include "server_code/sampling_structure_repository.h"
#include "server_code/query_cursor_repository.h"
#include "server_code/RStree_basic.h"
#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/Sampling_server.h"
#include "server_code/Server_launcher.h"
#include "server_code/server_settings.h"

#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>

#include <glog/logging.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

bool test1()
{
    auto t = RStree_basic::build_RStree_basic("/home/robertc/data/small.csv", "");

    std::cout << "Size: " << t->get_size() << std::endl;

    serverProto::StartQueryRequest request1;

    request1.set_structure_name("TEST");
    request1.set_return_oid(true);
    request1.set_return_location(true);
    request1.set_return_time(true);
    request1.set_suggested_ttl(200);

    auto query_box = request1.mutable_query_region();
    query_box->mutable_min_point()->set_lat(-90.0f);
    query_box->mutable_min_point()->set_lon(-180.0f);
    query_box->mutable_min_point()->set_time(0);
    query_box->mutable_max_point()->set_lat(90.0f);
    query_box->mutable_max_point()->set_lon(180.0f);
    query_box->mutable_max_point()->set_time(2100000000);

    assert(query_box->max_point().lat() == 90.0f);

    auto query = t->get_query_cursor(request1);
    serverProto::QueryResponse response;
    query->perform_query(100, response);

    for (int i = 0; i < response.elements_size(); i++)
    {
        std::cout << response.elements(i).oid() << '\t' << response.elements(i).location().lat() << '\t' << response.elements(i).location().lon() << std::endl;
    }

    response.clear_lat_last();
}

bool test2()
{
    sampling_structure_repository repo;
    query_cursor_repository q_repo;

    // build request
    serverProto::BuildRequest request;
    request.set_name("test");
    request.set_force(false);
    request.set_input_location("/home/robertc/data/small.csv");
    request.set_payload_type(serverProto::SampleStructureType::NO_PAYLOAD);

    repo.add_new_structure(request);

    serverProto::ListStructuresResponse listStructure;
    repo.list_structures_proto(listStructure);

    serverProto::StartQueryRequest request1;

    request1.set_structure_name("TEST");
    request1.set_return_oid(true);
    request1.set_return_location(true);
    request1.set_return_time(true);
    request1.set_suggested_ttl(200);

    auto query_box = request1.mutable_query_region();
    query_box->mutable_min_point()->set_lat(-90.0f);
    query_box->mutable_min_point()->set_lon(-180.0f);
    query_box->mutable_min_point()->set_time(0);
    query_box->mutable_max_point()->set_lat(90.0f);
    query_box->mutable_max_point()->set_lon(180.0f);
    query_box->mutable_max_point()->set_time(2100000000);

    auto temp = repo.get_structure("test")->get_query_cursor(request1);

    serverProto::QueryResponse response;
    temp->perform_query(100, response);

    for (int i = 0; i < response.elements_size(); i++)
    {
        std::cout << response.elements(i).oid() << '\t' << response.elements(i).location().lat() << '\t' << response.elements(i).location().lon() << std::endl;
    }
}

int main(int argc, char* argv[])
{
    // parse arguments
    server_parse_args(argc, argv);

    FLAGS_logtostderr = 1;
    FLAGS_stderrthreshold = 0;

    // set the number of threads

    google::InitGoogleLogging(argv[0]);

    int port_number;
    int garbage_collection_frequency;

    LOG(INFO) << "STARTING (compiled version: " << __DATE__ << " " << __TIME__ << ")";
    LOG(INFO) << "starting port number: " << g_server_settings.port_number;
    LOG(INFO) << "garbage collection frequency: " << g_server_settings.garbage_collection_frequency;

    // initialize general server code
    //grpc_init();

    std::string connection_string = "0.0.0.0:" + std::to_string(g_server_settings.port_number);

    LOG(INFO) << "connection string: " << connection_string;
    Server_launcher launcher(connection_string);
    launcher.Blocking_wait();

    // shutdown general server code
    //grpc_shutdown();

    //BOOST_LOG_TRIVIAL(info) << "Shutting down";

    std::cout << "DONE" << std::endl;
    return 0;
}