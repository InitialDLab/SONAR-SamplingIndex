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
// A server cli written in C++ to look to do some basic interactions with the rpc server

#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <limits>
#include <thread>
#include <chrono>
#include <random>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>

#include "server_code/protobuf/sampling_api.grpc.pb.h"
#include "server_code/protobuf/sampling_api.pb.h"

#include "json_spirit/json_spirit_reader_template.h"
#include "json_spirit/json_spirit_writer_template.h"

#include "tclap/CmdLine.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using namespace serverProto;
using namespace std;

struct sampling_cli_settings
{
    std::string server_name;
    int port_number;
    std::string connection_string;

    bool request_listing;
    bool request_continue_query;
    bool request_create_query;
    bool request_build;
    bool request_delete;

    // options to collect different data items during a query
    bool collect_oid;
    bool collect_loc;
    bool collect_time;

    int printing_arguments;

    bool pretty_print;

    // arguments to create a query
    float min_lat;
    float min_lon;
    float max_lat;
    float max_lon;
    int min_time;
    int max_time;

    // arguments to continue a query
    int query_id;
    int request_count;

    // arguments to list the data structures

    // arguments to build a new collection.
    std::string collection_path;
    std::string collection_name;
};

sampling_cli_settings settings;
std::shared_ptr<SamplingDatabase::Stub> mp_stub_;

void connect()
{
    auto myChannel = grpc::CreateChannel(settings.connection_string, grpc::InsecureChannelCredentials());
    mp_stub_ = SamplingDatabase::NewStub(myChannel);
}

void list_data()
{
    ListStructuresRequest request;
    ListStructuresResponse reply;
    ClientContext context;

    Status status = mp_stub_->ListSamplingStructure(&context, request, &reply);
    if (status.ok())
    {
        // setup JSON data structure
        json_spirit::Array write_arr;

        // add to array of objects
        for (int i = 0; i < reply.structures_size(); ++i)
        {
            json_spirit::Object write_obj;

            write_obj.push_back(json_spirit::Pair("name", reply.structures(i).name()));
            write_obj.push_back(json_spirit::Pair("age", reply.structures(i).sec_since_rebuild()));
            write_obj.push_back(json_spirit::Pair("size", reply.structures(i).count()));
            write_obj.push_back(json_spirit::Pair("payload", reply.structures(i).payload_type()));

            // add information to JSON string to be printed!
            write_arr.push_back(write_obj);
        }

        // write the results
        std::cout << json_spirit::write_string(json_spirit::Value(write_arr), settings.printing_arguments);
    }
    else
    {
        std::cerr << "Unable to get a list of structures in server " << status.error_code() << std::endl;
        std::cerr << status.error_message() << std::endl;
    }
}

void build_index(){
    if (settings.collection_name.size() == 0)
    {
        std::cerr << "Must specify the name of the collection to build" << std::endl;
        exit(-1);
    }
    if (settings.collection_path.size() == 0)
    {
        std::cerr << "Must specify the location of the data to use to build the index" << std::endl;
        exit(-1);
    }

    BuildRequest request;
    request.set_name(settings.collection_name);
    request.set_force(false);
    request.set_payload_type(SampleStructureType::NO_PAYLOAD);
    request.set_input_location(settings.collection_path);
    request.set_remove_input(false);

    BuildResponse reply;
    ClientContext context;

    Status status = mp_stub_->BuildStructure(&context, request, &reply);
    if (status.ok())
    {
        // if it was built, return all the indexes stored by the server
        // (this should include the new item)
        list_data();
    }
    else
    {
        std::cerr << "Unable to build index for " << settings.collection_name << std::endl;
        std::cerr << status.error_message() << std::endl;
    }
}

void drop_index(){
    if (settings.collection_name.size() == 0)
    {
        std::cerr << "Must specify the name of the collection to drop" << std::endl;
        exit(-1);
    }

    DropRequest request;
    request.set_structure_name(settings.collection_name);

    DropResponse reply;
    ClientContext context;

    Status status = mp_stub_->DropStructure(&context, request, &reply);
    if (status.ok())
    {
        // if it was built, return all the indexes stored by the server
        // (this should include the new item)
        list_data();
    }
    else
    {
        std::cerr << "Unable to drop index for " << settings.collection_name << std::endl;
        std::cerr << status.error_message() << std::endl;
    }
}

// start a query
void start_query(){
    StartQueryRequest request;

    request.set_structure_name(settings.collection_name);
    request.mutable_query_region()->mutable_min_point()->set_lat(settings.min_lat);
    request.mutable_query_region()->mutable_min_point()->set_lon(settings.min_lon);
    request.mutable_query_region()->mutable_min_point()->set_time(settings.min_time);
    request.mutable_query_region()->mutable_max_point()->set_lat(settings.max_lat);
    request.mutable_query_region()->mutable_max_point()->set_lon(settings.max_lon);
    request.mutable_query_region()->mutable_max_point()->set_time(settings.max_time);

    request.set_return_oid(settings.collect_oid);
    request.set_return_location(settings.collect_loc);
    request.set_return_time(settings.collect_time);
    request.set_suggested_ttl(60);

    std::cout << settings.min_lat << std::endl;
    std::cout << settings.min_lon << std::endl;
    std::cout << settings.min_time << std::endl;
    std::cout << settings.max_lat << std::endl;
    std::cout << settings.max_lon << std::endl;
    std::cout << settings.max_time << std::endl;

    StartQueryResponse reply;
    ClientContext context;

    Status status = mp_stub_->StartQuery(&context, request, &reply);
    if (status.ok())
    {
        settings.query_id = reply.query_id();

        // if (!settings.request_continue_query)
        //     // TODO: output the query id using JSON
        // std::cout << "THE NEW QUERY ID IS: " << settings.query_id << std::endl;
    }
    else
    {
        std::cerr << "Unable to start query for index " << settings.collection_name << std::endl;
        std::cerr << status.error_message() << std::endl;
    }
}

void continue_query(){
    QueryRequest request;

    request.set_query_id(settings.query_id);
    request.set_elements_to_return(settings.request_count);

    QueryResponse reply;
    ClientContext context;

    Status status = mp_stub_->Query(&context, request, &reply);
    if (status.ok())
    {
        // setup JSON data structure
        json_spirit::Object write_output;
        json_spirit::Array write_arr;

        bool arr_write = false;

        // add to array of objects
        for (int i = 0; i < reply.elements_size(); ++i)
        {
            json_spirit::Object write_obj;
            // we will only write the item if it containts data
            bool write = false;

            if (reply.elements(i).oid() != ""){
                write = true;
                write_obj.push_back(json_spirit::Pair("oid", reply.elements(i).oid()));
            }
            if (reply.elements(i).has_location()
                && !(reply.elements(i).location().lat() == 0.0 && reply.elements(i).location().lon() == 0.0)) {
                write = true;
                write_obj.push_back(json_spirit::Pair("lat", reply.elements(i).location().lat()));
                write_obj.push_back(json_spirit::Pair("lon", reply.elements(i).location().lon()));

            }
            if (reply.elements(i).location().time() != 0){
                write = true;
                write_obj.push_back(json_spirit::Pair("time", reply.elements(i).location().time()));
            }

            // add information to JSON string to be printed!
            if (write){
                arr_write = true;
                write_arr.push_back(write_obj);
            }
        }

        if (arr_write)
            write_output.push_back(json_spirit::Pair("data", write_arr));
        write_output.push_back(json_spirit::Pair("Qid", settings.query_id));

        // write the results
        std::cout << json_spirit::write_string(json_spirit::Value(write_output), settings.printing_arguments);
    }
    else
    {
        std::cerr << "Unable to get results for the query " << status.error_code() << std::endl;
        std::cerr << status.error_message() << std::endl;
        exit(0);
    }
}

int main(int argc, char *argv[])
{
    TCLAP::CmdLine cmd("Interact with the sampling server using a convenent command line interface", ' ', "0.1");

    TCLAP::ValueArg<int> arg_port("p", "port", "port number to submit requests", false, 40053, "int", cmd);
    TCLAP::ValueArg<std::string> arg_server("s", "server", "server name", false, "localhost", "string", cmd);
    TCLAP::ValueArg<std::string> arg_indexName("i", "index", "Index name to interact with", false, "", "string", cmd);
    TCLAP::ValueArg<std::string> arg_sourceLocation("", "data_source", "data source to build a new index.  The path must be local on the machine the server is running", false, "", "string", cmd);

    TCLAP::ValueArg<float> arg_latmin("", "latmin", "minimum latitude for query", false, -90, "float", cmd);
    TCLAP::ValueArg<float> arg_latmax("", "latmax", "maximum latitude for query", false, 90, "float", cmd);
    TCLAP::ValueArg<float> arg_lonmin("", "lonmin", "minimum longitude for query", false, -180, "float", cmd);
    TCLAP::ValueArg<float> arg_lonmax("", "lonmax", "maximum longitude for query", false, 180, "float", cmd);
    TCLAP::ValueArg<long> arg_timemin("", "timemin", "minimum time for query (seconds since epoch)", false, 0, "int", cmd);
    TCLAP::ValueArg<long> arg_timemax("", "timemax", "maximum time for query (seconds since epoch)", false, std::numeric_limits<int>::max(), "int", cmd);

    TCLAP::ValueArg<int> arg_QueryId("", "queryid", "query id for a running query (used to collect additional items from an active query", false, 0, "int", cmd);
    TCLAP::ValueArg<int> arg_requestCount("", "count", "number of results to request for a query", false, 0, "int", cmd);

    TCLAP::SwitchArg arg_list("l", "list", "list information about the indexes registered in the server", cmd, false);
    TCLAP::SwitchArg arg_pretty("", "pretty", "print JSON pretty, so people like you can read it better", cmd, false);
    TCLAP::SwitchArg arg_build("b", "build_index", "run the build index command on the remote server", cmd, false);
    TCLAP::SwitchArg arg_drop("d", "drop_index", "drop the index specified on the remote server", cmd, false);
    TCLAP::SwitchArg arg_startQuery("", "start_query", "start a query using the arguments provided", cmd, false);
    TCLAP::SwitchArg arg_collectResults("", "get_results", "return results from a running query (a query can be started and continued at the same time)", cmd, false);
    TCLAP::SwitchArg arg_requestOID("", "q_oid", "when creating a new query, request that OIDs should be returned", cmd, false);
    TCLAP::SwitchArg arg_requestloc("", "q_loc", "when creating a new query, request that location should be returned (lat, lon)", cmd, false);
    TCLAP::SwitchArg arg_requesttime("", "q_time", "when creating a new query, request that time should be returned in results", cmd, false);

    cmd.parse(argc, argv);

    settings.server_name = arg_server.getValue();
    settings.port_number = arg_port.getValue();

    settings.collection_name = arg_indexName.getValue();
    settings.collection_path = arg_sourceLocation.getValue();

    settings.request_listing        = arg_list.getValue();
    settings.request_build          = arg_build.getValue();
    settings.request_delete         = arg_drop.getValue();
    settings.request_continue_query = arg_collectResults.getValue();
    settings.request_create_query   = arg_startQuery.getValue();
    settings.pretty_print           = arg_pretty.getValue();

    settings.collect_oid = arg_requestOID.getValue();
    settings.collect_loc = arg_requestloc.getValue();
    settings.collect_time = arg_requesttime.getValue();

    settings.min_lat  = arg_latmin.getValue();
    settings.min_lon  = arg_lonmin.getValue();
    settings.min_time = arg_timemin.getValue();
    settings.max_lat  = arg_latmax.getValue();
    settings.max_lon  = arg_lonmax.getValue();
    settings.max_time = arg_timemax.getValue();

    settings.query_id = arg_QueryId.getValue();
    settings.request_count = arg_requestCount.getValue();

    // setup printing arguments
    settings.printing_arguments = 0;
    settings.printing_arguments |= settings.pretty_print ? json_spirit::pretty_print : 0;

    // setup connection string
    settings.connection_string = settings.server_name + ":" + std::to_string(settings.port_number);
    
    connect();

    if (settings.request_listing)
    {
        list_data();
    }
    else if (settings.request_build)
    {
        build_index();
    }
    else if (settings.request_delete)
    {
        drop_index();
    }
    else {
        if (settings.request_create_query)
        {
            //std::cout << "starting to submit request for create" << std::endl;
            start_query();
        }
        if (settings.request_continue_query || settings.request_create_query)
        {
            //std::cout << "starting to submit request for continue" << std::endl;
            continue_query();
        }
    }

    std::cout << std::endl;

    return 0;
}