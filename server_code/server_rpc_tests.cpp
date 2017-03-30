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
//
// This is used to perform several actions agains the Sampling_server to see if it is working
//


#include <iostream>
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

using grpc::ChannelArguments;
//using grpc::ChannelInterface;
using grpc::ClientContext;
using grpc::Status;
using namespace serverProto;
using namespace std;


std::string oid_generator(std::default_random_engine &generator, int digits){
    std::string item = "";
    std::uniform_int_distribution<int> distribution(0, 15);
    for (int i = 0; i < digits; ++i)
        item += "0123456789ABCDEF"[distribution(generator)];
 
    return item;
}

bool test_create_structure(std::shared_ptr<SamplingDatabase::Stub> stub_, string name, string input_location)
{
    BuildRequest request;
    request.set_name(name);
    request.set_force(false);
    request.set_payload_type(SampleStructureType::NO_PAYLOAD);
    request.set_input_location(input_location);
    request.set_remove_input(false);

    BuildResponse reply;
    ClientContext context;

    std::cout << "sending request to server to build " << name << std::endl;

    Status status = stub_->BuildStructure(&context, request, &reply);
    if (status.ok()) {
        std::cout << "reported successful" << std::endl;
        return true;
    }
    else
    {
        std::cout << "reported unsuccessful with status " << status.error_code() << std::endl;
        std::cout << status.error_code() << std::endl;
        return false;
    }
}

bool test_drop_structure(std::shared_ptr<SamplingDatabase::Stub> stub_, string name)
{
    DropRequest request;
    request.set_structure_name(name);

    DropResponse reply;
    ClientContext context;

    std::cout << "sending request to drop " << name << std::endl;

    Status status = stub_->DropStructure(&context, request, &reply);
    if (status.ok()) {
        std::cout << "reported successful" << std::endl;
        return true;
    }
    else
    {
        std::cout << "reported unsuccessful with status " << status.error_code() << std::endl;
        std::cout << status.error_code() << std::endl;
        return false;
    }
}

bool test_list_structures(std::shared_ptr<SamplingDatabase::Stub> stub_)
{
    ListStructuresRequest request;

    ListStructuresResponse reply;
    ClientContext context;

    std::cout << "sending list request" << std::endl;

    Status status = stub_->ListSamplingStructure(&context, request, &reply);
    if (status.ok()){
        std::cout << "reported successful.  Contents of list follow:" << std::endl;
        std::cout << "\t" << reply.structures_size() << " data structures stored" << std::endl;
        for (int i = 0; i < reply.structures_size(); ++i)
        {
            std::cout << "" << i << "\t" << reply.structures(i).name() << std::endl;
            std::cout << "\t" << "sec since rebuild: " << reply.structures(i).sec_since_rebuild() << std::endl;
            std::cout << "\t" << "size of data structure: " << reply.structures(i).count() << std::endl;
            std::cout << "\t" << "payload: " << reply.structures(i).payload_type() << std::endl;
        }
    }
    else
    {
        std::cout << "reported unsuccessful with status " << status.error_code() << std::endl;
        std::cout << status.error_code() << std::endl;
        return false;
    }
}

int test_start_query(std::shared_ptr<SamplingDatabase::Stub> stub_, string name)
{
    StartQueryRequest request;
    request.set_structure_name(name);
    request.set_return_oid(true);
    request.set_return_location(true);
    request.set_return_time(true);
    request.set_return_payload(true);
    request.set_suggested_ttl(20);

    request.mutable_query_region()->mutable_min_point()->set_lat(-90.0f);
    request.mutable_query_region()->mutable_min_point()->set_lon(-180.0f);
    request.mutable_query_region()->mutable_min_point()->set_time(0);
    request.mutable_query_region()->mutable_max_point()->set_lat(90.0f);
    request.mutable_query_region()->mutable_max_point()->set_lon(180.0f);
    request.mutable_query_region()->mutable_max_point()->set_time(std::numeric_limits<int>::max());

    StartQueryResponse reply;
    ClientContext context;

    std::cout << "sending request start query on " << name << std::endl;

    Status status = stub_->StartQuery(&context, request, &reply);
    if (status.ok()) {
        std::cout << "reported successful with id=" << reply.query_id() << std::endl;
        return reply.query_id();
    }
    else
    {
        std::cout << "reported unsuccessful with status " << status.error_code() << std::endl;
        std::cout << status.error_code() << std::endl;
        return false;
    }
}

void printStats(const ElementStatistics& stats)
{
    std::cout << "size:  " << stats.sample_size() << std::endl;
    std::cout << "total: " << stats.total_count() << std::endl;
    std::cout << "mean:  " << stats.mean() << std::endl;
    std::cout << "stdev: " << stats.stdev() << std::endl;
    if (stats.type() == StatisicalType::INT_TYPE)
        std::cout << "int_min: " << stats.int_min() << std::endl;
    if (stats.type() == StatisicalType::FLOAT_TYPE)
        std::cout << "float_min: " << stats.float_min() << std::endl;
    if (stats.type() == StatisicalType::INT_TYPE)
        std::cout << "int_max: " << stats.int_max() << std::endl;
    if (stats.type() == StatisicalType::FLOAT_TYPE)
        std::cout << "float_max: " << stats.float_max() << std::endl;
}

bool test_request_query(std::shared_ptr<SamplingDatabase::Stub> stub_, int q_id, int count)
{
    QueryRequest request;
    request.set_query_id(q_id);
    request.set_elements_to_return(count);

    QueryResponse reply;
    ClientContext context;

    std::cout << "issuing query for qid=" << q_id << std::endl;

    Status status = stub_->Query(&context, request, &reply);
    if (status.ok()) {
        std::cout << "reported successful.  Listing items from query: " << std::endl;
        std::cout << " samples returned: " << reply.sample_count_last() << std::endl;
        std::cout << " total samples viewed for query: " << reply.sample_count_total() << std::endl;
        
        /*
        std::cout << "lat_last stats" << std::endl;
        printStats(reply.lat_last());
        std::cout << "lon_last stats" << std::endl;
        printStats(reply.lon_last());
        std::cout << "time_last stats" << std::endl;
        printStats(reply.time_last());
        
        std::cout << std::endl;
        std::cout << "lat_total stats" << std::endl;
        printStats(reply.lat_total());
        std::cout << "lon_total stats" << std::endl;
        printStats(reply.lon_total());
        std::cout << "time_total stats" << std::endl;
        printStats(reply.time_total());


        for (int i = 0; i < reply.elements_size(); i++)
        {
            std::cout << "item " << i << ' ';
            std::cout << reply.elements(i).oid() << " (" << reply.elements(i).location().lat() << ',' << reply.elements(i).location().lon() << ',' << reply.elements(i).location().time() << ")" << std::endl;
        }
        */
    }
    else
    {
        std::cout << "reported unsuccessful with status " << status.error_code() << std::endl;
        std::cout << status.error_message() << std::endl;
        return false;
    }
}

int main()
{
    grpc_init();

    std::cout << "creating channel and connection" << std::endl;
    auto myChannel = grpc::CreateChannel("localhost:40053", grpc::InsecureChannelCredentials());
    std::shared_ptr<SamplingDatabase::Stub> myStub(SamplingDatabase::NewStub(myChannel));

    bool ret_val;

    int number_of_requests = 100000;
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);
    std::uniform_int_distribution<int> request_item(1, 9);
    std::uniform_int_distribution<int> items_to_request(10, 10000);
    std::uniform_real_distribution<float> longitude_generator(-180.0, 180.0);
    std::uniform_real_distribution<float> latitude_generator(-90.0, 90.0);
    std::uniform_int_distribution<int> time_generator(std::numeric_limits<int>::max() / 2, std::numeric_limits<int>::max());
    std::uniform_int_distribution<int> items_to_insert_generator(1, 100);

    test_create_structure(myStub, "test", "testdata.csv");

    std::vector<int> query_ids_available;

    for (int i = 0; i < number_of_requests; ++i)
    {
        std::cout << "item " << i << std::endl;

        switch (request_item(generator))
        {
        case 1: case 2: case 3:
            // query
            query_ids_available.push_back(test_start_query(myStub, "test"));
            break;
        case 4:
            // list structures
            test_list_structures(myStub);
            break;
        case 5: case 6: case 7: case 8:
        {
            // get results from query
            if (query_ids_available.size() == 0) {
                continue;
            }
            std::uniform_int_distribution<int> query_to_use(0, query_ids_available.size());

            int qid = query_ids_available[query_to_use(generator)];

            test_request_query(myStub, qid, items_to_request(generator));

            break;
        }
        case 9:
            int items_to_insert = items_to_insert_generator(generator);
            InsertItemsRequest request;
            request.set_name("test");
            for (int i = 0; i < items_to_insert; ++i) {
                auto item = request.add_elements();
                item->set_oid(oid_generator(generator, 24));
                item->mutable_location()->set_lat(latitude_generator(generator));
                item->mutable_location()->set_lon(longitude_generator(generator));
                item->mutable_location()->set_time(time_generator(generator));

                std::cout << "inserting: " << item->oid() << "," << item->location().lat() << "," << item->location().lon() << "," << item->location().time() << "\n";
            }

            // submit the query
            InsertItemsResponse reply;
            ClientContext context;

            std::cout << "sending insert query query on " << "test" << std::endl;

            Status status = myStub->Insert(&context, request, &reply);
            if (status.ok()) {
                std::cout << "Insertion" << std::endl;
            }
            else
            {
                std::cout << "reported unsuccessful Insertion with status " << status.error_code() << std::endl;
                std::cout << status.error_code() << std::endl;
            }
            break;
        }

    }


    //std::cout << "listing structures (should be empty)" << std::endl;
    //test_list_structures(myStub);

    //std::cout << "creating test1" << std::endl;
    //ret_val = test_create_structure(myStub, "test1", "/home/robertc/data/small.csv");
    //std::cout << "creating test2" << std::endl;
    //ret_val = test_create_structure(myStub, "test2", "/home/robertc/data/small.csv");

    //std::cout << "listing structures (should have 2)" << std::endl;
    //test_list_structures(myStub);

    //std::cout << "Droping test1" << std::endl;
    //test_drop_structure(myStub, "test1");

    //std::cout << "listing structures (should be 1)" << std::endl;
    //test_list_structures(myStub);

    //std::cout << "starting a query" << std::endl;
    //int qid = test_start_query(myStub, "test2");

    //std::cout << "doing query 100 elements" << std::endl;
    //test_request_query(myStub, qid, 100);

    //std::cout << "doing query 100 elements (again)" << std::endl;
    //test_request_query(myStub, qid, 100);

    //// create a lot of usless queries
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //qid = test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");
    //test_start_query(myStub, "test2");

    ////std::this_thread::sleep_for(std::chrono::seconds(1));
    //test_request_query(myStub, qid, 1);

    //std::cout << "qid:" << qid << " should live a little longer" << std::endl;


    std::cout << "Done" << std::endl;
    return 0;
}