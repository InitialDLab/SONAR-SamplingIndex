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
// This helper program can be used to query the data structure
// build in the sampling database

#include <iostream>
#include <cstdlib>
#include <string>
#include <random>
#include <fstream>
#include <unistd.h>

#include <time.h>
#include <cstdlib>
#include <cstdio>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>

#include "server_code/protobuf/sampling_api.grpc.pb.h"
#include "server_code/protobuf/sampling_api.pb.h"

using grpc::ChannelArguments;
using grpc::ClientContext;
using grpc::Status;
using namespace serverProto;
using namespace std;

std::string get_arg(int argc, char *argv[], int i)
{
    if (i < argc)
    {
        return argv[i];
    }
    else
        return "";
}

struct comma_printer
{
    // how many to wait until returning the comma
    comma_printer(int i)
        :m_count(i)
    {

    }

    std::string place()
    {
        if (m_count == 0)
            return ",";
        --m_count;
        return "";
    }

private:
    int m_count;
};

int main(int argc, char *argv[])
{
    int i = 0;

    std::string server_str = get_arg(argc, argv, ++i);
    std::string command_str = get_arg(argc, argv, ++i);

    std::string data_name, smin_x, smin_y, smin_t, smax_x, smax_y, smax_t, requests;
    float min_x, min_y, max_x, max_y;
    int min_t, max_t;

    std::string sQ_id, scount;
    int Q_id, count;

    bool return_oid, return_time, return_loc;


    if (command_str == "start" || command_str == "oneshot")
    {
        data_name = get_arg(argc, argv, ++i);
        smin_x = get_arg(argc, argv, ++i);
        smin_y = get_arg(argc, argv, ++i);
        smin_t = get_arg(argc, argv, ++i);
        smax_x = get_arg(argc, argv, ++i);
        smax_y = get_arg(argc, argv, ++i);
        smax_t = get_arg(argc, argv, ++i);
        requests = get_arg(argc, argv, ++i);

        min_x = stof(smin_x);
        min_y = stof(smin_y);
        min_t = stoi(smin_t);
        max_x = stof(smax_x);
        max_y = stof(smax_y);
        max_t = stoi(smax_t);

        return_oid = (requests.find("oid") != std::string::npos);
        return_time = (requests.find("time") != std::string::npos);
        return_loc = (requests.find("location") != std::string::npos);
    }
    if (command_str == "query")
    {
        sQ_id = get_arg(argc, argv, ++i);
        Q_id = stoi(sQ_id);
    }
    if (command_str == "query" || command_str == "oneshot")
    {
        scount = get_arg(argc, argv, ++i);
        count = stoi(scount);
    }

    auto myChannel = grpc::CreateChannel(server_str + ":40053", grpc::InsecureChannelCredentials());
    auto p_stub_ = SamplingDatabase::NewStub(myChannel);

    // if needed, setup new query and output result
    if (command_str == "start" || command_str == "oneshot")
    {
        StartQueryRequest request;

        request.set_structure_name(data_name);
        request.mutable_query_region()->mutable_min_point()->set_lat(min_x);
        request.mutable_query_region()->mutable_min_point()->set_lon(min_y);
        request.mutable_query_region()->mutable_min_point()->set_time(min_t);
        request.mutable_query_region()->mutable_max_point()->set_lat(max_x);
        request.mutable_query_region()->mutable_max_point()->set_lon(max_y);
        request.mutable_query_region()->mutable_max_point()->set_time(max_t);
        request.set_return_oid(return_oid);
        request.set_return_location(return_loc);
        request.set_return_time(return_time);
        request.set_suggested_ttl(30);

        StartQueryResponse reply;
        ClientContext context;

        Status status = p_stub_->StartQuery(&context, request, &reply);
        if (status.ok())
        {
            Q_id = reply.query_id();
            if (command_str == "start")
                std::cout << "[ \"Qid\" : " << reply.query_id() << "]" << std::endl;
        }
        else
        {
            std::cerr << "ERROR: " << status.error_code() << std::endl;
        }
    }
    if (command_str == "query" || command_str == "oneshot")
    {
        QueryRequest request;

        request.set_query_id(Q_id);
        request.set_elements_to_return(count);

        QueryResponse reply;
        ClientContext context;

        Status status = p_stub_->Query(&context, request, &reply);
        if (status.ok())
        {
            std::cout.precision(11);
            std::cout << '[';
            comma_printer document(1);
            for (size_t i = 0; i < reply.elements_size(); ++i)
            {
                std::cout << document.place();

                std::cout << '{';
                comma_printer record(1);

                if (reply.elements(i).oid() != "")
                    std::cout << record.place() << "\"oid\":\"" << reply.elements(i).oid() << '\"';
                if (reply.elements(i).has_location() 
                    && !(reply.elements(i).location().lat() == 0.0
                    &&  reply.elements(i).location().lon() == 0.0)){
                    std::cout << record.place() << "\"lat\":" << reply.elements(i).location().lat();
                    std::cout << record.place() << "\"lon\":" << reply.elements(i).location().lon();
                }
                if (reply.elements(i).location().time() != 0)
                    std::cout << record.place() << "\"time\":" << reply.elements(i).location().time();

                std::cout << '}';
            }
            std::cout << ']' << std::endl;
        }
        else
        {
            std::cerr << "ERROR: " << status.error_code() << std::endl;
        }
    }

    return 0;
}