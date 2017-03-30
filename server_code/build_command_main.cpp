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
// This is a helper program which is used to help Jun import data
// from mongodb to the sampling structure.  If given the requested
// information it will pull data out of mongodb and issue the command
// to build that structure in the sampling database

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

using grpc::Channel;
using grpc::ChannelArguments;
//using grpc::ChannelInterface;
using grpc::ClientContext;
using grpc::Status;
using namespace serverProto;
using namespace std;

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        std::cerr << "expected the data in the following order\n"
            << "[database name] [database collection] [sample server location]\n";
        return 0;
    }

    //std::string db_location = argv[1];
    std::string db_name = argv[1];
    std::string db_collection = argv[2];
    //std::string db_username = argv[4];
    //std::string db_password = argv[5];

    //std::string geo_name = argv[6];
    //std::string time_name = argv[7];

    std::string sample_location = argv[3];

    //std::string flip_str = argv[9];
    //bool flip_location = flip_str[0] == '1';

    //bool time_is_null = time_name == "null";

    // dump the database from mongodb
    
    //std::cout << "dumping data" << std::endl;
     //create a unique random file name to place the data temporarily
    //std::default_random_engine generator;
    //std::uniform_int_distribution<char> distribution('a', 'z');
    std::string filename1 = db_name + "." + db_collection + ".tmp";

    std::string filename2 = filename1 + '2';

    //// dump the mongodb file to our temp directory

    //std::string export_command = "mongoexport --host " + db_location
    //    + " --db " + db_name
    //    + " --collection " + db_collection
    //    + " --username " + db_username
    //    + " --password " + db_password
    //    + " --out " + filename1
    //    + " --type=csv "
    //    + " --fields " + "_id" + "," + time_name + "," + geo_name;
    //int ret_error = system(export_command.c_str());

    //// the data comes out a little dirty, so we need to clean it up to prepare for importing
    //// to the sample server

    //ifstream myinfile(filename1);
    //ofstream myoutfile(filename2);

    //struct tm base_tm = { 0 };

    //base_tm.tm_hour = 0;   base_tm.tm_min = 0; base_tm.tm_sec = 0;
    //base_tm.tm_year = 70;  base_tm.tm_mon = 0; base_tm.tm_mday = 1;

    //time_t base_time = mktime(&base_tm);

    //std::cout << "filtering data" << std::endl;

    //std::string line;
    //getline(myinfile, line); // the first line is the header
    //myoutfile.precision(12);
    //while (getline(myinfile, line))
    //{
    //    if (line.size() == 0)
    //        continue;

    //    char oid[32];
    //    char time_pre[32];
    //    float lat;
    //    float lon;
    //    tm time;



    //    long seconds;
    //    if (time_is_null)
    //    {
    //        sscanf(line.c_str(), "ObjectId(%24s),,\"[%f,%f]\"", oid, &lat, &lon);
    //        seconds = 0;
    //    }
    //    else
    //    {
    //        sscanf(line.c_str(), "ObjectId(%24s),%19s.000Z,\"[%f,%f]\"", oid, time_pre, &lat, &lon);
    //        strptime(time_pre, "%Y-%m-%dT%H:%M:%S", &time);

    //        seconds = difftime(mktime(&time), base_time);
    //    }

    //    if (flip_location) {
    //        myoutfile << oid << ',' << lon << ',' << lat << ',' << seconds << "\n";
    //    }
    //    else {
    //        myoutfile << oid << ',' << lat << ',' << lon << ',' << seconds << "\n";
    //    }
    //}

    //myoutfile.close();
    //myinfile.close();


    // call getcwd to get current working directory in linux
    std::string path;
    {
        char buffer[1024];
        getcwd(buffer, 1024);
        path = buffer;
        path += "/" + filename2;
    }

    std::cout << "database building" << std::endl;

    // issue the command to build the database in the database
    auto myChannel = grpc::CreateChannel(sample_location + ":40053", grpc::InsecureChannelCredentials());
    auto stub_ = SamplingDatabase::NewStub(myChannel);

    BuildRequest request;
    request.set_name(db_name + "." + db_collection);
    request.set_force(true);
    request.set_payload_type(SampleStructureType::NO_PAYLOAD);
    request.set_input_location(path);
    request.set_remove_input(true);

    BuildResponse reply;
    ClientContext context;

    Status status = stub_->BuildStructure(&context, request, &reply);
    
    //remove(filename1.c_str());
    //remove(filename2.c_str());

    if (status.ok())
    {
        return 0;
    }
    else
    {
        return 1;
    }
}