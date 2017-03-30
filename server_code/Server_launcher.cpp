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
#include <thread>
#include <chrono>

#include "Server_launcher.h"
#include "server_settings.h"


Server_launcher::Server_launcher(const std::string &server_address)
    : mp_service(new Sampling_server(true, true))
    , mp_server(nullptr)
    , m_sleepTime(g_server_settings.garbage_collection_frequency)
{
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&*mp_service);
    //builder.RegisterAsyncService(&*mp_service);

    mp_server = builder.BuildAndStart();
    LOG(INFO) << "Started the server listening on " << server_address;
}


Server_launcher::~Server_launcher()
{
}


void Server_launcher::Blocking_wait()
{
    // periodically clean up resources and sleep between calls for 2 seconds
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(m_sleepTime));

        mp_service->garbageCollectQueries();
    }
}