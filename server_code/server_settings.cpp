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
#include "server_settings.h"

#include <iostream>

s_settings g_server_settings;

bool server_parse_args(int argc, char* argv[])
{
    TCLAP::CmdLine cmd("Launches a long running process to host a sampling server", ' ', "0.2");

    TCLAP::ValueArg<int> arg_port("p", "port", "port number to listed for requests", false, 40053, "int", cmd);
    TCLAP::ValueArg<int> arg_garbage("g", "garbage_freq", "frequency to run the garbage collector (in seconds)", false, 30, "int", cmd);

    cmd.parse(argc, argv);

    g_server_settings.port_number = arg_port.getValue();
    g_server_settings.garbage_collection_frequency = arg_garbage.getValue();

    return true;
}