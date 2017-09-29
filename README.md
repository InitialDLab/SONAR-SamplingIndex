Requirements building

Sampling Index requires:
* gcc
* automake
* Boost
* grpcio-tools (python pip)

# Building

Use the following procedure to setup the project for the first time.
After building once, only the last steps need to be repeated for
code changes (Build Index Server for release mode or debug mode)

## Setup Build Enviornment

Clone and initialize submodule

    $ git clone https://github.com/InitialDLab/SONAR-SamplingIndex.git
    $ cd SONAR-SamplingIndex
    $ git submodule init
    $ git submodule update
    
Build stxxl

    $ cd third_party/stxxl-1.4.1
    $ mkdir build
    $ cd build
    $ cmake ../. -DCMAKE_BUILD_TYPE=Release -DBUILD_STATIC_LIBS=ON
    $ make
    $ cd ../../..
    
Build grpc

    $ cd third_party/grpc
    $ git submodule init
    $ git submodule update
    $ make
    $ cd ../..
   
Build glog

    $ cd third_party/glog
    $ ./configure
    $ make
    $ cd ../..
    
    
## Building Index Server

Build index server (release mode)

    $ cmake -DCMAKE_BUILD_TYPE=Release .
    $ make

Build index server (debug mode)

    $ cmake -DCMAKE_BUILD_TYPE=Debug .
    $ make
