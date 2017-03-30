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
#include <fstream>
#include <iostream>
#include <string>
#include <random>
#include <memory>

#include <cstdlib>

#include <boost/timer/timer.hpp>

void clear_cache()
{
    int i = system("/uusoc/scratch/magpie18/robertc/sample/clearCache");
    if(i != 0)
        std::cerr << "WARNING: clear_cache() failed!" << std::endl;
}

int main()
{
    std::string FN = "./built/geo_shuffle.shf";
    constexpr size_t BLOCK_SIZE = 8192;

    constexpr int REPEAT = 30;
    

    size_t file_size = std::ifstream(FN, std::ifstream::ate|std::ifstream::binary).tellg();
    size_t block_count = file_size / BLOCK_SIZE;

    std::cerr << "block count " << block_count << std::endl;


    if(0)
    {
        std::unique_ptr<std::ifstream> fin;
        char buffer[BLOCK_SIZE];
        clear_cache();
        {
            boost::timer::auto_cpu_timer _;
            fin.reset(new std::ifstream(FN, std::ifstream::binary));
            fin->rdbuf()->pubsetbuf(nullptr, 0);
            std::cerr << fin->rdbuf()->in_avail() << std::endl;
        }

        fin->seekg(8192*1024*50);

        for(int i = 0; i < 100; ++i)
        {
            boost::timer::auto_cpu_timer _;
            fin->read(buffer, BLOCK_SIZE);
            std::cerr << i << ' ' << fin->rdbuf()->in_avail() << std::endl;
        }

    }

    {
        std::cerr << "random" << std::endl;
        clear_cache();
        std::ifstream fin(FN, std::ifstream::binary);
        char buffer[BLOCK_SIZE];

        std::default_random_engine rng;
        std::uniform_int_distribution<int> dist(0,(int)block_count-1);

        boost::timer::auto_cpu_timer _;
        boost::timer::auto_cpu_timer __;
        __.stop();
        for(int i = 0; i < REPEAT/10; ++i)
        {
            int j = dist(rng);
            fin.seekg(j * BLOCK_SIZE);
            for(int k = 0; k < 10; ++k)
            {
                boost::timer::cpu_timer timer;
                __.resume();
                fin.read(buffer, BLOCK_SIZE);
                __.stop();
                std::cout << i << ' ' << k << ' ' << timer.elapsed().wall/1000000.0 << std::endl;
            }
        }
        __.report();
    }

    if(0)
    {
        std::cerr << "sequential" << std::endl;
        clear_cache();
        std::ifstream fin(FN, std::ifstream::binary);
        char buffer[BLOCK_SIZE];
        boost::timer::auto_cpu_timer _;
        boost::timer::auto_cpu_timer __;
        __.stop();
        for(int i = 0; i < REPEAT; ++i)
        {
            boost::timer::cpu_timer timer;
            __.resume();
            fin.read(buffer, BLOCK_SIZE);
            __.stop();
            std::cout << i << ' ' << timer.elapsed().wall/1000000.0 << std::endl;
        }
        __.report();
    }


    return 0;
}
