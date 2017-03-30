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
#include <string>
#include <random>

#include <cstdlib>

#include <boost/timer/timer.hpp>
#include <boost/random/taus88.hpp>
#include <boost/random/uniform_smallint.hpp>

#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>

constexpr int REPEAT = 1000000;

template <typename T>
void test(std::string const& name)
{
    T t;
    boost::timer::auto_cpu_timer _;
    float s = 0;
    for(int i = 0; i < REPEAT; ++i)
    {
        s += t.run();
    }
    std::cerr << name << ' ' << s << std::endl;
            std::cerr << std::endl;
}

struct Rand
{
    int run() { rand()%10000; }
};

struct Drand48
{
    int run() { (size_t)(drand48()*10000); }
};

struct GSLReal
{
    GSLReal() { _gsl_rng = gsl_rng_alloc (gsl_rng_taus); }

    gsl_rng *_gsl_rng = nullptr;
    int run() {
        return gsl_ran_flat(_gsl_rng, 0, 10000);
    }
};

template<typename RNG, typename DIST>
struct Dist
{
    RNG rng;
    int run() { 
        DIST dist(0, 10000);
        dist(rng);
    }
};

template<typename RNG, typename DIST>
struct Dist_1 
{
    RNG rng;
    DIST dist{0, 10000};
    int run() { 
        dist(rng);
    }
};

template<typename RNG, typename DIST>
struct Dist_2 
{
    int run() { 
        RNG rng;
        DIST dist{0, 10000};
        dist(rng);
    }
};

void test_uni()
{
    test<Rand>("rand");
    test<Drand48>("drand48");

    using std_default = std::default_random_engine;
    using taus88 = boost::random::taus88;

    using std_uni = std::uniform_int_distribution<int>;
    using smallint = boost::random::uniform_smallint<int>;

    test<Dist<std_default, std_uni>>("uni_default");
    test<Dist_1<std_default, std_uni>>("uni_default_1");
    test<Dist<taus88, std_uni>>("uni_taus88");
    test<Dist_1<taus88, std_uni>>("uni_taus88_1");

    test<Dist<std_default, smallint>>("smallint_default");
    test<Dist_1<std_default, smallint>>("smallint_default_1");
    test<Dist<taus88, smallint>>("smallint_taus88");
    test<Dist_1<taus88, smallint>>("smallint_taus88_1");
}

template<typename RNG, typename DIST>
struct RealDist
{
    RNG rng;
    float run() { 
        DIST dist(0, 1);
        return dist(rng);
    }
};

template<typename RNG, typename DIST>
struct RealDist_1 
{
    RNG rng;
    DIST dist{0, 1};
    float run() { 
        return dist(rng);
    }
};

template<typename RNG, typename DIST>
struct RealDist_2 
{
    float run() { 
        RNG rng;
        DIST dist{0, 1};
        return dist(rng);
    }
};

void test_real()
{
    using std_default = std::default_random_engine;
    using taus88 = boost::random::taus88;

    using std_real = std::uniform_real_distribution<float>;

    test<Rand>("rand");
    test<Drand48>("drand48");
    test<GSLReal>("GSL_real");

    test<RealDist<std_default, std_real>>("real_default");
    test<RealDist_1<std_default, std_real>>("real_default_1");
    test<RealDist<taus88, std_real>>("real_taus88");
    test<RealDist_1<taus88, std_real>>("real_taus88_1");
}

void test_binomial()
{
    using taus88 = boost::random::taus88;
    taus88 rng;
    for(size_t total = 1; total < 100; total += 10)
    {
        //for(double prob = 0.0; prob < 1; prob += 0.2)
        double prob = 0.001;
        {
            size_t s = 0;
            {
                boost::timer::auto_cpu_timer _;
                for(int i = 0; i < REPEAT; ++i)
                {
                    s += std::binomial_distribution<size_t>(total, prob)(rng);
                }
            }
            std::cerr << "binomial " << total << ' ' << prob << ' ' << s << std::endl;
            std::cerr << std::endl;
        }
        {
            size_t s = 0;
            {
                boost::timer::auto_cpu_timer _;
                std::uniform_real_distribution<float> coin_dist(0,1);
                for(int i = 0; i < REPEAT; ++i)
                {
                    for(size_t j = 0; j < total; ++j)
                        if(coin_dist(rng) < prob)
                            ++s;
                }
            }
            std::cerr << "coin " <<  total << ' ' << prob << ' ' << s << std::endl;
            std::cerr << std::endl;
        }
    }
}

int main()
{
//    test_uni();
    test_real();
//    test_binomial();
    return 0;
}
