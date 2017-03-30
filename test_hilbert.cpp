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
#include <algorithm>

#include <boost/geometry/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>

#include "hilbert/hilbert.h"

namespace bg = boost::geometry;
namespace bm = ::boost::mpl;
namespace bmp = ::boost::mpl::placeholders;

void test_2d()
{
    hilbert::HilbertValueComputer<unsigned int, 2, 8> hvc;

    /*
    using point_t = bg::model::point<int, 2, bg::cs::cartesian>;
    std::cerr << "here" << std::endl;
    int x,y;
    while(std::cin >> x >> y)
    {
        auto hv = hvc(point_t(x,y));
        for(auto & p : hv.values)
            std::cout << p << ' ';
        std::cout << std::endl;
    }
    return 0;
    */

    using point_t = bg::model::point<int, 2, bg::cs::cartesian>;
    std::vector<point_t> v;
    int DIM_SIZE = (1LL << 8);
    for(int i = 0; i < DIM_SIZE; ++i)
    {
        for(int j = 0; j < DIM_SIZE; ++j)
        {
            v.push_back(point_t(i,j));
        }
    }

    std::sort(v.begin(), v.end(), [&](point_t const& p1, point_t const& p2)->bool{
        return hvc(p1) < hvc(p2);
    });

    /*
    for(auto & p : v)
    {
        auto hv = hvc(p);
        for(auto & i : hv.values)
            std::cerr << i << ' ';
        std::cerr << std::endl;
    }
    */

    for(auto iter = v.begin(); iter != --v.end(); )
    {
        std::cout << iter->get<0>() << ' ' << iter->get<1>() << std::endl;
        ++iter;
        std::cout << iter->get<0>() << ' ' << iter->get<1>() << std::endl;
        std::cout << std::endl;
    }
}
void test_3d()
{
    std::cerr << "building lookup" << std::endl;
    hilbert::HilbertValueComputer<unsigned int, 3, 4> hvc;


    std::cerr << "generating data" << std::endl;
    using point_t = bg::model::point<int, 3, bg::cs::cartesian>;
    std::vector<point_t> v;
    int DIM_SIZE = (1LL << 4);
    for(int i = 0; i < DIM_SIZE; ++i)
    {
        for(int j = 0; j < DIM_SIZE; ++j)
        {
            for(int k = 0; k < DIM_SIZE; ++k)
            {
                v.push_back(point_t(i,j,k));
            }
        }
    }

    std::cerr << "sorting data" << std::endl;
    std::sort(v.begin(), v.end(), [&](point_t const& p1, point_t const& p2)->bool{
        return hvc(p1) < hvc(p2);
    });

    /*
    for(auto & p : v)
    {
        auto hv = hvc(p);
        for(auto & i : hv.values)
            std::cerr << i << ' ';
        std::cerr << std::endl;
    }
    */

    std::cerr << "saving" << std::endl;
    for(auto iter = v.begin(); iter != --v.end(); )
    {
        std::cout << iter->get<0>() << ' ' << iter->get<1>() << ' ' << iter->get<2>() << std::endl;
        ++iter;
        std::cout << iter->get<0>() << ' ' << iter->get<1>() << ' ' << iter->get<2>() << std::endl;
        std::cout << std::endl;
    }

    std::cerr << "done" << std::endl;
}

struct printer
{
    template <typename V>
    void operator () (V)
    {
        using T0 = typename bm::at_c<V, 0>::type;
        std::cout << T0::dim << ' ' << T0::x << ' ' << T0::y << ' ' << T0::z << ' ';
        using T = typename bm::at_c<V, 1>::type;
        std::cout << T::x << ' ' << T::y << ' ' << T::z << std::endl;
    }
};

int main()
{
    //test_2d();
    test_3d();
    //bm::for_each<hilbert::three_d::HilbertRecursionBase<0, false, false, false>>(printer());
    return 0;
}
