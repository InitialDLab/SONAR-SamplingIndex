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
/*
 * dump structures into disk
 *
 * by Lu Wang
 */
#pragma once

#include "serialization/default_serializers.h"

template <typename T>
struct has_dump_load_method
{
    static constexpr bool value = false;
};

template <typename T>
struct serializer
    : public default_serializer<T, has_dump_load_method<T>::value>
{ };

template <typename T>
void dump_value(std::ostream & out, T const& t)
{
    serializer<T>::dump(out, t);
}

template <typename T>
void load_value(std::istream & in, T & t)
{
    serializer<T>::load(in, t);
}

template <typename ArrayType>
void dump_array(std::ostream & out, ArrayType const& at)
{
    dump_value(out, (uint16_t)at.size());
    for(auto const& v : at)
        dump_value(out, v);
}

template <typename ArrayType>
void load_array(std::istream & in, ArrayType & at)
{
    uint16_t s;
    load_value(in, s);
    at.resize(s);
    int id = 0;
    for (auto & v : at) {
        load_value(in, v);
        id++;
    }
}
