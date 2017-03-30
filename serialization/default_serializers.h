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
 * dump simple structures into disk
 *
 * by Lu Wang
 */
#pragma once

#include <cstddef>
#include <iostream>

// default serializer
template <typename T, bool has_dump_load_method>
struct default_serializer
{};

/*
 * direct memory dump
 */
template <typename T>
struct default_serializer<T, false>
{
    using value_type = T;

    static constexpr 
    size_t size = sizeof(value_type);

    static inline 
    void dump(std::ostream & out, value_type const& v)
    {
        out.write(reinterpret_cast<const char *>(&v), size);
    }

    static inline
    void load(std::istream & in, value_type & v)
    {
        in.read(reinterpret_cast<char *>(&v), size);
    }
};


/*
 * call dump_to and load_from methods
 */
template <typename T>
struct default_serializer<T, true>
{
    using value_type = T;

    static constexpr 
    size_t size = T::serialization_size;

    static inline 
    void dump(std::ostream & out, value_type const& v)
    {
        v.dump_to(out);
    }

    static inline
    void load(std::istream & in, value_type & v)
    {
        v.load_from(in);
    }

    static inline
    value_type * load_p(std::istream & in)
    {
        value_type::load_p_from(in);
    }
};
