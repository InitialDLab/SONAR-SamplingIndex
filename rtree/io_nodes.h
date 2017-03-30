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
 * node types for io_rtree
 *
 * by Lu Wang
 */
#pragma once

namespace rtree {

template <typename, typename, typename, typename>
struct io_node;

template <typename Box, typename Key, typename Value, typename SampleValue>
struct io_visitor
{
    using node_type = io_node<Box, Key, Value, SampleValue>;
    using internal_node_type = io_internal_node<Box, Key, Value, SampleValue>;
    using leaf_node_type = io_leaf_node<Box, Key, Value, SampleValue>;

    io_visitor() {}
    virtual ~io_visitor() {}
    virtual void apply(internal_node_type &) = 0;
    virtual void apply(leaf_node_type &) = 0;
};

template<typename Box, typename Key, typename Value, typename SampleValue>
struct io_node
{

    virtual ~io_node() {}
    using visitor_type = io_visitor<Box, Key, Value, SampleValue>;
    virtual void apply_visitor(visitor_type &) = 0;
};



} // namespace rtree
