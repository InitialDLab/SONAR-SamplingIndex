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
//
// implementation of the exact query sentinel used to figure out what the exact aggregates are.
//

#include "streaming_aggragate_query.h"

#include <algorithm>
#include <limits>


bool streaming_aggragate_query::within(mongo_types::box3d region, mongo_types::point3d location)
{
    return (location.get<0>() >= region.min_corner().get<0>()) && (location.get<0>() <= region.max_corner().get<0>())
        && (location.get<1>() >= region.min_corner().get<1>()) && (location.get<1>() <= region.max_corner().get<1>())
        && (location.get<2>() >= region.min_corner().get<2>()) && (location.get<2>() <= region.max_corner().get<2>());
}


streaming_aggragate_query_time::streaming_aggragate_query_time(mongo_types::box3d query_region)
    : m_queryRegion(query_region),
    m_count{ 0 },
    m_total{ 0 },
    m_min{ std::numeric_limits<decltype(m_min)>::max() },
    m_max{ std::numeric_limits<decltype(m_max)>::min() },
    m_Mk{ 0.0 },
    m_Qk{ 0.0 }
{ }

void streaming_aggragate_query_time::submit(const mongo_types::sample_entry &element)
{
    // first check if this is an element of interest
    if (!within(m_queryRegion, element.get_point()))
        return;

    int time = element.timestamp;

    m_count++;
    m_total += time;
    m_min = std::min(m_min, time);
    m_max = std::max(m_max, time);

    // update variance must happen after the count has been updated
    update_variance(time);
}

// the information for how the variance is updated is found in a paper found
// at the following URL
// http://www.cs.berkeley.edu/~mhoemmen/cs194/Tutorials/variance.pdf
// calculating the variance this way should allow the variance to
// be calculated in a single pass with minimal rounding error.
void streaming_aggragate_query_time::update_variance(int value)
{
    if (m_count == 1)
    {
        m_Mk = value;
        m_Qk = 0;
    }
    else
    {
        m_Qk = m_Qk + (m_count - 1) * (value - m_Mk) * (value - m_Mk) / (m_count);
        m_Mk = m_Mk + (value - m_Mk) / m_count;
    }
}

double streaming_aggragate_query_time::get_avg()
{
    if (m_count == 0)
        return 0;
    else
        return 0.0 + m_total / m_count;
}

double streaming_aggragate_query_time::get_var()
{
    if (m_count == 0)
        return 0;
    else
     return m_Qk / m_count;
}

int streaming_aggragate_query_time::get_min()
{
    return m_min;
}

int streaming_aggragate_query_time::get_max()
{
    return m_max;
}

long streaming_aggragate_query_time::get_count()
{
    return m_count;
}

mongo_types::box3d streaming_aggragate_query_time::get_queryBox()
{
    return m_queryRegion;
}