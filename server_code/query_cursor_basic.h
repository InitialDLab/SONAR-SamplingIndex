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
#pragma once

#include <memory>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/error_of.hpp>
#include <boost/accumulators/statistics/error_of_mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

#include "basic_types.h"
#include "RStree_basic.h"
#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/query_cursor.h"

typedef rtree::sample_query_cursor<boost::geometry::model::box<boost::geometry::model::point<float, 3ul, boost::geometry::cs::cartesian> >, boost::geometry::model::box<boost::geometry::model::point<float, 3ul, boost::geometry::cs::cartesian> >, std::array<unsigned int, 4ul>, server_types::basic_entry, server_types::basic_entry> basicCursor_t;
typedef boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::mean, boost::accumulators::tag::variance, boost::accumulators::tag::max, boost::accumulators::tag::min, boost::accumulators::tag::error_of<boost::accumulators::tag::mean>, boost::accumulators::tag::count > > StreamingStatistics_t;
typedef boost::accumulators::accumulator_set<long, boost::accumulators::stats<boost::accumulators::tag::min, boost::accumulators::tag::max> > int_minMax_accumulator_t;


class query_cursor_basic : public query_cursor
{
public:
    // ttl in seconds
    query_cursor_basic(std::shared_ptr<basic_rtree> source, const serverProto::box& query_region, bool returnOID, bool returnTime, bool returnLocation, int ttl = 60);

    virtual ~query_cursor_basic();

    // get the settings requested for the cursor 
    // (what to return as part of the returned elements)
    bool returning_OID();
    bool returning_location();
    bool returning_payload();
    bool returning_time();

    // get the ttl for the object
    int get_ttl();

    // return true if this cursor is expired (lived to the end of its ttl) and is ready to be cleared up
    bool is_expired();

    // get the counted number of elements in the requested range for this query
    long get_total_elements_in_query_range();

    long get_elements_analyzed_count();

    // return the region this query is going to sample from
    serverProto::box get_query_region();

    // returns the box3d version of the query region (this is lossy because
    // time is converted from an int type to a float type.
    server_types::box3d get_query_box3d() const;

    static server_types::box3d get_query_box3d(const serverProto::box& q_region);


    // perform the query and return a query response with the requested data
    void perform_query(int count, serverProto::QueryResponse&);

private:
    using query_cursor_t = rtree::sample_query_cursor < boost::geometry::model::box<boost::geometry::model::point<float, 3ul, boost::geometry::cs::cartesian> >, boost::geometry::model::box<boost::geometry::model::point<float, 3ul, boost::geometry::cs::cartesian> >, std::array<unsigned int, 4ul>, server_types::basic_entry, server_types::basic_entry > ;

    query_cursor_t m_cursor;
    std::shared_ptr<basic_rtree> m_source;


    void get_stats(const StreamingStatistics_t& stats, serverProto::ElementStatistics& toRet) const;

    serverProto::box m_queryRegion;

    bool m_returning_OID;
    bool m_returning_location;
    bool m_returning_time;

    long m_elements_in_range;
    long m_elements_analyzed;

    int m_ttl;

    time_t last_used_time;

    StreamingStatistics_t    m_latTotal;
    StreamingStatistics_t    m_lonTotal;
    StreamingStatistics_t    m_timeTotal;
    int_minMax_accumulator_t m_timeMinMax;

};
