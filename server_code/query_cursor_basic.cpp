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
#include <time.h>
#include <memory>
#include <vector>

#include <glog/logging.h>

#include "query_cursor_basic.h"

#include "server_code/protobuf/sampling_api.pb.h"


query_cursor_basic::query_cursor_basic(std::shared_ptr<basic_rtree> source
                                     , const serverProto::box& query_region
                                     , bool returnOID
                                     , bool returnTime
                                     , bool returnLocation
                                     , int ttl)
                                     : m_queryRegion(query_region)
                                     , m_source(source)
                                     , m_returning_OID(returnOID)
                                     , m_returning_location(returnLocation)
                                     , m_returning_time(returnTime)
                                     , m_elements_analyzed(0)
                                     , m_ttl(ttl)
                                     , m_cursor(source->sample_query(get_query_box3d(query_region)))
                                       
{
    // we will set some max ttl.  It only can be up to 5 minutes
    m_ttl = std::min(60*5, m_ttl);

    // setup query cursor

    // count the number of elements in the region (or estimate count if that is the only thing available to us)
    // we would like to get an exact count.  We don't have good guarantees for approximate counts
    m_elements_in_range = source->naive_sample_query(get_query_box3d()).get_count();

    LOG(INFO) << "for new query, we think there are " << m_elements_in_range << " elements in the range";

    // setup accumulators and accumulator list
    // accumulators are already setup?

    // set last used time to now
    time(&last_used_time);
}

query_cursor_basic::~query_cursor_basic()
{
}

bool query_cursor_basic::returning_OID()
{
    return m_returning_OID;
}

bool query_cursor_basic::returning_location()
{
    return m_returning_location;
}

bool query_cursor_basic::returning_payload()
{
    return false;
}

bool query_cursor_basic::returning_time()
{
    return m_returning_time;
}

int query_cursor_basic::get_ttl()
{
    return m_ttl;
}

bool query_cursor_basic::is_expired()
{
    double seconds_since_use = difftime(time(NULL), last_used_time);

    return seconds_since_use > m_ttl;
}

long query_cursor_basic::get_total_elements_in_query_range()
{
    return m_elements_in_range;
}

long query_cursor_basic::get_elements_analyzed_count()
{
    return m_elements_analyzed;
}

serverProto::box query_cursor_basic::get_query_region()
{
    return m_queryRegion;
}

server_types::box3d query_cursor_basic::get_query_box3d() const
{
    server_types::box3d box_builder;
    box_builder.min_corner().set<0>(m_queryRegion.min_point().lat() );
    box_builder.min_corner().set<1>(m_queryRegion.min_point().lon() );
    box_builder.min_corner().set<2>(m_queryRegion.min_point().time());
    box_builder.max_corner().set<0>(m_queryRegion.max_point().lat() );
    box_builder.max_corner().set<1>(m_queryRegion.max_point().lon() );
    box_builder.max_corner().set<2>(m_queryRegion.max_point().time());

    return box_builder;
}

server_types::box3d query_cursor_basic::get_query_box3d(const serverProto::box& q_region)
{
    server_types::box3d box_builder;
    box_builder.min_corner().set<0>(q_region.min_point().lat());
    box_builder.min_corner().set<1>(q_region.min_point().lon());
    box_builder.min_corner().set<2>(q_region.min_point().time());
    box_builder.max_corner().set<0>(q_region.max_point().lat());
    box_builder.max_corner().set<1>(q_region.max_point().lon());
    box_builder.max_corner().set<2>(q_region.max_point().time());

    return box_builder;
}

void query_cursor_basic::get_stats(const StreamingStatistics_t& stats, serverProto::ElementStatistics &toRet) const
{
    using namespace boost::accumulators;

    //serverProto::ElementStatistics toRet;

    toRet.set_type(serverProto::StatisicalType::FLOAT_TYPE);
    toRet.set_sample_size(count(stats));
    toRet.set_total_count(this->m_elements_in_range);
    toRet.set_mean(mean(stats));
    // TODO: SET CONFIDENCE REGION
    // TODO: SET CONFICENCE LEVEL
    toRet.set_stdev(variance(stats));

    toRet.set_float_min(min(stats));
    toRet.set_float_max(max(stats));

    //return toRet;
}

void query_cursor_basic::perform_query(int count, serverProto::QueryResponse& toReturn)
{
    // I assume the query will be short, so I only set it at the beginning of the query.
    time(&last_used_time);

    // setup accumulators running for this particular query
    StreamingStatistics_t    latRun;
    StreamingStatistics_t    lonRun;
    StreamingStatistics_t    timeRun;
    int_minMax_accumulator_t timeMinMaxRun;

    toReturn.Clear();

    // query the data structure, filling the accumulators with the new data
    // as we are filling the accumulators, also fill the QueryResponse.
    // NOTE: if needed, we don't have to do it this way.  We can implemented
    // a back inserter function for QueryResponse which will also fill accumulators.
    // doing this will allow only a single scan of the data.  currently we are
    // doing a double scan.
    std::vector<server_types::basic_entry> query_buffer;
    if (this->m_elements_in_range == 0)
    {
        // do nothing if there is not anything to query
    }
    else if (this->m_elements_in_range <= count) {
        m_source->range_report(get_query_box3d(), back_inserter(query_buffer));
    }
    else{
        m_cursor.get_samples(count, back_inserter(query_buffer));
    }

    // update the total query count
    m_elements_analyzed += query_buffer.size();

    for (size_t i = 0; i < query_buffer.size(); i++)
    {
        // add to accumulators
        latRun(query_buffer[i].loc.lat);
        m_latTotal(query_buffer[i].loc.lat);

        lonRun(query_buffer[i].loc.lon);
        m_lonTotal(query_buffer[i].loc.lon);

        timeRun(query_buffer[i].timestamp);
        m_timeTotal(query_buffer[i].timestamp);

        timeMinMaxRun(query_buffer[i].timestamp);
        m_timeMinMax(query_buffer[i].timestamp);

        // insert the element in to queryResponse
        serverProto::element* elm = toReturn.add_elements();
        if (m_returning_OID)
            elm->set_oid(query_buffer[i].oid.to_string());
        if (m_returning_location)
        {
            auto sloc = elm->mutable_location();
            sloc->set_lat(query_buffer[i].loc.lat);
            sloc->set_lon(query_buffer[i].loc.lon);
        }
        if (m_returning_time)
        {
            auto sloc = elm->mutable_location();
            sloc->set_time(query_buffer[i].timestamp);
        }
    }

    // set the accumulator data.
    using namespace boost::accumulators;
    get_stats(latRun,      *(toReturn.mutable_lat_last()));
    get_stats(lonRun,      *(toReturn.mutable_lon_last()));
    get_stats(timeRun,     *(toReturn.mutable_time_last()));
    get_stats(m_latTotal,  *(toReturn.mutable_lat_total()));
    get_stats(m_lonTotal,  *(toReturn.mutable_lon_total()));
    get_stats(m_timeTotal, *(toReturn.mutable_time_total()));

    toReturn.set_sample_count_last(query_buffer.size());
    toReturn.set_sample_count_total(m_elements_analyzed);
}