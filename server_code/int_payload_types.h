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

#include <iostream>
#include <iomanip>
#include <ctime>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/functional/hash.hpp>

#include "../serialization/default_serializers.h"
#include "../serialization/serializer.h"

#include "basic_types.h"

namespace server_types {
namespace bg = boost::geometry;

using point2d = bg::model::point<float, 2, bg::cs::cartesian>;
using point3d = bg::model::point<float, 3, bg::cs::cartesian>;
using box2d = bg::model::box<point2d>;
using box3d = bg::model::box<point3d>;

struct int_payload_entry : public basic_entry
{
    int_payload_entry() { }

    int_payload_entry(float x, float y, int timestamp, std::string const & id, int payload)
        :loc(x,y)
        ,timestamp(timestamp)
        ,oid(id)
    { }

    int payload;

    void dump_to (std::ostream & out) const {
        basic_entry::dump_to(out);

        serializer<int>::dump(out, payload);
    }

    void load_from (std::istream & in) {
        basic_entry::load_from(in);

        serializer<int>::load(in, payload);
    }

    static constexpr
    size_t serialization_size = 
        serializer<float>::size + 
        serializer<float>::size +
        serializer<int>::size +
        serializer<OID>::size +
        serializer<int>::size;
};

inline
std::ostream & operator << (std::ostream & out, entry & e)
{
    // convert time
    // std::put_time not implemented in gcc...
    std::time_t t = e.timestamp;
    tm * ti;
    char buffer[100];
    strftime(buffer, 100, "%c %Z", std::gmtime(&t));

    out << bg::wkt(e.loc) << ' ' 
        << e.oid << ' ' 
        << e.timestamp << ' ' 
        << buffer;
    return out;
}

inline
std::istream & operator >> (std::istream & in, entry & e)
{
    float x,y;
    long ts;
    std::string id;

    in >> x >> y >> ts >> id;
    e.loc.set<0>(x);
    e.loc.set<1>(y);
    e.oid = id;
    e.timestamp = ts / 1000;

    return in;
}

struct sample_entry 
{
    sample_entry() { }
    sample_entry(entry const& e)
        : loc(e.loc)
        , timestamp(e.timestamp)
        , oid(e.oid)
    { }

    point3d get_point(void) const 
    {
        return point3d(loc.get<0>(), loc.get<1>(), timestamp);
    }

    using hilbert_point_type = bg::model::point<unsigned int, 3, bg::cs::cartesian>;
    hilbert_point_type convert_for_hilbert(void) const {
        assert(timestamp >= 0);
        return hilbert_point_type(
                   // lat: -90 ~ +90
                   (unsigned int)((loc.get<0>() + 90.0) * 10000000.0),
                   // lon: -180 ~ +180
                   (unsigned int)((loc.get<1>() + 180.0) * 10000000.0),
                   // we should have no entry prior to 1970
                   // so no worry about negative values
                   (unsigned int)timestamp
               );
    }

    point2d loc; // lat, lon
    int timestamp;
    OID oid;
};

inline
bool operator == (entry const& e, sample_entry const& se) { return e.oid == se.oid; }
inline
bool operator == (sample_entry const& se, entry const& e) { return e.oid == se.oid; }
inline
bool operator == (sample_entry const& se1, sample_entry const& se2) { return se1.oid == se2.oid; }

inline
std::ostream & operator << (std::ostream & out, sample_entry & e)
{
    // convert time
    // std::put_time not implemented in gcc...
    std::time_t t = e.timestamp;
    tm * ti;
    char buffer[100];
    strftime(buffer, 100, "%c %Z", std::gmtime(&t));

    out << bg::wkt(e.loc) << ' ' 
        << e.timestamp << ' ' 
        << buffer;
    return out;
}

} // namespace mongo_types

template <>
struct has_dump_load_method<mongo_types::entry>
{
    static constexpr bool value = true;
};

namespace std {
    template <>
    struct hash<mongo_types::sample_entry>
    {
        std::size_t operator () (mongo_types::sample_entry const& e) const {
            return boost::hash_range(e.oid.id.begin(), e.oid.id.end());
        }
    };
}
