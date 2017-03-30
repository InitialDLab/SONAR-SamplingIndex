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

#include "serialization/default_serializers.h"
#include "serialization/serializer.h"

namespace mongo_types {
namespace bg = boost::geometry;

// ObjectID in mongodb
struct OID
{
    OID() {}

    OID (std::string const& idstr) {
        from_string(idstr);
    }

    OID & operator = (std::string const& idstr) {
        from_string(idstr);
        return *this;
    }

    bool operator == (OID const& oid) const {
        return id == oid.id;
    }

    void from_string(std::string const& idstr) {
        assert(idstr.size() == oid_len * 2);
        auto iter = id.begin();
        for(size_t i = 0; i < oid_len * 2; i += 2)
        {
            *iter = (char_map[(unsigned char)idstr[i]] << 4) + char_map[(unsigned char)idstr[i+1]];
            ++iter;
        }
    }

    static constexpr size_t oid_len = 12;
    std::array<unsigned char, oid_len> id;
    static const unsigned char char_map[256]; 
};

inline
std::ostream & operator << (std::ostream & out, OID const& oid)
{
    std::ios old_state(nullptr);
    old_state.copyfmt(out);
    out << std::hex;
    for (auto c : oid.id)
    {
        if ((int)c > 0xf)
            out << (int)c;
        else if ((int)c > 0x0)
            out << '0' << (int)c;
        else
            out << "00";
    }
    out.copyfmt(old_state);
    return out;
}

using point2d = bg::model::point<float, 2, bg::cs::cartesian>;
using point3d = bg::model::point<float, 3, bg::cs::cartesian>;
using box2d = bg::model::box<point2d>;
using box3d = bg::model::box<point3d>;

struct entry
{
    entry () 
        :loc(0,0)
        , timestamp(0)
        , oid()
    { }

    entry(float x, float y, int timestamp, std::string const & id)
        :loc(x,y)
        ,timestamp(timestamp)
        ,oid(id)
    { }

    point2d loc; // lat, lon
    int timestamp;
    OID oid;

    bool operator == (entry const& e) const {
        return oid == e.oid;
    }

    /*
     * get the point to calculate the Hilbert value from
     * basically we need a 3d integral point from (0,0,0) to (max,max,max)
     */
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

    void dump_to (std::ostream & out) const {
        out.write(reinterpret_cast<const char *>(this), sizeof(*this));

        //out.write(reinterpret_cast<const char *>(&(loc.get<0>())), sizeof(float));
        //out.write(reinterpret_cast<const char *>(&(loc.get<1>())), sizeof(float));
        //out.write(reinterpret_cast<const char *>(&timestamp), sizeof(int));
        //out.write(reinterpret_cast<const char *>(&oid), sizeof(OID));

        //serializer<float>::dump(out, loc.get<0>());
        //serializer<float>::dump(out, loc.get<1>());
        //serializer<int>::dump(out, timestamp);
        //serializer<OID>::dump(out, oid);
    }

    void load_from (std::istream & in) {
        in.read(reinterpret_cast<char *>(this), sizeof(*this));

        //float i;
        //serializer<float>::load(in, i);
        //loc.set<0>(i);
        //serializer<float>::load(in, i);
        //loc.set<1>(i);
        //serializer<int>::load(in, timestamp);
        //serializer<OID>::load(in, oid);
    }

    static constexpr
    size_t serialization_size = 
        serializer<float>::size + 
        serializer<float>::size +
        serializer<int>::size +
        serializer<OID>::size;

    point3d get_point(void) const {
        return point3d(loc.get<0>(), loc.get<1>(), timestamp);
    }
};

inline
std::ostream & operator << (std::ostream & out, entry & e)
{
    // convert time
    // std::put_time not implemented in gcc...
    std::time_t t = e.timestamp;
    //tm * ti;
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
    sample_entry()
        : loc()
        , timestamp(0)
        , oid()
    { }
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
    //tm * ti;
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
