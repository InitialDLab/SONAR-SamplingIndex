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
#include <type_traits>
#include <tuple>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/functional/hash.hpp>

#include "../serialization/default_serializers.h"
#include "../serialization/serializer.h"

#include "../mongo_types.h"

namespace server_types {
namespace bg = boost::geometry;

struct s2dlocation
{
    float lat; // index 0
    float lon; // index 1
};

// ObjectID in mongodb
struct OID
{
    bool operator == (OID const& oid) const {
        return id == oid.id;
    }

    void from_string(std::string const& idstr) {
        assert(idstr.size() == oid_len * 2);
        auto iter = id.begin();
        for(size_t i = 0; i < oid_len * 2; i += 2)
        {
            *iter = (mongo_types::OID::char_map[(unsigned char)idstr[i]] << 4) + mongo_types::OID::char_map[(unsigned char)idstr[i+1]];
            ++iter;
        }
    }

    std::string to_string()
    {
        char small_buffer[3];

        std::string to_ret = "";
        for (auto c : id)
        {
            snprintf(small_buffer, 3, "%02x", c);

            to_ret += small_buffer;
        }

        return to_ret;
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



struct basic_entry
{
    void set_data(float lat, float lon, int timestamp_arg, std::string const & id)
    {
        loc.lat = lat;
        loc.lon = lon;
        timestamp = timestamp_arg;
        oid.from_string(id);
    }

    static basic_entry build(float lat, float lon, int timestamp_arg, std::string const & id)
    {
        basic_entry temp;
        temp.set_data(lat, lon, timestamp_arg, id);
        return temp;
    }

    s2dlocation loc;
    OID oid;
    int timestamp;

    bool operator == (basic_entry const& e) const {
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
                   (unsigned int)((loc.lat + 90.0) * 10000000.0),
                   // lon: -180 ~ +180
                   (unsigned int)((loc.lon + 180.0) * 10000000.0),
                   // we should have no entry prior to 1970
                   // so no worry about negative values
                   (unsigned int)timestamp
               );
    }

    /*virtual*/ void dump_to (std::ostream & out) const {
        serializer<float>::dump(out, loc.lat);
        serializer<float>::dump(out, loc.lon);
        serializer<int>::dump(out, timestamp);
        serializer<OID>::dump(out, oid);
    }

    /*virtual*/ void load_from (std::istream & in) {
        float i;
        serializer<float>::load(in, i);
        loc.lat = i;
        serializer<float>::load(in, i);
        loc.lon = i;
        serializer<int>::load(in, timestamp);
        serializer<OID>::load(in, oid);
    }

    static constexpr
    size_t serialization_size = 
        serializer<float>::size + 
        serializer<float>::size +
        serializer<int>::size +
        serializer<OID>::size;

    point3d get_point(void) const {
        return point3d(loc.lat, loc.lon, timestamp);
    }
};



//inline
//std::ostream & operator << (std::ostream & out, basic_entry & e)
//{
//    // convert time
//    // std::put_time not implemented in gcc...
//    std::time_t t = e.timestamp;
//    tm * ti;
//    char buffer[100];
//    strftime(buffer, 100, "%c %Z", std::gmtime(&t));
//
//    out << bg::wkt(e.loc) << ' ' 
//        << e.oid << ' ' 
//        << e.timestamp << ' ' 
//        << buffer;
//    return out;
//}

inline
std::istream & operator >> (std::istream & in, basic_entry & e)
{
    float x,y;
    long ts;
    std::string id;

    in >> x >> y >> ts >> id;
    e.loc.lat = x;
    e.loc.lon = y;
    e.oid.from_string(id);
    e.timestamp = ts / 1000;

    return in;
}

// to support disk based sorting using stxxl library, the data types must be POD data types
// we do a built in check here to make sure they are valid.
// TODO: we should probably put this code into the rtree index?
static_assert(std::is_pod<s2dlocation>::value, "location data structure must be POD");
static_assert(std::is_pod<OID>::value, "Index value must be a POD type to be correctly sorted on disk");
//static_assert(std::is_pod<point2d>::value, "point2d data type must be a POD type to be correctly sorted on disk");
static_assert(std::is_pod<basic_entry>::value, "Internal value must be a POD type to be correctly sorted on disk");

} // namespace basic_types

template <>
struct has_dump_load_method<server_types::basic_entry>
{
    static constexpr bool value = true;
};
