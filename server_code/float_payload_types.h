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

struct float_payload_entry : public basic_entry
{
    float_payload_entry() { }

    float_payload_entry(float x, float y, int timestamp, std::string const & id, float payload)
        :basic_entry(x, y, timestamp, id)
        , payload(payload)
    { }

    float payload

    void dump_to (std::ostream & out) const {
        basic_entry::dump_to(out);

        serializer<float>::dump(out, payload);
    }

    void load_from (std::istream & in) {
        basic_entry::load_from(in);

        serializer<float>::load(in, payload);
    }

    static constexpr
    size_t serialization_size = 
        serializer<float>::size + 
        serializer<float>::size +
        serializer<int>::size +
        serializer<OID>::size +
        serializer<float>::size;
};

inline
std::ostream & operator << (std::ostream & out, float_payload_entry & e)
{
    // Call the << operator for basic_entry
    // add payload to the line
    out << static_cast<basic_entry>(e) << ' ' << e.payload

}

inline
std::istream & operator >> (std::istream & in, float_payload_entry & e)
{
    float x,y;
    long ts;
    std::string id;

    in >> x >> y >> ts >> id >> e.payload;
    e.loc.set<0>(x);
    e.loc.set<1>(y);
    e.oid = id;
    e.timestamp = ts / 1000;

    return in;
}

} // namespace mongo_types

template <>
struct has_dump_load_method<mongo_types::float_payload_entry>
{
    static constexpr bool value = true;
};
