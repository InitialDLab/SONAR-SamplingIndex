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
 * Calculate hilbert value
 *
 * by Lu Wang
 */

#ifndef HILBERT_H__
#define HILBERT_H__

#include <type_traits>
#include <limits>

#include <boost/integer.hpp>
#include <boost/integer/integer_mask.hpp>

#include <boost/mpl/at.hpp>
#include <boost/mpl/bind.hpp>
#include <boost/mpl/for_each.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/pair.hpp>
#include <boost/mpl/placeholders.hpp>
#include <boost/mpl/range_c.hpp>
#include <boost/mpl/transform.hpp>
#include <boost/mpl/vector.hpp>
#include <boost/mpl/vector_c.hpp>
#include <boost/mpl/zip_view.hpp>

#include <boost/geometry/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>

#include <array>

namespace hilbert {

namespace bm = ::boost::mpl;
namespace bmp = ::boost::mpl::placeholders;

template<typename Value, size_t S>
using HilbertValue = std::array<Value, S>;


#include "hilbert/2d.h"
#include "hilbert/3d.h"


template<typename Value, size_t Dim, size_t LookupTableWidth>
struct HilbertValueComputer { };


template<typename Value, size_t LookupTableWidth>
struct HilbertValueComputer<Value, 2, LookupTableWidth>
    : two_d::HilbertValueComputer<Value, LookupTableWidth>
{ };

template<typename Value, size_t LookupTableWidth>
struct HilbertValueComputer<Value, 3, LookupTableWidth>
    : three_d::HilbertValueComputer<Value, LookupTableWidth>
{ };

} // namespace hilbert

#endif //HILBERT_H__
