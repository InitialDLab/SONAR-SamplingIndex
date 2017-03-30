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
#ifndef _3D_H__
#define _3D_H__

namespace three_d
{

template<int Dim>
struct next_dim
    : bm::int_< (Dim + 1) % 3>
{ };

template<int Dim>
struct prev_dim
    : bm::int_< (Dim + 2) % 3>
{ };

template<int Dim>
struct reverse_dim
    : bm::int_< (3 - Dim) % 3>
{ };
   
constexpr int direction_count = 12;

template<int Dim, bool X, bool Y, bool Z>
struct Direction
{
    static constexpr int dim = Dim;
    static constexpr bool x = X;
    static constexpr bool y = Y;
    static constexpr bool z = Z;
    static constexpr int order = Dim * 4 + (x ? 2 : 0) + (y ? 1 : 0);

    BOOST_STATIC_ASSERT(order < direction_count);
    BOOST_STATIC_ASSERT(Z == (X != Y));
};

template<int N>
struct order_to_direction
{
    using type = Direction<N/4, (N&2), (N&1), ((N>>1)&1)^(N&1)>;
};

struct direction_order_verifier
{
    template<typename N>
    struct apply
    {
        using type = bm::bool_<
            order_to_direction<N::value>::type::order == N::value
        >;
    };
};

BOOST_MPL_ASSERT(( 
    bm::fold<
        bm::transform<
            bm::range_c<int, 0, direction_count>,
            direction_order_verifier,
            bm::back_inserter<bm::vector<>>
        >,
        bm::true_,
        bm::and_<bmp::_,bmp::_>
    >::type
));

// All directiosn we need
struct AllDirectionOrders
    : bm::range_c<int, 0, direction_count>
{ };

// Each level of curve is broken down into smaller curves
// Each curve has a direction
template<int Dim, bool X, bool Y, bool Z>
struct NextLevelDirectionsBase
    : bm::vector <
          Direction<prev_dim<Dim>::value, Z, X, Y>,
          Direction<next_dim<Dim>::value, Y, Z, X>,
          Direction<next_dim<Dim>::value, Y, Z, X>,
          Direction<Dim, X, !Y, !Z>,
          Direction<Dim, X, !Y, !Z>,
          Direction<next_dim<Dim>::value, !Y, Z, !X>,
          Direction<next_dim<Dim>::value, !Y, Z, !X>,
          Direction<prev_dim<Dim>::value, !Z, !X, Y>
      >
{ };

template<typename Dir>
struct NextLevelDirectionsFromDir
    : NextLevelDirectionsBase<Dir::dim, Dir::x, Dir::y, Dir::z>
{ };

template<int N>
struct NextLevelDirectionsFromOrder
    : NextLevelDirectionsFromDir<typename order_to_direction<N>::type>
{ };

// mark the 2x2x2 corners
template<bool X, bool Y, bool Z>
struct Corner
    : bm::vector_c<bool, X, Y, Z>
{
    static constexpr bool x = X;
    static constexpr bool y = Y;
    static constexpr bool z = Z;
};


template<int Dim>
struct shift_corner
{
    template<typename C>
    struct apply
    {
        using type = Corner<
            bm::at<C, reverse_dim<Dim>>::type::value,
            bm::at<C, next_dim<reverse_dim<Dim>::value>>::type::value,
            bm::at<C, prev_dim<reverse_dim<Dim>::value>>::type::value
        >;
    };
};

// in which order shall we walk through the 2x2x2 sub-paritions
template <int Dim, bool X, bool Y, bool Z>
struct NextLevelPartitions
    : bm::transform <
          bm::vector <
              Corner< X,  Y,  Z>,
              Corner< X,  Y, !Z>,
              Corner< X, !Y, !Z>,
              Corner< X, !Y,  Z>,
              Corner<!X, !Y,  Z>,
              Corner<!X, !Y, !Z>,
              Corner<!X,  Y, !Z>,
              Corner<!X,  Y,  Z>
          >,
          shift_corner<Dim>
      >::type
{ };


template<int Dim, bool X, bool Y, bool Z>
struct HilbertRecursionBase
    : bm::zip_view<
          bm::vector<
              NextLevelDirectionsBase<Dim, X, Y, Z>,
              NextLevelPartitions<Dim, X, Y, Z>
          >
      > ::type
{ };

template <typename Dir>
struct HilbertRecurionFromDir
    : HilbertRecursionBase<Dir::dim, Dir::x, Dir::y, Dir::z>
{ };

template <int N>
struct HilbertRecurionFromOrder
    : HilbertRecurionFromDir<typename order_to_direction<N>::type>
{ };


// Value is the type of each coordinate
// will use at least 8^(LookupTableWidth + 1) * LookupTableWidth * 3 bits of memory for lookup table
// 24KB when LookupTableWidth == 4
// 192MB when LookupTableWidth == 8
// 6PB when LookupTableWidth == 16
template<typename Value, size_t LookupTableWidth>
struct HilbertValueComputer
{
    BOOST_STATIC_ASSERT(std::is_unsigned<Value>::value);
    BOOST_STATIC_ASSERT(std::numeric_limits<Value>::digits % LookupTableWidth == 0);
    BOOST_STATIC_ASSERT(std::numeric_limits<unsigned long >::digits >= LookupTableWidth + 1);

    static constexpr int HilbertValueWidth = 
        (std::numeric_limits<Value>::digits + LookupTableWidth - 1) / LookupTableWidth;

    // int type for LookupTable
    using LTint_t = typename boost::uint_t<LookupTableWidth * 3>::least;
    using LTint_fast_t = typename boost::uint_t<LookupTableWidth * 3>::fast;
    using LTentry_t = std::pair<LTint_t, int>;

    using value_type = HilbertValue<LTint_t, HilbertValueWidth>;

    static constexpr unsigned long LOOKUP_DIM_SIZE = (1UL << LookupTableWidth);

    LTentry_t lookup[direction_count][LOOKUP_DIM_SIZE][LOOKUP_DIM_SIZE][LOOKUP_DIM_SIZE];

    static constexpr int DefaultDirection = 0;

    using point_type = boost::geometry::model::point<unsigned long, 3, boost::geometry::cs::cartesian>;

    template<int OriginalDir>
    struct Builder
    {
        template <typename HilbertRecursion>
        void operator () (HilbertRecursion)
        {
            using CurDir = typename bm::at_c<HilbertRecursion, 0>::type;
            using CurPart = typename bm::at_c<HilbertRecursion, 1>::type;

            // determine the min_corner of the current partition
            const unsigned long partition_size = (1UL << level);
            point_type pp(
                p.get<0>() + (CurPart::x ? partition_size : 0),
                p.get<1>() + (CurPart::y ? partition_size : 0),
                p.get<2>() + (CurPart::z ? partition_size : 0)
            );

            hvc.build_lookup_table<CurDir::order, OriginalDir>(
                pp,
                level, 
                order
            );
        }
        
        HilbertValueComputer & hvc;
        point_type const& p;
        size_t level;
        LTint_fast_t & order;
    };

    template<int DirOrder, int OriginalDirOrder = DirOrder>
    void build_lookup_table(point_type const& p, size_t level, LTint_fast_t & order)
    {
        using CurDir = typename order_to_direction<DirOrder>::type;
        if(level == 0)
        {
            lookup[OriginalDirOrder][p.get<0>()][p.get<1>()][p.get<2>()] = std::make_pair(order, DirOrder);
            ++order;
            return;
        }

        --level;
        bm::for_each<HilbertRecurionFromOrder<DirOrder>>(
                Builder<OriginalDirOrder>({*this, p, level, order})
        );
    }


    struct BuildOneDirection
    {
        template<typename CurDir>
        void operator () (CurDir)
        {
            point_type p(0,0,0);
            LTint_fast_t order = 0;
            hvc.build_lookup_table<CurDir::value>(p, LookupTableWidth, order);
        }
        HilbertValueComputer & hvc;
    };

    HilbertValueComputer()
    {
        bm::for_each<AllDirectionOrders>(BuildOneDirection({*this}));
    }

    template<typename Point>
    value_type
    operator () (Point const& p) const
    {
        value_type res;
        constexpr auto coord_mask = boost::low_bits_mask_t<LookupTableWidth>::sig_bits_fast;

        auto iter = res.begin();

        Value x = p.template get<0>();
        Value y = p.template get<1>();
        Value z = p.template get<2>();

        int cur_dir = DefaultDirection;
        for(int cur_bit = std::numeric_limits<Value>::digits - LookupTableWidth;
                cur_bit >= 0; 
                cur_bit -= LookupTableWidth)
        {
            auto cur_lookup = lookup[(int)cur_dir]
                                    [(x >> cur_bit) & coord_mask]
                                    [(y >> cur_bit) & coord_mask]
                                    [(z >> cur_bit) & coord_mask];
            cur_dir = cur_lookup.second;

            assert(iter != res.end());
            *iter = cur_lookup.first;
            ++iter;
        }

        return res;
    }

    value_type min_value() const
    {
        value_type res;

        // HACK: we are currently only using unsigned int for hilbert values.  should change the
        // code to accomidate for different input types
        for (auto i = res.begin(); i != res.end(); ++i)
            *i = std::numeric_limits<unsigned int>::min();

        return res;
    }

    value_type max_value() const
    {
        value_type res;

        // HACK: we are currently only using unsigned int for hilbert values.  should change the
        // code to accomidate for different input types
        for (auto i = res.begin(); i != res.end(); ++i)
            *i = std::numeric_limits<unsigned int>::max();

        return res;
    }
};


} // namespace three_d

#endif //_3D_H__