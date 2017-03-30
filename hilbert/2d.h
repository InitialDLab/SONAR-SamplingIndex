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
#ifndef _2D_H__
#define _2D_H__

namespace two_d
{
    enum class Direction : int
    {
        Up, Down, Left, Right
    };

    using AllDirections = bm::vector_c<
        int, 
        (int)Direction::Up, 
        (int)Direction::Down, 
        (int)Direction::Left, 
        (int)Direction::Right
    >;

    template<int X, int Y>
    struct Corner 
    { 
        static constexpr int x = X;
        static constexpr int y = Y;
    };

    using CornerSW = Corner<0, 0>;
    using CornerNW = Corner<0, 1>;
    using CornerNE = Corner<1, 1>;
    using CornerSE = Corner<1, 0>;

    
    // corners and directions for the next level
    template <typename C0, Direction D0, typename C1, Direction D1, typename C2, Direction D2, typename C3, Direction D3>
    struct BaseHilbertLevel
        : bm::vector<
              bm::pair<C0, bm::int_<(int)D0>>,
              bm::pair<C1, bm::int_<(int)D1>>,
              bm::pair<C2, bm::int_<(int)D2>>,
              bm::pair<C3, bm::int_<(int)D3>>
          >
    { };

    template <Direction Dir>
    struct HilbertLevel { };

    template <>
    struct HilbertLevel<Direction::Up>
        : BaseHilbertLevel<
            CornerSW, Direction::Right, 
            CornerSE, Direction::Up, 
            CornerNE, Direction::Up,
            CornerNW, Direction::Left
          >
    { };

    template <>
    struct HilbertLevel<Direction::Down>
        : BaseHilbertLevel<
            CornerNE, Direction::Left,
            CornerNW, Direction::Down,
            CornerSW, Direction::Down, 
            CornerSE, Direction::Right
          >
    { };

    template <>
    struct HilbertLevel<Direction::Left>
        : BaseHilbertLevel<
            CornerNE, Direction::Down,
            CornerSE, Direction::Left,
            CornerSW, Direction::Left, 
            CornerNW, Direction::Up 
          >
    { };

    template <>
    struct HilbertLevel<Direction::Right>
        : BaseHilbertLevel<
            CornerSW, Direction::Up, 
            CornerNW, Direction::Right, 
            CornerNE, Direction::Right,
            CornerSE, Direction::Down
          >
    { };

// Value is the type of each coordinate
// will use at least 4^(LookupTableWidth + 1) * LookupTableWidth * 2 bits of memory for lookup table
// 1KB when LookupTableWidth == 4
// 512KB when LookupTableWidth == 8
// 64GB when LookupTableWidth == 16
template<typename Value, size_t LookupTableWidth>
struct HilbertValueComputer
{
    BOOST_STATIC_ASSERT(std::is_unsigned<Value>::value);
    BOOST_STATIC_ASSERT(std::numeric_limits<Value>::digits % LookupTableWidth == 0);
    BOOST_STATIC_ASSERT(std::numeric_limits<unsigned long>::digits >= LookupTableWidth+1);

    static constexpr int HilbertValueWidth = 
        (std::numeric_limits<Value>::digits + LookupTableWidth - 1) / LookupTableWidth;

    // int type for LookupTable
    using LTint_t = typename boost::uint_t<LookupTableWidth * 2>::least;
    using LTint_fast_t = typename boost::uint_t<LookupTableWidth * 2>::fast;
    using LTentry_t = std::pair<LTint_t, Direction>;

    using value_type = HilbertValue<LTint_t, HilbertValueWidth>;

    static constexpr unsigned long LOOKUP_DIM_SIZE = (1UL << LookupTableWidth);

    LTentry_t lookup[4][LOOKUP_DIM_SIZE][LOOKUP_DIM_SIZE];

    static constexpr Direction DefaultDirection = Direction::Up;


    template<Direction OriginalDir>
    struct Builder
    {
        template <typename CurLevel>
        void operator () (CurLevel) 
        {
            using CurCorner = typename CurLevel::first;
            constexpr Direction CurDirection = (Direction)CurLevel::second::value;
            hvc.build_lookup_table<CurDirection, OriginalDir>(
                CurCorner::x ? (x + (1UL << level)) : x,
                CurCorner::y ? (y + (1UL << level)) : y,
                level, 
                order
            );
        }
        
        HilbertValueComputer & hvc;
        unsigned long x;
        unsigned long y;
        size_t level;
        LTint_fast_t & order;
    };

    template<Direction Dir, Direction OriginalDir = Dir>
    void build_lookup_table(unsigned long x, unsigned long y, size_t level, LTint_fast_t & order)
    {
        if(level == 0)
        {
            lookup[(int)OriginalDir][x][y] = std::make_pair(order, Dir);
            ++order;
            return;
        }

        --level;
        bm::for_each<HilbertLevel<Dir>>(Builder<OriginalDir>({*this, x, y, level, order}));
    }


    struct BuildOneDirection
    {
        template<typename CurDir>
        void operator () (CurDir)
        {
            LTint_fast_t order = 0;
            hvc.build_lookup_table<(Direction)CurDir::value>(0, 0, LookupTableWidth, order);
        }
        HilbertValueComputer & hvc;
    };

    HilbertValueComputer()
    {
        bm::for_each<AllDirections>(BuildOneDirection({*this}));
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

        Direction cur_dir = DefaultDirection;
        for(int cur_bit = std::numeric_limits<Value>::digits - LookupTableWidth;
                cur_bit >= 0; 
                cur_bit -= LookupTableWidth)
        {
            auto cur_lookup = lookup[(int)cur_dir]
                                    [(x >> cur_bit) & coord_mask]
                                    [(y >> cur_bit) & coord_mask];
            cur_dir = cur_lookup.second;

            assert(iter != res.end());
            *iter = cur_lookup.first;
            ++iter;
        }

        return res;
    }
};

} // namspace two_d

#endif //_2D_H__