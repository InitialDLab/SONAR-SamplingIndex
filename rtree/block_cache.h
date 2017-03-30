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
// #ifndef BLOCK_CACHE_H__
// #define BLOCK_CACHE_H__

// #include <unordered_map>

// namespace rtree {

// template<typename Key, typename Value>
// struct BlockCache
// {
//     BlockCache(size_t capacity)
//         : capacity(capacity)
//     { }

//     using value_ptr = std::shared_ptr<Value>;

//     value_ptr get(Key const& key);
//     void put(Key const& key, value_ptr const& value);

//     void flush(void);

//     // 0 for unlimited
//     void set_capacity (size_t c) { capacity = c; }

//     size_t size (void) const { return entries.size(); }

// private:
//     void compact(void);

//     size_t capacity;

//     using CacheEntry = std::pair<Key, value_ptr>;
//     std::list<CacheEntry> entries;
//     //std::unordered_map<Key, typename std::list<CacheEntry>::iterator> cache;
//     std::map<Key, typename std::list<CacheEntry>::iterator> cache;
// };

// #define TDECL template<typename Key, typename Value>
// #define TARGS <Key, Value>

// TDECL
// typename BlockCache TARGS::value_ptr
// BlockCache TARGS::
// get(Key const& key)
// {
//     auto iter = cache.find(key);
//     if(iter == cache.end()) 
//         return value_ptr();

//     /*
//      * currently each block holds a single instance of stream
//      * we do not support multiple access at the same time due to get/put pointers
//      */
//     assert(iter->second->second.unique());

//     entries.splice(entries.begin(), entries, iter->second);
//     return iter->second->second;
// }

// TDECL
// void
// BlockCache TARGS::
// put(Key const& key, value_ptr const& value)
// {
// //    auto iter = cache.find(key);
// //    assert(iter == cache.end());

//     if((capacity > 0) && (entries.size() > capacity))
//         compact();

//     entries.push_front(std::make_pair(key, value));
//     cache.insert(std::make_pair(key, entries.begin()));
// }

// TDECL
// void
// BlockCache TARGS::
// flush(void)
// {
//     entries.clear();
//     cache.clear();
// }


// TDECL
// void
// BlockCache TARGS::
// compact(void)
// {
//     while(entries.size() > capacity)
//     {
//         cache.erase(entries.back().first);
//         entries.pop_back();
//     }
// }

// #undef TDECL
// #undef TARGS

// } // namespace rtree

// #endif //BLOCK_CACHE_H__
