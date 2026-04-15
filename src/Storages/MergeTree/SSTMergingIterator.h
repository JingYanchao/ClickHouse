#pragma once

#include <memory>
#include <queue>
#include <vector>

#include <Storages/MergeTree/SSTFileUtil.h>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>

namespace DB
{

/// Lightweight multi-way merge iterator over rocksdb SST iterators.
/// Produces keys in sorted order; when keys are equal, the iterator
/// with the smaller index (older part) comes first.
/// Used by `buildAllDeleteBitmapsForPartition` for startup dedup.
class SSTMergingIterator
{
public:
    SSTMergingIterator(
        std::vector<std::unique_ptr<rocksdb::Iterator>> iters,
        std::vector<SSTFileReaderPtr> readers);

    bool valid() const { return !min_heap.empty(); }
    void seekToFirst();
    void next();

    rocksdb::Slice key() const { return iters[min_heap.top()]->key(); }
    rocksdb::Slice value() const { return iters[min_heap.top()]->value(); }
    size_t currentIndex() const { return min_heap.top(); }

private:
    struct Comparator
    {
        const std::vector<std::unique_ptr<rocksdb::Iterator>> * iters_ptr;
        const rocksdb::Comparator * cmp;

        explicit Comparator(const std::vector<std::unique_ptr<rocksdb::Iterator>> * p)
            : iters_ptr(p), cmp(rocksdb::BytewiseComparator()) {}

        bool operator()(size_t lhs, size_t rhs) const
        {
            int res = cmp->Compare((*iters_ptr)[lhs]->key(), (*iters_ptr)[rhs]->key());
            if (res > 0) return true;
            if (res < 0) return false;
            /// Equal keys: smaller index (older part) should come first (top of min-heap).
            return lhs > rhs;
        }
    };

    using MinHeap = std::priority_queue<size_t, std::vector<size_t>, Comparator>;
    std::vector<std::unique_ptr<rocksdb::Iterator>> iters;
    /// Hold SSTFileReaderPtr to keep underlying memory alive.
    std::vector<SSTFileReaderPtr> readers;
    MinHeap min_heap;
};

}
