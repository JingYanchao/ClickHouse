#include <Storages/MergeTree/SSTMergingIterator.h>

namespace DB
{

SSTMergingIterator::SSTMergingIterator(
    std::vector<std::unique_ptr<rocksdb::Iterator>> iters_,
    std::vector<SSTFileReaderPtr> readers_)
    : iters(std::move(iters_))
    , readers(std::move(readers_))
    , min_heap(Comparator(&iters))
{
}

void SSTMergingIterator::seekToFirst()
{
    /// Rebuild the heap from scratch.
    min_heap = MinHeap(Comparator(&iters));
    for (size_t i = 0; i < iters.size(); ++i)
    {
        iters[i]->SeekToFirst();
        if (iters[i]->Valid() && !iters[i]->key().empty())
            min_heap.push(i);
    }
}

void SSTMergingIterator::next()
{
    chassert(valid());
    auto idx = min_heap.top();
    min_heap.pop();
    iters[idx]->Next();
    if (iters[idx]->Valid() && !iters[idx]->key().empty())
        min_heap.push(idx);
}

}
