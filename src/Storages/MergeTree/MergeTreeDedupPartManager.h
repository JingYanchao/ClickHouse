#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
#include <queue>

namespace DB
{

class IMergeTreeDataPart;
struct DataPartsLock;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct ProjectionIndexBitmap;
using ProjectionIndexBitmapPtr = std::shared_ptr<ProjectionIndexBitmap>;

/// Maps part pointer -> delete bitmap (rows to be marked as deleted).
using PartDeleteBitmapType = std::unordered_map<const IMergeTreeDataPart *, ProjectionIndexBitmapPtr>;


struct UniqueProcessLock
{
    explicit UniqueProcessLock(std::mutex & unique_process_lock_);
    ~UniqueProcessLock();

    UniqueProcessLock(const UniqueProcessLock &) = delete;
    UniqueProcessLock & operator=(const UniqueProcessLock &) = delete;
    UniqueProcessLock(UniqueProcessLock &&) = default;
    UniqueProcessLock & operator=(UniqueProcessLock &&) = default;

    void unlock() { lock.unlock(); }

private:
    std::optional<Stopwatch> wait_watch;
    std::unique_lock<std::mutex> lock;
    std::optional<Stopwatch> lock_watch;
};

class MergeTreeDedupPartManager
{
public:
explicit MergeTreeDedupPartManager(const MergeTreeData & storage_);
    ~MergeTreeDedupPartManager() = default;

    /// Deduplicate new_part against all active parts in the same partition.
    /// Populates delta_deleted_rows_map with rows to be deleted.
    /// The op parameter indicates how this part was produced (insert, merge, etc.),
    /// enabling optimization strategies (e.g. skipping older parts for merge).
    void dedupPart(const DataPartPtr & new_part, MergeTreeData::CommitOperation op = MergeTreeData::CommitOperation::Insert);

    /// Apply accumulated delta delete bitmaps to each part's delete_mark_bitmap.
    /// Should be called under DataPartsLock to ensure atomicity with part state transitions.
    void commitDeleteMarkBuffers(const DataPartsLock & lock);

    /// Build delete marks for all active parts on startup.
    /// For each partition, parts are sorted from oldest to newest and deduped
    /// using a multi-way merge of their SST iterators.
    void buildAllDeleteMarksOnStartup();

    UniqueProcessLock lockUniqueProcess() { return UniqueProcessLock(unique_process_mutex); }

    /// A data part paired with its SST reader, used uniformly for both
    /// the input part and visible parts during dedup.
    struct PartWithSSTReader
    {
        DataPartPtr part;
        SSTFileReaderPtr reader;
    };

    /// SST context for dedup: holds the input part and pre-opened
    /// SSTFileReaders for visible parts.
    struct DedupPartWithSSTReaders
    {
        PartWithSSTReader input;
        std::vector<PartWithSSTReader> visible_parts;
    };

    /// Lightweight multi-way merge iterator over rocksdb SST iterators.
    /// Produces keys in sorted order; when keys are equal, the iterator
    /// with the smaller index (older part) comes first.
    /// Used by `buildAllDeleteMarksForPartition` for startup dedup.
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

private:
    /// Open SST readers for a list of parts, skipping parts without SST.
    /// Returns pairs of (part, reader) for parts that have valid SST readers.
    /// Shared by prepareSSTReadersForDedup and buildAllDeleteMarksForPartition.
    std::vector<PartWithSSTReader> openSSTReadersForParts(
        const DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot);

    DedupPartWithSSTReaders prepareSSTReadersForDedup(
        const DataPartPtr & input_part,
        const StorageMetadataPtr & metadata_snapshot,
        MergeTreeData::DataPartsVector & visible_parts);

    /// Optimize visible parts list for merge-produced parts by filtering out
    /// parts that cannot conflict with the merge result.
    void optimizeVisiblePartsForMerge(
        const DataPartPtr & new_part,
        MergeTreeData::DataPartsVector & visible_parts);

    /// Build intra-part delete mark bitmap by scanning the input SST.
    /// Rows not present in the SST are duplicates that lost during SST
    /// construction (last-write-wins). Returns nullptr if no duplicates.
    ProjectionIndexBitmapPtr buildIntraPartDeleteMark(
        const DataPartPtr & input_part,
        const SSTFileReader & input_sst);

    /// Build delete marks for all parts in a single partition using a
    /// multi-way merge of their SST iterators. This is O(N·log(N)·K)
    /// where N = number of parts and K = total keys, compared to the
    /// previous O(N²·K) approach.
    void buildAllDeleteMarksForPartition(
        const DataPartsVector & partition_parts,
        const StorageMetadataPtr & metadata_snapshot);

    const MergeTreeData & storage;
    std::mutex unique_process_mutex;
    PartDeleteBitmapType delta_deleted_rows_map; /// protected by unique_process_mutex
    LoggerPtr log;
};
using MergeTreeDedupPartManagerPtr = std::shared_ptr<MergeTreeDedupPartManager>;
}
