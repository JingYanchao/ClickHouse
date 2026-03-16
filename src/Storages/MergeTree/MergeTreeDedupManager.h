#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <roaring/roaring.hh>

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SSTFileUtil.h>

namespace DB
{

class IMergeTreeDataPart;
struct DataPartsLock;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Maps part pointer -> delete bitmap (rows to be marked as deleted).
using PartDeleteBitmapType = std::unordered_map<const IMergeTreeDataPart *, roaring::Roaring>;


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

class MergeTreeDedupManager
{
public:
    explicit MergeTreeDedupManager(const MergeTreeData & storage_);
    ~MergeTreeDedupManager() = default;

    /// Deduplicate the new_part against all active parts in the same partition.
    /// Populates delta_deleted_rows_map with rows to be deleted.
    /// The source parameter indicates how this part was produced (insert, merge, etc.),
    /// enabling optimization strategies for merge-produced parts.
    void dedupUniqueIndex(const DataPartPtr & new_part, MergeTreeData::CommitOperation op = MergeTreeData::CommitOperation::Insert);

    /// Apply accumulated delta delete bitmaps to each part's delete_mark_bitmap.
    /// Should be called under DataPartsLock to ensure atomicity with part state transitions.
    void commitDeleteMarkBuffers(const DataPartsLock & lock);

    /// Rebuild delete marks for all active parts on startup.
    /// For each partition, parts are sorted from oldest to newest; each part is deduped
    /// against all older parts using prepareSSTReadersForDedup + dedupKeysThroughNewCommitParts.
    void rebuildAllDeleteMarks();

    UniqueProcessLock lockUniqueProcess() { return UniqueProcessLock(unique_process_mutex); }

    const PartDeleteBitmapType & getDeltaDeletedRowsMap() const { return delta_deleted_rows_map; }

private:
    /// Prepare SST readers for dedup: open input_part's SST reader and all visible parts' SST readers.
    /// Returns {input_sst_reader, visible_sst_readers}. If input_part has no SST, input_sst_reader is nullptr.
    struct DedupSSTContext
    {
        SSTFileReaderPtr input_sst;
        std::vector<SSTFileReaderPtr> visible_sst_readers;
    };

    DedupSSTContext prepareSSTReadersForDedup(
        const DataPartPtr & input_part,
        const StorageMetadataPtr & metadata_snapshot,
        MergeTreeData::DataPartsVector & visible_parts);

    /// Optimize visible parts list for merge-produced parts.
    /// Filters out older parts that cannot conflict with the merge result,
    /// and clears visible_parts entirely when the merge result contains
    /// exactly the same effective rows as the covered source parts.
    void optimizeVisiblePartsForMerge(
        const DataPartPtr & new_part,
        MergeTreeData::DataPartsVector & visible_parts);

    /// Build intra-part delete mark bitmap by scanning the input SST.
    /// Rows whose part_offset is NOT present in the SST are duplicates
    /// that lost during SST construction (last-write-wins) and must be deleted.
    /// Returns nullptr when no intra-part duplicates exist.
    ProjectionIndexBitmapPtr buildIntraPartDeleteMark(
        const DataPartPtr & input_part,
        const SSTFileReader & input_sst);

    const MergeTreeData & storage;
    std::mutex unique_process_mutex;
    PartDeleteBitmapType delta_deleted_rows_map; /// protected by unique_process_mutex
    LoggerPtr log;
};
using MergeTreeDedupManagerPtr = std::shared_ptr<MergeTreeDedupManager>;
}
