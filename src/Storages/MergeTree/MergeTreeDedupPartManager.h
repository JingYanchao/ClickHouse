#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <boost/icl/interval_set.hpp>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

class IMergeTreeDataPart;
struct DataPartsLock;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct ProjectionIndexBitmap;
using ProjectionIndexBitmapPtr = std::shared_ptr<ProjectionIndexBitmap>;

using PartDeleteBitmapMap = std::unordered_map<const IMergeTreeDataPart *, ProjectionIndexBitmapPtr>;

/// Dedup metadata collected by the source replica for a fetched part.
struct DedupMetadataForFetch
{
    ProjectionIndexBitmapPtr delete_mark;
    boost::icl::interval_set<Int64> block_ranges;

    void serialize(WriteBuffer & out) const;
    static DedupMetadataForFetch deserialize(ReadBuffer & in);
};

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

    void dedupPart(
        const DataPartPtr & new_part,
        const std::optional<MergeTreeData::DeleteMarkSnapshotMap> & source_part_delete_mark_snapshots = std::nullopt);

    /// Apply accumulated delta delete bitmaps. Must be called under DataPartsLock.
    void commitDeleteMarkBuffers(const DataPartsLock & lock);

    /// Build delete marks for all active parts on startup.
    void buildAllDeleteMarksOnStartup();

    UniqueProcessLock lockUniqueProcess() { return UniqueProcessLock(unique_process_mutex); }

    /// Collect dedup metadata for the fetching replica.
    DedupMetadataForFetch collectDedupMetadataForFetch(const DataPartPtr & part) const;

    /// A data part paired with its SST reader.
    struct PartWithSSTReader
    {
        DataPartPtr part;
        SSTFileReaderPtr reader;

        void releaseBufferMemory() const
        {
            if (reader)
                reader->releaseBufferMemory();
        }
    };

    static void releaseAllBufferMemory(const std::vector<PartWithSSTReader> & readers)
    {
        for (const auto & r : readers)
            r.releaseBufferMemory();
    }

    /// SST context for dedup: input part + visible parts with their SST readers.
    struct DedupPartWithSSTReaders
    {
        PartWithSSTReader input;
        std::vector<PartWithSSTReader> visible_parts;

        void releaseBufferMemory() const
        {
            input.releaseBufferMemory();
            releaseAllBufferMemory(visible_parts);
        }
    };

private:
    /// Open SST readers for a list of parts, skipping parts without SST.
    static std::vector<PartWithSSTReader> openSSTReadersForParts(
        const DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot);

    DedupPartWithSSTReaders prepareSSTReadersForDedup(
        const DataPartPtr & input_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & other_parts);

    /// Dedup for INSERT-produced parts.
    void dedupForInsert(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & visible_parts);

    /// Dedup for FETCH-produced parts.
    void dedupForFetch(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & source_parts,
        const DataPartsVector & all_visible_parts);

    /// Dedup for MERGE-produced parts: propagate delete marks from source parts.
    void dedupForMerge(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & source_parts,
        const MergeTreeData::DeleteMarkSnapshotMap & delete_mark_snapshots,
        const DataPartsVector & all_visible_parts);

    /// Dedup for MUTATION-produced parts: clone source part's delete mark.
    void dedupForMutation(
        const DataPartPtr & new_part,
        const DataPartPtr & source_part);

    /// Build delete marks for all parts in a single partition on startup.
    void buildAllDeleteMarksForPartitionOnStartup(
        const DataPartsVector & partition_parts,
        const StorageMetadataPtr & metadata_snapshot);

    /// Propagate newly deleted keys from source parts into the merged part.
    void dedupDeletedKeysFromSourceParts(
        const DataPartPtr & new_part,
        const DataPartsVector & source_parts,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::DeleteMarkSnapshotMap & source_delete_mark_snapshots = {});

    const MergeTreeData & storage;
    std::mutex unique_process_mutex;
    PartDeleteBitmapMap delta_deleted_rows_map; /// protected by unique_process_mutex

    /// Set to true after `buildAllDeleteMarksOnStartup` completes.
    std::atomic<bool> startup_completed{false};

    LoggerPtr log;
};
using MergeTreeDedupPartManagerPtr = std::shared_ptr<MergeTreeDedupPartManager>;
}
