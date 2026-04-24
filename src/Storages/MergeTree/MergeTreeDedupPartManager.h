#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <Common/RWLock.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/ProjectionsDescription.h>
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

/// Dedup metadata sent from the source replica for a fetched part.
struct DedupMetadataForFetch
{
    ProjectionIndexBitmapPtr delete_bitmap;
    boost::icl::interval_set<Int64> block_ranges;

    void serialize(WriteBuffer & out) const;
    static DedupMetadataForFetch deserialize(ReadBuffer & in);
};

struct UniqueProcessLock
{
    explicit UniqueProcessLock(const RWLock & unique_process_lock_);
    ~UniqueProcessLock();

    UniqueProcessLock(const UniqueProcessLock &) = delete;
    UniqueProcessLock & operator=(const UniqueProcessLock &) = delete;
    UniqueProcessLock(UniqueProcessLock &&) = default;
    UniqueProcessLock & operator=(UniqueProcessLock &&) = default;

    void unlock() { lock_holder.reset(); }

private:
    std::optional<Stopwatch> wait_watch;
    RWLockImpl::LockHolder lock_holder;
    std::optional<Stopwatch> lock_watch;
};

class MergeTreeDedupPartManager
{
public:
explicit MergeTreeDedupPartManager(MergeTreeData & storage_);
    ~MergeTreeDedupPartManager() = default;

    void dedupPart(
        const DataPartPtr & new_part,
        const std::optional<MergeTreeData::DeleteBitmapSnapshotMap> & source_part_delete_bitmap_snapshots = std::nullopt);

    /// Apply delta delete bitmaps. Must be called under DataPartsLock.
    void commitDeleteBitmapBuffers(const DataPartsLock & lock);

    /// Persist delete bitmaps to RocksDB (key: part name, value: serialized bitmap).
    void storeDeleteMarkBuffers(const std::unordered_map<std::string, DeleteBitmapPtr> & buffers);

    /// Load a persisted delete bitmap for a part from RocksDB (nullptr if not found).
    ProjectionIndexBitmapPtr loadDeleteBitmapFromRocksDB(const std::string & part_name);

    /// Remove the persisted delete bitmap for a part from RocksDB.
    void removeDeleteBitmap(const std::string & part_name) const;

    /// Re-dedup and persist parts whose delete bitmaps are missing in RocksDB.
    void checkAndDedupPartsOnStartup(const DataPartsVector & all_active_parts);

    /// Load persisted delete bitmaps from RocksDB for all active parts,
    /// then re-dedup any parts that have no persisted bitmap.
    void loadDeleteBitmapsAndCheckOnStartup();

    UniqueProcessLock lockUniqueProcess() { return UniqueProcessLock(unique_process_mutex); }

    /// Dedup committing parts under UniqueProcessLock (BeforeTransactionCommit hook).
    std::any dedupPartsBeforeTransactionCommit(
        const MergeTreeData::MutableDataParts & parts,
        const MergeTreeData::BeforeCommitHookContext & before_commit_context);

    /// Collect dedup metadata for a fetched part.
    DedupMetadataForFetch collectDedupMetadataForFetch(const DataPartPtr & part) const;

    /// A data part with its SST reader.
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

private:
    /// Open SST readers for the input part and all visible parts.
    std::pair<PartWithSSTReader, std::vector<PartWithSSTReader>> prepareSSTReadersForDedup(
        const DataPartPtr & input_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & visible_parts);

    void dedupForInsert(
        const PartWithSSTReader & input,
        std::vector<PartWithSSTReader> & parts_to_search);

    void dedupForFetch(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & source_parts,
        const PartWithSSTReader & input,
        std::vector<PartWithSSTReader> & parts_to_search);

    /// Propagate delete bitmaps from source parts into the merged part.
    void dedupForMerge(
        const DataPartPtr & new_part,
        const PartWithSSTReader & input,
        const std::vector<PartWithSSTReader> & source_parts_with_readers,
        const MergeTreeData::DeleteBitmapSnapshotMap & delete_bitmap_snapshots);

    /// Clone source part's delete bitmap for mutation.
    void dedupForMutation(
        const DataPartPtr & new_part,
        const DataPartsVector & source_parts);

    void dedupDeletedKeysFromSourceParts(
        const DataPartPtr & new_part,
        const SSTFileReaderPtr & input_reader,
        const std::vector<PartWithSSTReader> & source_part_readers,
        const MergeTreeData::DeleteBitmapSnapshotMap & source_delete_bitmap_snapshots = {});

    MergeTreeData & storage;
    /// FIFO-fair lock that serializes the dedup commit phase across all write paths.
    RWLock unique_process_mutex = RWLockImpl::create();
    PartDeleteBitmapMap delta_deleted_rows_map; /// protected by unique_process_mutex

    /// RocksDB for persistent delete bitmaps.
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    String rocksdb_dir;

    std::atomic<bool> startup_completed{false};

    LoggerPtr log;
};
using MergeTreeDedupPartManagerPtr = std::shared_ptr<MergeTreeDedupPartManager>;
}
