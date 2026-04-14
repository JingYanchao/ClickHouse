#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/MergeTree/SSTMergingIterator.h>
#include <rocksdb/comparator.h>
#include <rocksdb/iterator.h>
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
/// Contains the part's delete mark and the union of all active parts'
/// block ranges in the same partition, captured atomically under the
/// dedup lock.
struct DedupMetadataForFetch
{
    ProjectionIndexBitmapPtr delete_mark;
    boost::icl::interval_set<Int64> block_ranges;

    /// Serialize the metadata (delete mark + block ranges) into a write buffer.
    void serialize(WriteBuffer & out) const;

    /// Deserialize the metadata from a read buffer.
    /// Fields are populated into the returned struct; the caller is
    /// responsible for moving them into the fetched part.
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

    /// Deduplicate new_part against all active parts in the same partition.
    /// Populates delta_deleted_rows_map with rows to be deleted.
    ///
    /// The operation type is inferred from part metadata:
    ///   - level == 0 + mutation == 0                  → Insert  (full cross-part dedup)
    ///   - level == 0 + mutation > 0                   → Mutation of level-0 part (clone delete marks)
    ///   - level > 0 + mutation source found           → Mutation (clone source delete marks)
    ///   - level > 0 + has snapshots + no mut. source  → Merge   (propagate source delete marks)
    ///   - level > 0 + no snapshots + no mut. source   → Fetch   (dual-iterator or full dedup)
    ///
    /// The optional delete_mark_snapshots carries a snapshot of source parts'
    /// delete marks taken at merge/mutation start, enabling diff-based dedup
    /// that only processes newly deleted keys instead of scanning all keys.
    /// When the optional has a value (even an empty map), the part is treated
    /// as a locally-produced merge; when it is nullopt, the part is treated
    /// as a fetch (or insert/mutation, which are handled by earlier branches).
    void dedupPart(
        const DataPartPtr & new_part,
        const std::optional<MergeTreeData::DeleteMarkSnapshotMap> & source_part_delete_mark_snapshots = std::nullopt);

    /// Apply accumulated delta delete bitmaps to each part's delete_mark_bitmap.
    /// Should be called under DataPartsLock to ensure atomicity with part state transitions.
    void commitDeleteMarkBuffers(const DataPartsLock & lock);

    /// Build delete marks for all active parts on startup.
    /// For each partition, parts are sorted from oldest to newest and deduped
    /// using a multi-way merge of their SST iterators.
    void buildAllDeleteMarksOnStartup();

    UniqueProcessLock lockUniqueProcess() { return UniqueProcessLock(unique_process_mutex); }

    /// Collect dedup metadata needed by the fetching replica.
    /// Acquires the dedup lock internally, captures the part's delete mark
    /// and the union of all active parts' block ranges in the same partition.
    DedupMetadataForFetch collectDedupMetadataForFetch(const DataPartPtr & part) const;

    /// A data part paired with its SST reader, used uniformly for both
    /// the input part and visible parts during dedup.
    struct PartWithSSTReader
    {
        DataPartPtr part;
        SSTFileReaderPtr reader;

        /// Release the underlying ReadBuffer memory (~1MB per file) while
        /// keeping Bloom filter and index blocks pinned in the cached reader.
        void releaseBufferMemory() const
        {
            if (reader)
                reader->releaseBufferMemory();
        }
    };

    /// Release ReadBuffer memory for a batch of PartWithSSTReader entries.
    static void releaseAllBufferMemory(const std::vector<PartWithSSTReader> & readers)
    {
        for (const auto & r : readers)
            r.releaseBufferMemory();
    }

    /// SST context for dedup: holds the input part and pre-opened
    /// SSTFileReaders for visible parts.
    struct DedupPartWithSSTReaders
    {
        PartWithSSTReader input;
        std::vector<PartWithSSTReader> visible_parts;

        /// Release ReadBuffer memory for all held SST readers.
        void releaseBufferMemory() const
        {
            input.releaseBufferMemory();
            releaseAllBufferMemory(visible_parts);
        }
    };

private:
    /// Open SST readers for a list of parts, skipping parts without SST.
    /// Returns pairs of (part, reader) for parts that have valid SST readers.
    /// Shared by prepareSSTReadersForDedup and buildAllDeleteMarksForPartition.
    static std::vector<PartWithSSTReader> openSSTReadersForParts(
        const DataPartsVector & parts,
        const StorageMetadataPtr & metadata_snapshot);

    DedupPartWithSSTReaders prepareSSTReadersForDedup(
        const DataPartPtr & input_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & other_parts);

    /// Dedup path for INSERT-produced parts: cross-part dedup against all
    /// active visible parts using parallel SST key comparison.
    void dedupForInsert(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & visible_parts);

    /// Dedup path for FETCH-produced parts: uses reverse dedup approach.
    /// Iterates the small visible parts' SST keys and MultiGet into the
    /// large fetched part's SST. When fetch_dedup_block_ranges (the union of
    /// all active parts' block ranges obtained from the source replica at
    /// fetch time) is available, visible parts fully covered by those ranges
    /// are skipped entirely, dramatically reducing the dedup scope. The source
    /// replica's delete mark is applied directly to the fetched part, avoiding
    /// diff computation.
    void dedupForFetch(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & source_parts,
        const DataPartsVector & all_visible_parts);

    /// Dedup path for MERGE-produced parts: propagate delete marks from
    /// source parts into the merged part via deleted-keys optimization.
    /// Source parts (covered by the merged part's block range) are pre-computed
    /// by `dedupPart` and passed in directly.
    /// No cross-part dedup is needed because merge only combines existing data
    /// without introducing new unique keys.
    void dedupForMerge(
        const DataPartPtr & new_part,
        const StorageMetadataPtr & metadata_snapshot,
        const DataPartsVector & source_parts,
        const MergeTreeData::DeleteMarkSnapshotMap & delete_mark_snapshots,
        const DataPartsVector & all_visible_parts);

    /// Dedup path for MUTATION-produced parts: a mutation transforms exactly
    /// one source part into one new part.
    /// The source part is pre-identified by `dedupPart` (same block range,
    /// lower mutation version) and passed in directly.
    /// Mutation preserves row count, so row offsets are unchanged.
    /// Simply clone the source part's delete mark bitmap to the new part.
    void dedupForMutation(
        const DataPartPtr & new_part,
        const DataPartPtr & source_part);

    /// Build delete marks for all parts in a single partition using a
    /// multi-way merge of their SST iterators. This is O(N·log(N)·K)
    /// where N = number of parts and K = total keys, compared to the
    /// previous O(N²·K) approach.
    void buildAllDeleteMarksForPartitionOnStartup(
        const DataPartsVector & partition_parts,
        const StorageMetadataPtr & metadata_snapshot);

    /// Propagate newly deleted keys from source parts into the merged part.
    /// Only processes keys whose delete status changed since the merge started
    /// (diff between current and snapshot delete marks).
    /// Correctness relies on version transitivity: a higher-version INSERT
    /// that deletes a merge winner necessarily deletes all its losers too.
    void dedupDeletedKeysFromSourceParts(
        const DataPartPtr & new_part,
        const DataPartsVector & source_parts,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::DeleteMarkSnapshotMap & source_delete_mark_snapshots = {});

    const MergeTreeData & storage;
    std::mutex unique_process_mutex;
    PartDeleteBitmapMap delta_deleted_rows_map; /// protected by unique_process_mutex

    /// Defensive flag: set to true after `buildAllDeleteMarksOnStartup` completes.
    /// `dedupPart` asserts this flag to catch accidental reordering of startup
    /// steps that would allow background merges/mutations to run before the
    /// initial delete mark rebuild is finished.
    std::atomic<bool> startup_completed{false};

    LoggerPtr log;
};
using MergeTreeDedupPartManagerPtr = std::shared_ptr<MergeTreeDedupPartManager>;
}
