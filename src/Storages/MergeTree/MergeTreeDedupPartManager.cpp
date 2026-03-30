#include <Storages/MergeTree/MergeTreeDedupPartManager.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

namespace CurrentMetrics
{
    extern const Metric UniqueKeyDedupThreads;
    extern const Metric UniqueKeyDedupThreadsActive;
    extern const Metric UniqueKeyDedupThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event UniqueProcessLockWaitMicroseconds;
    extern const Event UniqueProcessLockHoldMicroseconds;
    extern const Event UniqueKeyDedupBoundaryKeyScanMicroseconds;
    extern const Event UniqueKeyDedupParallelProcessMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Default number of parallel dedup threads.
static constexpr size_t DEDUP_MAX_PARALLEL = 16;
/// Minimum number of keys to trigger parallel dedup; below this threshold, single-threaded path is used.
static constexpr size_t MIN_KEYS_FOR_PARALLEL = 16 * 16 * 2;

/// Type alias for the visible part SST metadata used by dedup functions.
using VisiblePartSSTInfo = MergeTreeDedupPartManager::DedupSSTContext::VisiblePartSSTMeta;

/// Per-thread delta bitmap: records rows to be deleted for each affected part.
using DeltaBitmaps = PartDeleteBitmapType;

/// Maximum number of keys to batch in a single MultiGet call.
static constexpr size_t MULTI_GET_BATCH_SIZE = 16;

/// Process a range of keys from the input SST and record dedup losers
/// into thread-local delta_bitmaps. Keys are batched via MultiGet
/// to amortize per-call overhead.
static void dedupKeyRange(
    const DataPartPtr & input_part,
    const SSTFileReader & input_sst,
    const MergeTreeData::DataPartsVector & all_visible_parts,
    const std::vector<VisiblePartSSTInfo> & visible_part_infos,
    const std::string & start_key,
    size_t num_keys,
    DeltaBitmaps & delta_bitmaps,
    size_t shard_id)
{
    /// Cached entry from the input SST iterator, used for batching.
    struct InputKeyEntry
    {
        std::string key;
        UniqueValueEntryFull entry;
    };

    if (num_keys == 0)
        return;

    Stopwatch shard_watch;

    rocksdb::ReadOptions opts;
    opts.fill_cache = true;
    auto input_iter = input_sst.newIterator(opts);
    input_iter->Seek(rocksdb::Slice(start_key));

    /// Detect once whether the SST uses row-level versioning (16-byte values)
    /// or part-level versioning (8-byte values).
    bool has_row_version = false;
    bool version_detected = false;

    /// Reusable batch buffer to avoid repeated allocations.
    std::vector<InputKeyEntry> batch;
    batch.reserve(MULTI_GET_BATCH_SIZE);

    /// Track which input keys have already been matched by a visible part
    /// (only one visible part can own a given unique key).
    std::vector<bool> matched;
    matched.reserve(MULTI_GET_BATCH_SIZE);

    size_t processed = 0;
    while (processed < num_keys)
    {
        /// Phase 1: Collect a batch of keys from the input iterator.
        batch.clear();
        for (; input_iter->Valid() && processed < num_keys && batch.size() < MULTI_GET_BATCH_SIZE;
             input_iter->Next(), ++processed)
        {
            if (unlikely(!input_iter->status().ok()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper new parts iterator has error {}", input_iter->status().ToString());

            if (unlikely(input_iter->key().empty() || input_iter->value().empty()))
                continue;

            const auto & value_slice = input_iter->value();
            auto input_entry = decodeUniqueValueEntry(value_slice.data(), value_slice.size());

            if (!version_detected)
            {
                has_row_version = (value_slice.size() == 16);
                version_detected = true;
            }

            batch.push_back({input_iter->key().ToString(), input_entry});
        }

        if (batch.empty())
            break;

        /// Phase 2: For each visible part, batch-lookup all unmatched keys.
        /// Reset matched flags for this batch.
        matched.assign(batch.size(), false);

        for (const auto & info : visible_part_infos)
        {
            /// Build a sub-batch of keys not yet matched by a previous visible part.
            std::vector<rocksdb::Slice> candidate_keys;
            std::vector<size_t> candidate_batch_indices; /// index into batch[]
            candidate_keys.reserve(batch.size());
            candidate_batch_indices.reserve(batch.size());

            for (size_t i = 0; i < batch.size(); ++i)
            {
                if (matched[i])
                    continue;

                candidate_keys.emplace_back(batch[i].key);
                candidate_batch_indices.push_back(i);
            }

            if (candidate_keys.empty())
                break;

            /// Batch MultiGet for all candidate keys at once.
            std::vector<std::string> values;
            auto statuses = info.reader->multiGet(candidate_keys, &values);

            /// Process results — compare versions and mark losers.
            const auto & current_part = all_visible_parts[info.index];
            auto current_delete_bitmap = current_part->getDeleteMarkBitmap();

            /// Retrieve the effective version for a part/entry pair.
            /// When row-level versioning is available, use the per-row version
            /// stored in the entry; otherwise fall back to part-level max_block.
            auto get_part_version = [has_row_version](const DataPartPtr & part, const UniqueValueEntryFull & entry) -> UInt64
            {
                return has_row_version ? entry.version : static_cast<UInt64>(part->info.max_block);
            };

            for (size_t c = 0; c < candidate_keys.size(); ++c)
            {
                if (!statuses[c].ok())
                    continue;

                const auto & current_value = values[c];
                if (unlikely(current_value.empty()))
                    continue;
                if (unlikely(current_value.size() != 8 && current_value.size() != 16))
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Unexpected unique projection value size: {} (expected 8 or 16)",
                        current_value.size());

                auto current_entry = decodeUniqueValueEntry(current_value.data(), current_value.size());

                if (current_delete_bitmap && current_delete_bitmap->contains(current_entry.part_offset))
                    continue;

                size_t batch_idx = candidate_batch_indices[c];
                const auto & input_entry = batch[batch_idx].entry;

                auto input_version = get_part_version(input_part, input_entry);
                auto visible_version = get_part_version(current_part, current_entry);

                if (input_version > visible_version
                    || (input_version == visible_version && input_entry.part_offset > current_entry.part_offset))
                {
                    /// Input part wins — mark the visible part's row as deleted.
                    delta_bitmaps[current_part.get()].add(static_cast<uint32_t>(current_entry.part_offset));
                }
                else
                {
                    /// Visible part wins — mark the input part's row as deleted.
                    delta_bitmaps[input_part.get()].add(static_cast<uint32_t>(input_entry.part_offset));
                }

                /// Only one visible part can own a given unique key.
                matched[batch_idx] = true;
            }
        }
    }

    LOG_TEST(
        getLogger("DedupKeyRange"),
        "shard={}, num_keys={}, processed={}, elapsed={}us",
        shard_id, num_keys, processed, shard_watch.elapsedMicroseconds());
}

/// Deduplicate keys from input_part against visible parts using parallel threads.
/// Splits the input SST key space into shards, processes each in a separate thread,
/// then merges per-shard delta bitmaps into cross_part_delete_marks.
static void dedupKeysThroughNewCommitParts(
    const DataPartPtr & input_part,
    const SSTFileReader & input_sst,
    const MergeTreeData::DataPartsVector & all_visible_parts,
    const std::vector<VisiblePartSSTInfo> & visible_sst_infos,
    PartDeleteBitmapType & cross_part_delete_marks)
{
    if (visible_sst_infos.empty())
        return;

    /// Get total key count from SST properties — O(1), no scanning needed.
    auto props = input_sst.getProperties();
    const size_t total_keys = props ? props->num_entries : 0;
    if (total_keys == 0)
        return;

    /// For small key sets, fall back to single-threaded path to avoid thread overhead.
    const size_t num_threads = (total_keys < MIN_KEYS_FOR_PARALLEL) ? 1 : std::min(DEDUP_MAX_PARALLEL, total_keys);
    const size_t stride = total_keys / num_threads;

    /// One sequential scan to sample boundary keys for each shard.
    std::vector<std::string> boundary_keys;
    boundary_keys.reserve(num_threads);
    {
        Stopwatch boundary_scan_watch;
        rocksdb::ReadOptions opts;
        opts.fill_cache = false;
        auto scan_iter = input_sst.newIterator(opts);
        size_t idx = 0;
        for (scan_iter->SeekToFirst(); scan_iter->Valid(); scan_iter->Next(), ++idx)
        {
            if (idx % stride == 0 && boundary_keys.size() < num_threads)
                boundary_keys.push_back(scan_iter->key().ToString());
        }
        ProfileEvents::increment(ProfileEvents::UniqueKeyDedupBoundaryKeyScanMicroseconds, boundary_scan_watch.elapsedMicroseconds());
    }

    const size_t actual_shards = boundary_keys.size();
    if (actual_shards == 0)
        return;

    /// Allocate per-shard delta bitmaps.
    std::vector<DeltaBitmaps> delta_bitmaps(actual_shards);

    Stopwatch parallel_process_watch;
    {
        /// Unified parallel path: even for a single shard the thread pool
        /// overhead is negligible, and dedupKeyRange handles num_keys == 0
        /// with an early return so empty shards are essentially free.
        ThreadPool dedup_pool(
            CurrentMetrics::UniqueKeyDedupThreads,
            CurrentMetrics::UniqueKeyDedupThreadsActive,
            CurrentMetrics::UniqueKeyDedupThreadsScheduled,
            actual_shards);

        ThreadPoolCallbackRunnerLocal<void> runner(dedup_pool, ThreadName::UNIQUE_KEY_DEDUP);

        for (size_t shard = 0; shard < actual_shards; ++shard)
        {
            /// Last shard gets the remainder.
            size_t shard_keys = (shard == actual_shards - 1) ? (total_keys - stride * shard) : stride;

            runner.enqueueAndKeepTrack(
                [&input_part,
                 &input_sst,
                 &all_visible_parts,
                 &visible_sst_infos,
                 start_key = boundary_keys[shard],
                 shard_keys,
                 &delta_bitmap = delta_bitmaps[shard],
                 shard]
                {
                    dedupKeyRange(
                        input_part, input_sst, all_visible_parts, visible_sst_infos, start_key, shard_keys, delta_bitmap, shard);
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyDedupParallelProcessMicroseconds, parallel_process_watch.elapsedMicroseconds());

    /// Merge all per-shard delta bitmaps into the final output.
    for (auto & shard_delta : delta_bitmaps)
    {
        for (auto & [part_ptr, bitmap] : shard_delta)
        {
            if (!bitmap.isEmpty())
                cross_part_delete_marks[part_ptr] |= bitmap;
        }
    }

    /// Release ReadBuffer memory (~1MB per file) now that cross-part dedup is done.
    /// Bloom filter and block cache remain in the cached SSTFileReader.
    input_sst.releaseBufferMemory();
    for (const auto & info : visible_sst_infos)
    {
        if (info.reader)
            info.reader->releaseBufferMemory();
    }
}

/// Merge cross-part delete marks into each affected part's persistent delete mark bitmap.
/// Uses COW (Copy-On-Write): always creates a new bitmap object so that concurrent
/// readers holding a shared_ptr to the old bitmap are not affected.
static void applyCrossPartDeleteMarks(const PartDeleteBitmapType & cross_part_marks_map)
{
    for (const auto & [part_ptr, cross_part_marks] : cross_part_marks_map)
    {
        if (cross_part_marks.isEmpty())
            continue;

        /// COW: create a fresh bitmap, copy existing marks if any, then add new ones.
        auto new_marks = (part_ptr->rows_count <= std::numeric_limits<UInt32>::max())
            ? ProjectionIndexBitmap::create32()
            : ProjectionIndexBitmap::create64();

        auto existing_marks = part_ptr->getDeleteMarkBitmap();
        if (existing_marks)
            roaring_bitmap_or_inplace(new_marks->data.bitmap32, existing_marks->data.bitmap32);

        for (const auto it : cross_part_marks)
            new_marks->add(it);

        /// Atomically replace the pointer so readers see either the old or new bitmap, never a half-modified one.
        part_ptr->setDeleteMarkBitmap(new_marks);
    }
}

MergeTreeDedupPartManager::MergeTreeDedupPartManager(const MergeTreeData & storage_)
    : storage(storage_)
    , log(getLogger(fmt::format("{} (MergeTreeDedupPartManager)", storage.getStorageID().getNameForLogs())))
{
}

MergeTreeDedupPartManager::DedupSSTContext MergeTreeDedupPartManager::prepareSSTReadersForDedup(
    const DataPartPtr & input_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPartsVector & visible_parts)
{
    DedupSSTContext ctx;

    if (input_part->rows_count == 0)
        return ctx;

    /// Get cached SST reader for input part.
    ctx.input_sst = input_part->getOrOpenSSTReader(metadata_snapshot);
    if (!ctx.input_sst)
    {
        LOG_DEBUG(log, "part '{}' has no unique projection SST, skip dedup", input_part->name);
        return ctx;
    }

    /// Build visible part SST readers from cache.
    ctx.visible_sst_metas.reserve(visible_parts.size());
    for (size_t i = 0; i < visible_parts.size(); ++i)
    {
        auto reader = visible_parts[i]->getOrOpenSSTReader(metadata_snapshot);
        if (!reader)
            continue;
        ctx.visible_sst_metas.push_back({i, std::move(reader)});
    }

    return ctx;
}

ProjectionIndexBitmapPtr MergeTreeDedupPartManager::buildIntraPartDeleteMark(
    const DataPartPtr & input_part,
    const SSTFileReader & input_sst)
{
    auto props = input_sst.getProperties();
    size_t sst_entry_count = props ? props->num_entries : 0;

    /// All rows have distinct keys — no intra-part duplicates.
    if (sst_entry_count >= input_part->rows_count)
        return nullptr;

    /// Collect all surviving offsets (one per unique key) from the SST.
    auto input_part_delete_mark = ProjectionIndexBitmap::create32();
    {
        rocksdb::ReadOptions opts;
        opts.fill_cache = false;
        auto iter = input_sst.newIterator(opts);
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            if (iter->value().size() >= 8)
            {
                auto entry = decodeUniqueValueEntry(iter->value().data(), iter->value().size());
                input_part_delete_mark->add(static_cast<UInt32>(entry.part_offset));
            }
        }
    }

    /// Flip [0, rows_count): survivors become 0, losers become 1.
    input_part_delete_mark->flipRange(0, input_part->rows_count);

    if (input_part_delete_mark->empty())
        return nullptr;

    return input_part_delete_mark;
}

void MergeTreeDedupPartManager::optimizeVisiblePartsForMerge(
    const DataPartPtr & new_part,
    MergeTreeData::DataPartsVector & visible_parts)
{
    /// For merge-produced parts, filter out visible parts whose
    /// max_block < new_part's min_block. These older parts were already
    /// deduped against the merge's source parts during prior commits.
    std::erase_if(visible_parts, [&](const auto & part)
    {
        return part->info.max_block < new_part->info.min_block;
    });

    /// Check if covered parts' effective rows (rows - delete marks) sum
    /// equals new_part's rows. If so, the merge didn't introduce new key
    /// conflicts against remaining visible parts — we can skip cross-part
    /// dedup entirely by clearing visible_parts.
    size_t covered_effective_rows = 0;
    for (const auto & part : visible_parts)
    {
        if (part->info.min_block >= new_part->info.min_block
            && part->info.max_block <= new_part->info.max_block)
        {
            size_t deleted = 0;
            if (auto bm = part->getDeleteMarkBitmap())
                deleted = bm->cardinality();
            covered_effective_rows += (part->rows_count - deleted);
        }
    }

    if (covered_effective_rows == new_part->rows_count)
    {
        LOG_TRACE(log, "optimizeVisiblePartsForMerge: merge part '{}' effective rows match "
            "covered parts ({} rows), clearing visible parts to skip cross-part dedup",
            new_part->name, new_part->rows_count);
        visible_parts.clear();
    }
}

void MergeTreeDedupPartManager::dedupPart(const DataPartPtr & new_part, MergeTreeData::CommitOperation op)
{
    if (!new_part)
        return;

    delta_deleted_rows_map.clear();
    Stopwatch dedup_watch;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    MergeTreeData::DataPartsVector all_visible_parts;
    {
        /// Acquire a shared read lock to scan active parts in the same partition.
        auto shared_lock = storage.readLockParts();
        all_visible_parts = storage.getDataPartsVectorInPartitionForInternalUsage(
            {MergeTreeData::DataPartState::Active}, new_part->info.getPartitionId(), shared_lock);
    }

    /// Filter out the new part itself and empty parts.
    std::erase_if(all_visible_parts, [&](const auto & part)
    {
        return part->rows_count == 0 || part->name == new_part->name;
    });

    /// Apply per-operation optimizations that may reduce or clear visible_parts.
    if (op == MergeTreeData::CommitOperation::Merge)
        optimizeVisiblePartsForMerge(new_part, all_visible_parts);

    /// Open the input part's SST reader (from cache). Required for both
    /// cross-part and intra-part dedup.
    auto sst_ctx = prepareSSTReadersForDedup(new_part, metadata_snapshot, all_visible_parts);
    if (!sst_ctx.input_sst)
        return;

    /// Cross-part dedup: only needed when there are visible parts to compare against.
    /// When visible_parts is empty, skip boundary key scanning, shard partitioning,
    /// and all per-shard multiGet work entirely.
    if (!all_visible_parts.empty() && !sst_ctx.visible_sst_metas.empty())
    {
        PartDeleteBitmapType cross_part_delta;
        dedupKeysThroughNewCommitParts(
            new_part, *sst_ctx.input_sst, all_visible_parts, sst_ctx.visible_sst_metas,
            cross_part_delta);

        for (auto & [part_ptr, delete_marks] : cross_part_delta)
        {
            if (!delete_marks.isEmpty())
                delta_deleted_rows_map[part_ptr] |= delete_marks;
        }
    }

    /// Intra-part dedup: detect rows that lost to other rows with the same
    /// unique key during SST construction (last-write-wins).
    auto input_part_delete_mark = buildIntraPartDeleteMark(new_part, *sst_ctx.input_sst);
    if (input_part_delete_mark)
    {
        LOG_DEBUG(log, "dedupPart: part '{}' has {} intra-part duplicate rows",
            new_part->name, input_part_delete_mark->cardinality());
        roaring_bitmap_or_inplace(
            &delta_deleted_rows_map[new_part.get()].roaring, input_part_delete_mark->data.bitmap32);
    }

    LOG_DEBUG(log, "dedupPart: part '{}', op={}, visible_parts={}, elapsed={}us",
        new_part->name, static_cast<int>(op), all_visible_parts.size(), dedup_watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::commitDeleteMarkBuffers(const DataPartsLock & /*lock*/)
{
    applyCrossPartDeleteMarks(delta_deleted_rows_map);
    delta_deleted_rows_map.clear();
}

void MergeTreeDedupPartManager::buildAllDeleteMarksOnStartup()
{
    Stopwatch total_watch;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    /// Get all active parts.
    auto all_parts = storage.getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active});

    if (all_parts.empty())
    {
        return;
    }

    LOG_INFO(log, "buildAllDeleteMarksOnStartup: starting build for {} active parts", all_parts.size());

    /// Group parts by partition_id, skip empty parts.
    std::unordered_map<String, DataPartsVector> parts_by_partition;
    for (const auto & part : all_parts)
    {
        if (part->rows_count == 0)
            continue;
        parts_by_partition[part->info.getPartitionId()].push_back(part);
    }

    size_t dedup_count = 0;
    for (auto & [partition_id, partition_parts] : parts_by_partition)
    {
        /// Sort by max_block ascending so that older parts come first.
        /// Each part only deduplicates against preceding (older) parts.
        std::sort(partition_parts.begin(), partition_parts.end(),
            [](const DataPartPtr & a, const DataPartPtr & b)
            {
                return a->info.max_block < b->info.max_block;
            });

        for (size_t i = 0; i < partition_parts.size(); ++i)
        {
            const auto & current_part = partition_parts[i];

            /// Intra-part dedup first: detect rows that lost during SST construction.
            /// This must happen before cross-part dedup so that the current part's
            /// delete marks are visible to later rounds.
            auto sst_ctx_intra = prepareSSTReadersForDedup(current_part, metadata_snapshot, partition_parts /* unused for intra */);
            if (!sst_ctx_intra.input_sst)
                continue;

            auto input_part_delete_mark = buildIntraPartDeleteMark(current_part, *sst_ctx_intra.input_sst);
            if (input_part_delete_mark)
            {
                LOG_DEBUG(log, "buildAllDeleteMarksOnStartup: part '{}' has {} intra-part duplicate rows",
                    current_part->name, input_part_delete_mark->cardinality());

                PartDeleteBitmapType intra_delta;
                roaring_bitmap_or_inplace(
                    &intra_delta[current_part.get()].roaring, input_part_delete_mark->data.bitmap32);
                applyCrossPartDeleteMarks(intra_delta);
            }

            /// Cross-part dedup: only compare against older parts [0, i).
            /// The first part (i == 0) has no predecessors, so skip cross-part dedup.
            if (i > 0)
            {
                DataPartsVector older_parts(partition_parts.begin(), partition_parts.begin() + i);

                auto sst_ctx = prepareSSTReadersForDedup(current_part, metadata_snapshot, older_parts);
                if (sst_ctx.input_sst && !older_parts.empty() && !sst_ctx.visible_sst_metas.empty())
                {
                    PartDeleteBitmapType cross_part_delta;
                    dedupKeysThroughNewCommitParts(
                        current_part, *sst_ctx.input_sst, older_parts, sst_ctx.visible_sst_metas,
                        cross_part_delta);

                    /// Apply immediately so subsequent rounds see updated delete marks.
                    applyCrossPartDeleteMarks(cross_part_delta);
                }
            }

            ++dedup_count;
        }

        LOG_DEBUG(log, "buildAllDeleteMarksOnStartup: partition '{}', parts={}, dedup_rounds={}",
                  partition_id, partition_parts.size(), partition_parts.size());
    }

    LOG_INFO(log, "buildAllDeleteMarksOnStartup: complete, performed {} dedup rounds, total elapsed={}ms",
             dedup_count, total_watch.elapsedMilliseconds());
}

UniqueProcessLock::UniqueProcessLock(std::mutex & unique_process_lock_)
    : wait_watch(Stopwatch(CLOCK_MONOTONIC)), lock(unique_process_lock_), lock_watch(Stopwatch(CLOCK_MONOTONIC))
{
    ProfileEvents::increment(ProfileEvents::UniqueProcessLockWaitMicroseconds, wait_watch->elapsedMicroseconds());
}


UniqueProcessLock::~UniqueProcessLock()
{
    if (lock_watch.has_value())
        ProfileEvents::increment(ProfileEvents::UniqueProcessLockHoldMicroseconds, lock_watch->elapsedMicroseconds());
}
}
