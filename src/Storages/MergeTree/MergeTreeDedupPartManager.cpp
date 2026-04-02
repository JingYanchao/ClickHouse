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

/// Maximum number of keys to batch in a single MultiGet call.
static constexpr size_t MULTI_GET_BATCH_SIZE = 16;

using PartWithSSTReader = MergeTreeDedupPartManager::PartWithSSTReader;

/// Retrieve the effective version for a part/entry pair.
/// When row-level versioning is available (16-byte SST values), use the per-row
/// version stored in the entry; otherwise fall back to part-level max_block.
static UInt64 getEffectiveVersion(
    bool has_row_version,
    const DataPartPtr & part,
    const UniqueValueEntryFull & entry)
{
    return has_row_version ? entry.version : static_cast<UInt64>(part->info.max_block);
}

inline void addToBitmap(ProjectionIndexBitmapPtr & bitmap, UInt64 value, size_t part_rows)
{
    if (!bitmap)
        bitmap = ProjectionIndexBitmap::create(part_rows);
    bitmap->add(value);
}


/// Detect whether the SST uses row-level versioning (16-byte values) or
/// part-level versioning (8-byte values). Returns true for row-level.
static bool detectRowVersioning(size_t value_size)
{
    return value_size == 16;
}

static void deduplicateKeyByBucket(
    const PartWithSSTReader & input,
    const std::vector<PartWithSSTReader> & visible_parts,
    const std::string & start_key,
    size_t num_keys,
    PartDeleteBitmapMap & delta_bitmaps,
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
    opts.fill_cache = false;
    auto input_iter = input.reader->newIterator(opts);
    input_iter->Seek(rocksdb::Slice(start_key));

    /// Detect once whether the SST uses row-level versioning (16-byte values)
    /// or part-level versioning (8-byte values), via `detectRowVersioning`.
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
                has_row_version = detectRowVersioning(value_slice.size());
                version_detected = true;
            }

            batch.push_back({input_iter->key().ToString(), input_entry});
        }

        if (batch.empty())
            break;

        /// Phase 2: For each visible part, batch-lookup all unmatched keys.
        /// Reset matched flags for this batch.
        matched.assign(batch.size(), false);

        for (const auto & visible : visible_parts)
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
            auto statuses = visible.reader->multiGet(candidate_keys, &values);

            /// Process results — compare versions and mark losers.
            const auto & current_part = visible.part;
            auto current_delete_bitmap = current_part->getDeleteMarkBitmap();

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

                auto input_version = getEffectiveVersion(has_row_version, input.part, input_entry);
                auto visible_version = getEffectiveVersion(has_row_version, current_part, current_entry);

                /// Determine the winner: higher version wins.
                /// When versions are equal, use `max_block` as tiebreaker —
                /// the part with a larger `max_block` is newer and should win.
                /// When `max_block` is also equal (e.g. same part compared
                /// against itself, which should not happen), the input part
                /// wins because it is the one being committed.
                bool input_wins = (input_version > visible_version)
                    || (input_version == visible_version
                        && input.part->info.max_block >= current_part->info.max_block);

                if (input_wins)
                {
                    /// Input part wins — mark the visible part's row as deleted.
                    addToBitmap(delta_bitmaps[current_part.get()], current_entry.part_offset, current_part->rows_count);
                }
                else
                {
                    /// Visible part wins — mark the input part's row as deleted.
                    addToBitmap(delta_bitmaps[input.part.get()], input_entry.part_offset, input.part->rows_count);
                }

                /// Only one visible part can own a given unique key.
                matched[batch_idx] = true;
            }
        }
    }

    LOG_TEST(
        getLogger("deduplicateKeyByBucket"),
        "shard={}, num_keys={}, processed={}, elapsed={}us",
        shard_id, num_keys, processed, shard_watch.elapsedMicroseconds());
}

/// Deduplicate keys from the input part against visible parts using parallel threads.
/// Splits the input SST key space into shards, processes each in a separate thread,
/// then merges per-shard delta bitmaps into a result map.
/// Returns a map from part pointer to the computed delete mark bitmap.
static PartDeleteBitmapMap dedupKeysThroughNewCommitParts(
    const PartWithSSTReader & input,
    const std::vector<PartWithSSTReader> & visible_parts)
{
    PartDeleteBitmapMap result;

    if (visible_parts.empty())
        return result;

    /// Get total key count from SST properties — O(1), no scanning needed.
    auto props = input.reader->getProperties();
    const size_t total_keys = props ? props->num_entries : 0;
    if (total_keys == 0)
        return result;

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
        auto scan_iter = input.reader->newIterator(opts);
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
        return result;

    /// Allocate per-shard delta bitmaps.
    std::vector<PartDeleteBitmapMap> delta_bitmaps(actual_shards);
    Stopwatch parallel_process_watch;
    {
        /// Unified parallel path: even for a single shard the thread pool
        /// overhead is negligible, and deduplicateByUniqueKey handles num_keys == 0
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
                [&input,
                 &visible_parts,
                 start_key = boundary_keys[shard],
                 shard_keys,
                 &delta_bitmap = delta_bitmaps[shard],
                 shard]
                {
                    deduplicateKeyByBucket(
                        input, visible_parts, start_key, shard_keys, delta_bitmap, shard);
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyDedupParallelProcessMicroseconds, parallel_process_watch.elapsedMicroseconds());

    /// Merge all per-shard delta bitmaps into the final output.
    /// Iterate by part: for each part, create a new bitmap by merging all shard deltas.
    for (const auto & visible : visible_parts)
    {
        const auto * part_ptr = visible.part.get();
        auto bitmap = ProjectionIndexBitmap::create(visible.part->rows_count);

        for (const auto & shard_delta : delta_bitmaps)
        {
            auto it = shard_delta.find(part_ptr);
            if (it != shard_delta.end() && it->second && !it->second->empty())
                bitmap->unionWith(*it->second);
        }

        if (!bitmap->empty())
            result.emplace(part_ptr, std::move(bitmap));
    }

    /// Release ReadBuffer memory (~1MB per file) now that cross-part dedup is done.
    /// Bloom filter and index blocks remain pinned in the cached SSTFileReader.
    input.reader->releaseBufferMemory();
    for (const auto & visible : visible_parts)
    {
        if (visible.reader)
            visible.reader->releaseBufferMemory();
    }

    return result;
}

/// Merge cross-part delete marks into each affected part's persistent delete mark bitmap.
/// Both writes (commitDeleteMarkBuffers under DataPartsLock) and reads (createStorageSnapshot
/// under readLockParts) are serialized by the parts lock, so there is no data race.
/// We still use COW (Copy-On-Write) — always creating a new bitmap object — so that
/// snapshot readers holding a shared_ptr to the old bitmap are not affected by later updates.
static void applyCrossPartDeleteMarks(const PartDeleteBitmapMap & cross_part_marks_map)
{
    for (const auto & [part_ptr, cross_part_marks] : cross_part_marks_map)
    {
        if (!cross_part_marks || cross_part_marks->empty())
            continue;

        /// COW: create a fresh bitmap, copy existing marks if any, then add new ones.
        auto new_marks = ProjectionIndexBitmap::create(part_ptr->rows_count);
        auto existing_marks = part_ptr->getDeleteMarkBitmap();
        if (existing_marks)
            new_marks->unionWith(*existing_marks);

        new_marks->unionWith(*cross_part_marks);

        /// Replace the bitmap pointer. Readers that already captured the old shared_ptr
        /// in a snapshot continue to use it; new snapshots will pick up the updated bitmap.
        part_ptr->setDeleteMarkBitmap(new_marks);
    }
}

MergeTreeDedupPartManager::MergeTreeDedupPartManager(const MergeTreeData & storage_)
    : storage(storage_)
    , log(getLogger(fmt::format("{} (MergeTreeDedupPartManager)", storage.getStorageID().getNameForLogs())))
{
}

std::vector<MergeTreeDedupPartManager::PartWithSSTReader> MergeTreeDedupPartManager::openSSTReadersForParts(
    const DataPartsVector & parts,
    const StorageMetadataPtr & metadata_snapshot)
{
    std::vector<PartWithSSTReader> result;
    result.reserve(parts.size());
    for (const auto & part : parts)
    {
        auto reader = part->getOrOpenSSTReader(metadata_snapshot);
        if (!reader)
            continue;
        result.push_back({part, std::move(reader)});
    }
    return result;
}

MergeTreeDedupPartManager::DedupPartWithSSTReaders MergeTreeDedupPartManager::prepareSSTReadersForDedup(
    const DataPartPtr & input_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPartsVector & visible_parts)
{
    DedupPartWithSSTReaders ctx;

    if (input_part->rows_count == 0)
        return ctx;

    /// Get cached SST reader for input part.
    auto input_reader = input_part->getOrOpenSSTReader(metadata_snapshot);
    if (!input_reader)
    {
        LOG_DEBUG(log, "part '{}' has no unique projection SST, skip dedup", input_part->name);
        return ctx;
    }
    ctx.input = {input_part, std::move(input_reader)};

    /// Build visible part SST readers from cache.
    ctx.visible_parts = openSSTReadersForParts(visible_parts, metadata_snapshot);
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
                input_part_delete_mark->add(entry.part_offset);
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
    /// Always remove parts that are covered by the merged part (i.e. the
    /// merge's source parts).  At the time `dedupPart` runs, source parts
    /// are still Active, but they will become Outdated once the merge
    /// transaction commits.  If we let them participate in cross-part
    /// dedup, the merged part and a source part can share the same
    /// `max_block` (the merged part inherits the largest source
    /// `max_block`), causing the version-comparison tie-breaker to
    /// potentially let the source part win and mark the merged part's row
    /// as deleted.  After commit the source part becomes Outdated, so the
    /// row is lost from all Active parts — leading to fewer rows than
    /// expected.
    ///
    /// Also filter out parts whose max_block < new_part's min_block:
    /// these older parts were already deduped against the merge's source
    /// parts during prior commits and cannot conflict.
    ///
    /// While filtering, accumulate the effective rows (rows - delete marks)
    /// of covered source parts. If the sum equals new_part's rows, the
    /// merge did not introduce any new key conflicts against the remaining
    /// visible parts, so we can skip cross-part dedup entirely.
    size_t covered_effective_rows = 0;
    std::erase_if(visible_parts, [&](const auto & part)
    {
        /// Covered by the merged part (source parts).
        if (part->info.min_block >= new_part->info.min_block
            && part->info.max_block <= new_part->info.max_block)
        {
            size_t deleted = 0;
            if (auto bm = part->getDeleteMarkBitmap())
                deleted = bm->cardinality();
            covered_effective_rows += (part->rows_count - deleted);
            return true;
        }

        /// Older parts that cannot conflict.
        if (part->info.max_block < new_part->info.min_block)
            return true;

        return false;
    });

    /// If the covered source parts' effective rows match the merged part's
    /// row count, the merge preserved all unique keys without introducing
    /// new conflicts — skip cross-part dedup against remaining visible parts.
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
    if (!sst_ctx.input.reader)
        return;

    /// Cross-part dedup: only needed when there are visible parts to compare against.
    /// When visible_parts is empty, skip boundary key scanning, shard partitioning,
    /// and all per-shard multiGet work entirely.
    if (!sst_ctx.visible_parts.empty())
    {
        auto cross_part_marks = dedupKeysThroughNewCommitParts(sst_ctx.input, sst_ctx.visible_parts);
        /// Merge cross-part marks into delta_deleted_rows_map.
        for (auto & [part_ptr, bitmap] : cross_part_marks)
        {
            if (bitmap && !bitmap->empty())
            {
                auto & target = delta_deleted_rows_map[part_ptr];
                if (!target)
                    target = ProjectionIndexBitmap::create(part_ptr->rows_count);
                target->unionWith(*bitmap);
            }
        }
    }

    /// Intra-part dedup: detect rows that lost to other rows with the same
    /// unique key during SST construction (last-write-wins).
    if (auto input_part_delete_mark = buildIntraPartDeleteMark(new_part, *sst_ctx.input.reader))
    {
        LOG_DEBUG(log, "dedupPart: part '{}' has {} intra-part duplicate rows",
            new_part->name, input_part_delete_mark->cardinality());
        auto & intra_target = delta_deleted_rows_map[new_part.get()];
        if (!intra_target)
            intra_target = ProjectionIndexBitmap::create(new_part->rows_count);
        intra_target->unionWith(*input_part_delete_mark);
    }

    LOG_DEBUG(log, "dedupPart: part '{}', op={}, visible_parts={}, elapsed={}us",
        new_part->name, static_cast<int>(op), all_visible_parts.size(), dedup_watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::commitDeleteMarkBuffers(const DataPartsLock & /*lock*/)
{
    applyCrossPartDeleteMarks(delta_deleted_rows_map);
    delta_deleted_rows_map.clear();
}

/// ---- SSTMergingIterator implementation ----

MergeTreeDedupPartManager::SSTMergingIterator::SSTMergingIterator(
    std::vector<std::unique_ptr<rocksdb::Iterator>> iters_,
    std::vector<SSTFileReaderPtr> readers_)
    : iters(std::move(iters_))
    , readers(std::move(readers_))
    , min_heap(Comparator(&iters))
{
}

void MergeTreeDedupPartManager::SSTMergingIterator::seekToFirst()
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

void MergeTreeDedupPartManager::SSTMergingIterator::next()
{
    chassert(valid());
    auto idx = min_heap.top();
    min_heap.pop();
    iters[idx]->Next();
    if (iters[idx]->Valid() && !iters[idx]->key().empty())
        min_heap.push(idx);
}

/// ---- buildAllDeleteMarksForPartition ----
///
/// Two-step dedup for a single partition:
///
/// 1. Cross-part dedup via multi-way merge: walks all SST iterators in key
///    order. When the same key appears in multiple parts, the entry with the
///    highest version wins (ties broken by newer part). Losers are marked as
///    deleted immediately during the walk.
///
/// 2. Intra-part dedup: reuses buildIntraPartDeleteMark for each part to
///    find rows that lost during SST construction (last-write-wins). These
///    rows' offsets do NOT appear in the SST and are discovered via flip.

void MergeTreeDedupPartManager::buildAllDeleteMarksForPartition(
    const DataPartsVector & partition_parts,
    const StorageMetadataPtr & metadata_snapshot)
{
    auto part_readers = openSSTReadersForParts(partition_parts, metadata_snapshot);
    if (part_readers.empty())
        return;

    /// Step 1: Build the merging iterator over all parts' SST files.
    std::vector<std::unique_ptr<rocksdb::Iterator>> sst_iters;
    std::vector<SSTFileReaderPtr> sst_readers;
    std::vector<DataPartPtr> parts;

    rocksdb::ReadOptions opts;
    opts.fill_cache = false;

    for (const auto & [part, reader] : part_readers)
    {
        sst_iters.push_back(reader->newIterator(opts));
        sst_readers.push_back(reader);
        parts.push_back(part);
    }

    SSTMergingIterator merge_iter(std::move(sst_iters), std::move(sst_readers));
    merge_iter.seekToFirst();

    bool has_row_version = false;
    bool version_detected = false;

    /// Tracks the entry with the highest version for the current key group.
    struct IteratorEntryInfo
    {
        size_t part_index = 0;
        UniqueValueEntryFull entry{};
        UInt64 version = 0;
    };

    std::string last_key;
    IteratorEntryInfo last_max_entry_info{};

    /// Step 2: Walk the merged stream — cross-part dedup in a single pass.
    for (; merge_iter.valid(); merge_iter.next())
    {
        auto val = merge_iter.value();
        if (!version_detected)
        {
            has_row_version = detectRowVersioning(val.size());
            version_detected = true;
        }

        auto entry = decodeUniqueValueEntry(val.data(), val.size());
        size_t idx = merge_iter.currentIndex();

        /// Cross-part dedup: compare entries sharing the same key.
        auto current_key = merge_iter.key();
        UInt64 current_version = getEffectiveVersion(has_row_version, parts[idx], entry);

        if (last_key != current_key.ToString())
        {
            /// New key group — this entry becomes the current max.
            last_key = current_key.ToString();
            last_max_entry_info = {idx, entry, current_version};
        }
        else
        {
            if (current_version > last_max_entry_info.version
                || (current_version == last_max_entry_info.version
                    && parts[idx]->info.max_block >= parts[last_max_entry_info.part_index]->info.max_block))
            {
                /// Current entry has higher version — mark the previous max as deleted.
                parts[last_max_entry_info.part_index]->checkOrCreateDeleteMark()
                    ->add(last_max_entry_info.entry.part_offset);
                last_max_entry_info = {idx, entry, current_version};
            }
            else
            {
                /// Current entry loses — mark it as deleted.
                parts[idx]->checkOrCreateDeleteMark()
                    ->add(entry.part_offset);
            }
        }
    }

    /// Step 3: Intra-part dedup — reuse buildIntraPartDeleteMark for each part.
    for (const auto & [part, reader] : part_readers)
    {
        if (auto intra_mark = buildIntraPartDeleteMark(part, *reader))
        {
            LOG_DEBUG(log, "buildAllDeleteMarksForPartition: part '{}' has {} intra-part duplicate rows",
                part->name, intra_mark->cardinality());
            part->checkOrCreateDeleteMark()->unionWith(*intra_mark);
        }
    }

    /// Release ReadBuffer memory for all SST readers in this partition.
    for (const auto & [part, reader] : part_readers)
    {
        if (reader)
            reader->releaseBufferMemory();
    }
}

void MergeTreeDedupPartManager::buildAllDeleteMarksOnStartup()
{
    Stopwatch total_watch;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    /// Get all active parts.
    auto all_parts = storage.getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active});

    if (all_parts.empty())
        return;

    LOG_INFO(log, "buildAllDeleteMarksOnStartup: starting build for {} active parts", all_parts.size());

    /// Group parts by partition_id, skip empty parts.
    std::unordered_map<String, DataPartsVector> parts_by_partition;
    for (const auto & part : all_parts)
    {
        if (part->rows_count == 0)
            continue;
        parts_by_partition[part->info.getPartitionId()].push_back(part);
    }

    size_t total_parts_deduped = 0;
    for (auto & [partition_id, partition_parts] : parts_by_partition)
    {
        buildAllDeleteMarksForPartition(partition_parts, metadata_snapshot);
        total_parts_deduped += partition_parts.size();

        LOG_DEBUG(log, "buildAllDeleteMarksOnStartup: partition '{}', parts={}",
                  partition_id, partition_parts.size());
    }

    LOG_INFO(log, "buildAllDeleteMarksOnStartup: complete, deduped {} parts across {} partitions, total elapsed={}ms",
             total_parts_deduped, parts_by_partition.size(), total_watch.elapsedMilliseconds());
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
