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
#include <base/scope_guard.h>

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
    extern const int LOGICAL_ERROR;
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
    const DataPartPtr & part,
    const UniqueValueEntryFull & entry,
    size_t value_size)
{
    /// 16-byte values carry per-row version; 8-byte values use part-level max_block.
    return (value_size == 16) ? entry.version : static_cast<UInt64>(part->info.max_block);
}

inline void addToBitmap(ProjectionIndexBitmapPtr & bitmap, UInt64 value, size_t part_rows)
{
    if (!bitmap)
        bitmap = ProjectionIndexBitmap::create(part_rows);
    bitmap->add(value);
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

                auto input_version = getEffectiveVersion(input.part, input_entry, current_value.size());
                auto visible_version = getEffectiveVersion(current_part, current_entry, current_value.size());

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

/// Sample boundary keys from a single SST reader by scanning every `stride`-th key.
/// Returns up to `max_shards` boundary keys that partition the key space into roughly
/// equal-sized shards. Shared by INSERT and Merge/Mutation parallel dedup paths.
static std::vector<std::string> sampleBoundaryKeysFromSST(
    const SSTFileReaderPtr & reader,
    size_t total_keys,
    size_t max_shards)
{
    const size_t num_shards = (total_keys < MIN_KEYS_FOR_PARALLEL) ? 1 : std::min(max_shards, total_keys);
    const size_t stride = total_keys / num_shards;

    std::vector<std::string> boundary_keys;
    boundary_keys.reserve(num_shards);

    Stopwatch boundary_scan_watch;
    rocksdb::ReadOptions opts;
    opts.fill_cache = false;
    auto scan_iter = reader->newIterator(opts);
    size_t idx = 0;
    for (scan_iter->SeekToFirst(); scan_iter->Valid(); scan_iter->Next(), ++idx)
    {
        if (idx % stride == 0 && boundary_keys.size() < num_shards)
            boundary_keys.push_back(scan_iter->key().ToString());
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyDedupBoundaryKeyScanMicroseconds, boundary_scan_watch.elapsedMicroseconds());

    return boundary_keys;
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

    /// Release ReadBuffer memory (~1MB per file) when cross-part dedup is done.
    /// Bloom filter and index blocks remain pinned in the cached SSTFileReader.
    SCOPE_EXIT({
        input.releaseBufferMemory();
        MergeTreeDedupPartManager::releaseAllBufferMemory(visible_parts);
    });

    /// Get total key count from SST properties — O(1), no scanning needed.
    auto props = input.reader->getProperties();
    const size_t total_keys = props ? props->num_entries : 0;
    if (total_keys == 0)
        return result;

    auto boundary_keys = sampleBoundaryKeysFromSST(input.reader, total_keys, DEDUP_MAX_PARALLEL);

    const size_t actual_shards = boundary_keys.size();
    if (actual_shards == 0)
        return result;

    /// Recompute stride from actual_shards (which may differ from the
    /// requested max_shards when the SST has fewer keys than expected).
    const size_t stride = total_keys / actual_shards;

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
            /// Last shard processes all remaining keys until the end of the SST.
            /// Using SIZE_MAX lets the iterator naturally exhaust, which is safe
            /// because `deduplicateKeyByBucket` stops when the iterator is invalid.
            size_t shard_keys = (shard == actual_shards - 1) ? SIZE_MAX : stride;

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
    /// Include the input part as well — it may have rows marked as deleted
    /// when a visible part wins the version comparison.
    auto mergeDeltaBitmaps = [&](const IMergeTreeDataPart * part_ptr, size_t rows_count)
    {
        auto bitmap = ProjectionIndexBitmap::create(rows_count);
        for (const auto & shard_delta : delta_bitmaps)
        {
            auto it = shard_delta.find(part_ptr);
            if (it != shard_delta.end() && it->second && !it->second->empty())
                bitmap->unionWith(*it->second);
        }
        if (!bitmap->empty())
            result.emplace(part_ptr, std::move(bitmap));
    };

    for (const auto & [part, reader] : visible_parts)
        mergeDeltaBitmaps(part.get(), part->rows_count);

    /// Input part can also have rows deleted when visible parts win.
    mergeDeltaBitmaps(input.part.get(), input.part->rows_count);

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
        if (auto existing_marks = part_ptr->getDeleteMarkBitmap())
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
    const DataPartsVector & other_parts)
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

    /// Build SST readers for the other parts (visible parts or source parts) from cache.
    ctx.visible_parts = openSSTReadersForParts(other_parts, metadata_snapshot);
    return ctx;
}


void MergeTreeDedupPartManager::dedupForInsert(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & visible_parts)
{
    /// Open the input part's SST reader (from cache) for cross-part dedup.
    auto sst_ctx = prepareSSTReadersForDedup(new_part, metadata_snapshot, visible_parts);
    if (!sst_ctx.input.reader)
        return;

    /// Cross-part dedup against all visible parts.
    delta_deleted_rows_map = dedupKeysThroughNewCommitParts(sst_ctx.input, sst_ctx.visible_parts);
}

/// Propagate delete marks from source parts to the fetched part using a
/// parallel dual-iterator (two-pointer) approach.
///
/// The fetched part's SST key space is split into shards via boundary key
/// sampling. Each shard independently builds a SSTMergingIterator over all
/// source parts' SSTs and a plain iterator over the fetched part's SST,
/// then walks them in lockstep:
///
///   - Keys matching in both streams: check if the source entry is
///     delete-marked. If so, mark the corresponding row in the fetched part.
///   - Keys in the fetched part but not in source parts: impossible when
///     source parts fully cover the fetched part's block range (no gaps).
///   - Keys in source parts but not in the fetched part: these are losers
///     eliminated during merge — skip them.
///
/// Complexity: O((N_source * log(K) + N_fetched) / P) with P = parallelism,
/// where K = number of source parts.
/// Run the dual-iterator logic for a single shard of the key space.
/// `start_key` is the first key in this shard; `end_key` is the first key
/// of the *next* shard (empty means "until the end of the SST").
/// Produces a per-shard bitmap of fetched-part rows to mark as deleted.
static void propagateDeleteMarksByShard(
    const std::vector<PartWithSSTReader> & source_part_readers,
    const SSTFileReaderPtr & fetched_sst_reader,
    size_t fetched_part_rows,
    const std::string & start_key,
    const std::string & end_key,
    ProjectionIndexBitmapPtr & shard_bitmap,
    size_t shard_id,
    LoggerPtr log)
{
    Stopwatch shard_watch;

    rocksdb::ReadOptions opts;
    opts.fill_cache = false;

    /// Build a merging iterator over all source parts' SSTs for this shard.
    std::vector<std::unique_ptr<rocksdb::Iterator>> sst_iters;
    std::vector<SSTFileReaderPtr> sst_readers;
    std::vector<DataPartPtr> parts;

    for (const auto & [part, reader] : source_part_readers)
    {
        sst_iters.push_back(reader->newIterator(opts));
        sst_readers.push_back(reader);
        parts.push_back(part);
    }

    MergeTreeDedupPartManager::SSTMergingIterator merge_iter(std::move(sst_iters), std::move(sst_readers));
    merge_iter.seek(rocksdb::Slice(start_key));

    /// Build a plain iterator over the fetched part's SST for this shard.
    auto fetched_iter = fetched_sst_reader->newIterator(opts);
    fetched_iter->Seek(rocksdb::Slice(start_key));

    auto bitmap = ProjectionIndexBitmap::create(fetched_part_rows);
    size_t marked = 0;
    size_t source_keys_scanned = 0;
    size_t fetched_keys_scanned = 0;

    /// Helper: check if a key has crossed the shard boundary.
    auto beyondShardEnd = [&](const rocksdb::Slice & key) -> bool
    {
        return !end_key.empty() && key.compare(rocksdb::Slice(end_key)) >= 0;
    };

    /// Skip delete-marked source entries so merge_iter always points to
    /// a live (non-deleted) key when valid.
    auto skipDeletedSourceEntries = [&]()
    {
        while (merge_iter.valid())
        {
            if (beyondShardEnd(merge_iter.key()))
                break;

            size_t idx = merge_iter.currentIndex();
            auto val = merge_iter.value();
            auto entry = decodeUniqueValueEntry(val.data(), val.size());

            auto source_delete_mark = parts[idx]->getDeleteMarkBitmap();
            if (!source_delete_mark || !source_delete_mark->contains(entry.part_offset))
                break; /// Found a live entry, stop skipping.

            merge_iter.next();
            ++source_keys_scanned;
        }
    };

    /// Walk both iterators in lockstep within this shard's key range.
    ///
    /// Three cases:
    ///   source_key < fetched_key → source-only key (merge loser), skip.
    ///   source_key == fetched_key → key exists live in source, no action.
    ///   source_key > fetched_key → fetched key has no live source entry,
    ///       mark the fetched row as deleted.
    while (fetched_iter->Valid() && !beyondShardEnd(fetched_iter->key()))
    {
        skipDeletedSourceEntries();

        auto fetched_key = fetched_iter->key();
        bool merge_iter_in_range = merge_iter.valid() && !beyondShardEnd(merge_iter.key());

        if (merge_iter_in_range)
        {
            auto source_key = merge_iter.key();
            int cmp_result = source_key.compare(fetched_key);

            if (cmp_result < 0)
            {
                merge_iter.next();
                ++source_keys_scanned;
                continue;
            }

            if (cmp_result == 0)
            {
                merge_iter.next();
                ++source_keys_scanned;
                fetched_iter->Next();
                ++fetched_keys_scanned;
                continue;
            }
        }

        /// source_key > fetched_key, or merging iterator exhausted/out-of-range:
        /// this fetched key has no live source entry → mark as deleted.
        auto fetched_val = fetched_iter->value();
        if (!fetched_val.empty())
        {
            auto fetched_entry = decodeUniqueValueEntry(fetched_val.data(), fetched_val.size());
            bitmap->add(fetched_entry.part_offset);
            ++marked;
        }

        fetched_iter->Next();
        ++fetched_keys_scanned;
    }

    if (!bitmap->empty())
        shard_bitmap = std::move(bitmap);

    LOG_TEST(log,
        "propagateDeleteMarksByShard: shard={}, source_keys_scanned={}, fetched_keys_scanned={}, "
        "marked={}, elapsed={}us",
        shard_id, source_keys_scanned, fetched_keys_scanned, marked, shard_watch.elapsedMicroseconds());
}

/// Parallel version of delete mark propagation from source parts to the
/// fetched part using dual-iterator (merging iterator vs fetched iterator).
///
/// Splits the fetched part's SST key space into shards using boundary key
/// sampling, then runs the dual-iterator logic per shard in parallel threads.
/// Each shard produces a local bitmap; results are merged at the end.
///
/// Complexity: O((N_source * log(K) + N_fetched) / P) with P = parallelism,
/// where K = number of source parts.
static void propagateDeleteMarksByDualIterator(
    std::vector<PartWithSSTReader> & source_part_readers,
    const SSTFileReaderPtr & fetched_sst_reader,
    size_t fetched_part_rows,
    ProjectionIndexBitmapPtr & result_bitmap,
    LoggerPtr log)
{
    Stopwatch watch;

    /// Get total key count from SST properties — O(1), no scanning needed.
    auto props = fetched_sst_reader->getProperties();
    const size_t total_keys = props ? props->num_entries : 0;
    if (total_keys == 0)
        return;

    auto boundary_keys = sampleBoundaryKeysFromSST(fetched_sst_reader, total_keys, DEDUP_MAX_PARALLEL);
    const size_t actual_shards = boundary_keys.size();
    if (actual_shards == 0)
        return;

    /// Per-shard bitmaps.
    std::vector<ProjectionIndexBitmapPtr> shard_bitmaps(actual_shards);

    Stopwatch parallel_watch;
    {
        ThreadPool dedup_pool(
            CurrentMetrics::UniqueKeyDedupThreads,
            CurrentMetrics::UniqueKeyDedupThreadsActive,
            CurrentMetrics::UniqueKeyDedupThreadsScheduled,
            actual_shards);

        ThreadPoolCallbackRunnerLocal<void> runner(dedup_pool, ThreadName::UNIQUE_KEY_DEDUP);

        for (size_t shard = 0; shard < actual_shards; ++shard)
        {
            /// shard_end_key is the start of the next shard; empty for the last shard
            /// (meaning "until the end of the SST").
            std::string shard_end_key = (shard + 1 < actual_shards) ? boundary_keys[shard + 1] : std::string{};

            runner.enqueueAndKeepTrack(
                [&source_part_readers,
                 &fetched_sst_reader,
                 fetched_part_rows,
                 start_key = boundary_keys[shard],
                 end_key = std::move(shard_end_key),
                 &shard_bitmap = shard_bitmaps[shard],
                 shard,
                 log]
                {
                    propagateDeleteMarksByShard(
                        source_part_readers, fetched_sst_reader, fetched_part_rows,
                        start_key, end_key, shard_bitmap, shard, log);
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyDedupParallelProcessMicroseconds, parallel_watch.elapsedMicroseconds());

    /// Merge all per-shard bitmaps into the final result.
    auto merged = ProjectionIndexBitmap::create(fetched_part_rows);
    size_t total_marked = 0;
    for (auto & shard_bitmap : shard_bitmaps)
    {
        if (shard_bitmap && !shard_bitmap->empty())
        {
            total_marked += shard_bitmap->cardinality();
            merged->unionWith(*shard_bitmap);
        }
    }

    if (!merged->empty())
        result_bitmap = std::move(merged);

    LOG_DEBUG(log,
        "propagateDeleteMarksByDualIterator: shards={}, total_marked={}, elapsed={}us",
        actual_shards, total_marked, watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::dedupForFetch(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & source_parts,
    const DataPartsVector & all_visible_parts)
{
    /// Check if source parts fully cover the fetched part's block range.
    /// If there are gaps (some source parts are missing on this replica),
    /// fall back to full cross-part dedup.
    std::vector<MergeTreePartInfo> source_infos;
    source_infos.reserve(source_parts.size());
    for (const auto & part : source_parts)
        source_infos.push_back(part->info);

    bool has_gaps = !MergeTreePartInfo::areAllBlockNumbersCovered(new_part->info, std::move(source_infos));

    if (has_gaps)
    {
        /// Filter out source parts that are covered by the fetched part's
        /// block range — they overlap with the fetched part's data and
        /// should not participate in cross-part dedup.
        DataPartsVector filtered_visible_parts;
        filtered_visible_parts.reserve(all_visible_parts.size());
        for (const auto & part : all_visible_parts)
        {
            if (part->info.min_block >= new_part->info.min_block
                && part->info.max_block <= new_part->info.max_block)
                continue; /// Covered by the fetched part, skip.
            filtered_visible_parts.push_back(part);
        }

        LOG_DEBUG(log,
            "dedupForFetch: part '{}', source parts have block gaps, "
            "falling back to full cross-part dedup (visible_parts: {} -> {})",
            new_part->name, all_visible_parts.size(), filtered_visible_parts.size());
        dedupForInsert(new_part, metadata_snapshot, filtered_visible_parts);
        return;
    }

    /// No gaps — source parts fully cover the fetched part's block range.
    /// Use the optimized dual-iterator approach: only propagate delete marks
    /// from source parts, no need for full cross-part dedup.

    /// Quick row-count check (analogous to dedupForMerge):
    /// If the fetched part's row count matches the sum of source parts'
    /// effective rows (rows - delete marks), no concurrent INSERT has
    /// deleted any key in the source parts — nothing to propagate.
    size_t source_effective_rows = 0;
    for (const auto & part : source_parts)
    {
        size_t deleted = 0;
        if (auto delete_mark = part->getDeleteMarkBitmap())
            deleted = delete_mark->cardinality();
        source_effective_rows += (part->rows_count - deleted);
    }

    if (source_effective_rows == new_part->rows_count)
    {
        LOG_DEBUG(log,
            "dedupForFetch: part '{}' rows ({}) match source effective rows, skip",
            new_part->name, new_part->rows_count);
        return;
    }

    /// Quick check: if no source part has any delete marks, nothing to propagate.
    bool any_delete_marks = false;
    for (const auto & part : source_parts)
    {
        auto dm = part->getDeleteMarkBitmap();
        if (dm && !dm->empty())
        {
            any_delete_marks = true;
            break;
        }
    }

    if (!any_delete_marks)
    {
        LOG_DEBUG(log,
            "dedupForFetch: part '{}', source parts have no delete marks, skip",
            new_part->name);
        return;
    }

    /// Open SST readers for the fetched part and source parts.
    auto sst_ctx = prepareSSTReadersForDedup(new_part, metadata_snapshot, source_parts);
    if (!sst_ctx.input.reader)
        return;

    if (sst_ctx.visible_parts.empty())
    {
        LOG_DEBUG(log,
            "dedupForFetch: part '{}', no source parts with SST, skip",
            new_part->name);
        return;
    }

    SCOPE_EXIT({ sst_ctx.releaseBufferMemory(); });

    ProjectionIndexBitmapPtr result_bitmap;

    propagateDeleteMarksByDualIterator(
        sst_ctx.visible_parts,
        sst_ctx.input.reader, new_part->rows_count,
        result_bitmap, log);

    if (result_bitmap && !result_bitmap->empty())
        delta_deleted_rows_map[new_part.get()] = std::move(result_bitmap);
}

void MergeTreeDedupPartManager::dedupForMerge(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & source_parts,
    const MergeTreeData::DeleteMarkSnapshotMap & source_delete_mark_snapshots)
{
    /// Merge only combines existing data — it does not introduce new
    /// unique keys. The only dedup work needed is propagating delete marks:
    /// if a concurrent INSERT marked a row as deleted in a source part, the
    /// corresponding row in the merged part must also be marked.
    ///
    /// Source parts are pre-computed by `dedupPart` (covered by the merged
    /// part's block range).

    size_t source_effective_rows = 0;
    for (const auto & part : source_parts)
    {
        size_t deleted = 0;
        if (auto delete_mark = part->getDeleteMarkBitmap())
            deleted = delete_mark->cardinality();
        source_effective_rows += (part->rows_count - deleted);
    }

    /// If the merged part's row count matches the sum of source parts'
    /// effective rows (rows - delete marks), no concurrent INSERT has
    /// deleted any key in the source parts — nothing to propagate.
    if (source_effective_rows == new_part->rows_count)
    {
        LOG_TRACE(log, "dedupForMerge: part '{}' rows ({}) match source effective rows, skip",
            new_part->name, new_part->rows_count);
        return;
    }

    if (new_part->rows_count)
        tryDedupDeletedKeysFromSourceParts(new_part, source_parts, metadata_snapshot, source_delete_mark_snapshots);
}

void MergeTreeDedupPartManager::dedupForMutation(
    const DataPartPtr & new_part,
    const DataPartPtr & source_part)
{
    /// Mutation transforms exactly one source part into one new part.
    /// The source part (same block range, lower mutation version) is
    /// pre-identified by `dedupPart`.

    chassert(source_part);

    /// Mutation preserves row count, so row offsets are unchanged.
    /// Clone the delete mark bitmap directly from the source part.
    if (new_part->rows_count != source_part->rows_count)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "dedupForMutation: part '{}' has {} rows, but source part '{}' has {} rows",
            new_part->name, new_part->rows_count, source_part->name, source_part->rows_count);

    auto source_delete_mark = source_part->getDeleteMarkBitmap();
    if (!source_delete_mark || source_delete_mark->empty())
    {
        LOG_TRACE(log, "dedupForMutation: part '{}', source part '{}' has no delete marks, skip",
            new_part->name, source_part->name);
        return;
    }

    delta_deleted_rows_map[new_part.get()] = source_delete_mark;

    LOG_DEBUG(log, "dedupForMutation: part '{}', cloned {} delete marks from source part '{}'",
        new_part->name, source_delete_mark->cardinality(), source_part->name);
}

/// Propagate deleted keys from source parts to the merged part.
///
/// For each source part, scan its SST and collect keys whose `part_offset` is
/// in the diff bitmap (i.e. newly deleted by a concurrent INSERT). Then batch-
/// lookup those keys in the merged part's SST to find the new offsets and mark
/// them as deleted.
///
/// Why we don't need a multi-way merge iterator with "ALL entries deleted" logic:
/// Version transitivity guarantees correctness. If key K exists in source parts
/// A (version=5) and B (version=3), merge picks A as the winner. A concurrent
/// INSERT that deletes A's entry must have a higher version (>5), which also
/// beats B (>5 > 3), so B's entry is also deleted. Therefore, if the winner is
/// deleted, all losers are necessarily deleted too — checking any single source
/// part that has the key deleted is sufficient.
///
/// Duplicate keys collected from multiple source parts are harmless: the merged
/// part's SST contains each key exactly once, so multiGet deduplicates naturally.
static void propagateDeletedKeysPerSourcePart(
    std::vector<PartWithSSTReader> & source_part_readers,
    const std::vector<ProjectionIndexBitmapPtr> & diff_bitmaps,
    const SSTFileReaderPtr & merged_sst_reader,
    size_t merged_part_rows,
    ProjectionIndexBitmapPtr & result_bitmap,
    LoggerPtr log)
{
    Stopwatch watch;

    auto bitmap = ProjectionIndexBitmap::create(merged_part_rows);
    size_t marked = 0;
    size_t keys_collected = 0;

    /// Reusable batch buffer for streaming multiGet against the merged part.
    std::vector<std::string> key_batch;
    key_batch.reserve(MULTI_GET_BATCH_SIZE);

    /// Flush the current batch: look up keys in the merged part and mark deletions.
    auto flushBatch = [&]()
    {
        if (key_batch.empty())
            return;

        std::vector<rocksdb::Slice> key_slices;
        key_slices.reserve(key_batch.size());
        for (const auto & k : key_batch)
            key_slices.emplace_back(k);

        std::vector<std::string> values;
        auto statuses = merged_sst_reader->multiGet(key_slices, &values);

        for (size_t j = 0; j < statuses.size(); ++j)
        {
            if (!statuses[j].ok())
                continue;

            const auto & val = values[j];
            if (unlikely(val.empty()))
                continue;

            auto merged_entry = decodeUniqueValueEntry(val.data(), val.size());
            bitmap->add(merged_entry.part_offset);
            ++marked;
        }

        key_batch.clear();
    };

    /// Iterate each source part independently: scan its SST and collect keys
    /// whose part_offset is in the diff bitmap.
    rocksdb::ReadOptions opts;
    opts.fill_cache = false;

    for (size_t idx = 0; idx < source_part_readers.size(); ++idx)
    {
        const auto & diff = diff_bitmaps[idx];
        if (!diff || diff->empty())
            continue;

        auto iter = source_part_readers[idx].reader->newIterator(opts);
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            if (unlikely(!iter->status().ok()))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "SST iterator error during delete mark propagation: {}", iter->status().ToString());

            auto val = iter->value();
            if (unlikely(val.empty()))
                continue;

            auto entry = decodeUniqueValueEntry(val.data(), val.size());
            if (!diff->contains(entry.part_offset))
                continue;

            key_batch.push_back(iter->key().ToString());
            ++keys_collected;

            if (key_batch.size() >= MULTI_GET_BATCH_SIZE)
                flushBatch();
        }
    }

    /// Flush remaining keys.
    flushBatch();

    if (!bitmap->empty())
        result_bitmap = std::move(bitmap);

    LOG_TEST(log,
        "propagateDeletedKeysPerSourcePart: keys_collected={}, marked={}, elapsed={}us",
        keys_collected, marked, watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::tryDedupDeletedKeysFromSourceParts(
    const DataPartPtr & new_part,
    const DataPartsVector & source_parts,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData::DeleteMarkSnapshotMap & delete_mark_snapshots)
{
    Stopwatch watch;

    /// Open SST readers for the merged part and source parts.
    auto sst_ctx = prepareSSTReadersForDedup(new_part, metadata_snapshot, source_parts);
    if (!sst_ctx.input.reader)
        return;

    /// Ensure SST buffer memory is released on all exit paths.
    SCOPE_EXIT({ sst_ctx.releaseBufferMemory(); });

    auto & source_part_readers = sst_ctx.visible_parts;
    if (source_part_readers.empty())
    {
        LOG_DEBUG(log,
            "tryDedupDeletedKeysFromSourceParts: part '{}', no source parts with SST, skip, elapsed={}us",
            new_part->name, watch.elapsedMicroseconds());
        return;
    }

    /// Compute diff bitmaps: diff = current_delete_marks AND NOT snapshot_delete_marks.
    /// Only entries in the diff were newly deleted by concurrent INSERTs during merge/mutation.
    /// When snapshots are not available (e.g. startup rebuild), all current delete marks
    /// are treated as "new" (diff = full delete marks).
    /// Index matches source_part_readers.
    std::vector<ProjectionIndexBitmapPtr> diff_bitmaps;
    diff_bitmaps.reserve(source_part_readers.size());
    size_t total_diff_count = 0;

    for (const auto & [part, reader] : source_part_readers)
    {
        auto delete_mark = part->getDeleteMarkBitmap();
        if (!delete_mark || delete_mark->empty())
        {
            diff_bitmaps.push_back(nullptr);
            continue;
        }

        auto it = delete_mark_snapshots.find(part->name);
        if (it == delete_mark_snapshots.end() || !it->second)
        {
            /// No snapshot for this part — all current delete marks are "new".
            total_diff_count += delete_mark->cardinality();
            diff_bitmaps.push_back(std::move(delete_mark));
        }
        else
        {
            /// Compute diff = current AND NOT snapshot.
            auto diff = ProjectionIndexBitmap::create(part->rows_count);
            diff->unionWith(*delete_mark);
            diff->andNotWith(*it->second);

            if (!diff->empty())
                total_diff_count += diff->cardinality();

            diff_bitmaps.push_back(std::move(diff));
        }
    }

    /// If no diffs at all, no concurrent INSERT deleted any key — skip entirely.
    if (total_diff_count == 0)
    {
        LOG_DEBUG(log,
            "tryDedupDeletedKeysFromSourceParts: part '{}', no diff delete marks, skip, elapsed={}us",
            new_part->name, watch.elapsedMicroseconds());
        return;
    }

    /// For each source part, scan its SST for keys whose offsets are in the diff
    /// bitmap (newly deleted by concurrent INSERTs), then look them up in the
    /// merged part's SST to propagate the deletion.
    ProjectionIndexBitmapPtr result_bitmap;

    propagateDeletedKeysPerSourcePart(
        source_part_readers, diff_bitmaps,
        sst_ctx.input.reader, new_part->rows_count,
        result_bitmap, log);

    if (result_bitmap && !result_bitmap->empty())
        delta_deleted_rows_map[new_part.get()] = std::move(result_bitmap);

    LOG_DEBUG(log,
        "tryDedupDeletedKeysFromSourceParts: part '{}', source_parts={}, diff_count={}, elapsed={}us",
        new_part->name, source_part_readers.size(), total_diff_count, watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::dedupPart(
    const DataPartPtr & new_part,
    const std::optional<MergeTreeData::DeleteMarkSnapshotMap> & source_delete_mark_snapshots)
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

    /// Infer operation type from part metadata and context:
    ///
    ///   level == 0 + mutation == 0                              → Insert  (full cross-part dedup)
    ///   level == 0 + mutation > 0                               → Mutation of level-0 part (clone delete marks)
    ///   level > 0 + mutation source found                       → Mutation (clone source delete marks)
    ///   level > 0 + has snapshots (has_value) + no mut. source  → Merge   (propagate source delete marks)
    ///   level > 0 + no snapshots (nullopt) + no mut. source     → Fetch   (full cross-part dedup)
    ///
    /// Source parts are collected in a single scan of visible_parts:
    ///   - source_parts: all parts covered by the new part's block range
    ///     (used by merge to propagate delete marks).
    ///   - mutation_source: a part with the exact same block range but lower
    ///     mutation version (the hallmark of a local mutation).

    const auto & info = new_part->info;

    DataPartsVector source_parts;
    DataPartPtr mutation_source;

    if (info.level > 0)
    {
        for (const auto & part : all_visible_parts)
        {
            if (part->info.min_block >= info.min_block
                && part->info.max_block <= info.max_block)
            {
                source_parts.push_back(part);

                /// Mutation source: exact same block range, lower mutation version.
                if (!mutation_source
                    && info.mutation > 0
                    && part->info.min_block == info.min_block
                    && part->info.max_block == info.max_block
                    && part->info.mutation < info.mutation)
                {
                    mutation_source = part;
                }
            }
        }
    }

    const char * op_name = "insert";

    if (info.level == 0 && info.mutation == 0)
    {
        /// Level-0 part with no mutation version — a fresh INSERT.
        dedupForInsert(new_part, metadata_snapshot, all_visible_parts);
    }
    else if (info.mutation > 0 && (info.level == 0 || mutation_source))
    {
        /// Part has a mutation version AND either:
        ///   - level == 0 (mutation of an insert part — find source inline), or
        ///   - a source part with the same block range exists (local mutation).
        /// Clone the source part's delete marks.
        op_name = "mutation";

        if (!mutation_source)
        {
            /// Level-0 mutation: source has the same block range at level 0.
            for (const auto & part : all_visible_parts)
            {
                if (part->info.min_block == info.min_block
                    && part->info.max_block == info.max_block
                    && part->info.mutation < info.mutation)
                {
                    mutation_source = part;
                    break;
                }
            }
        }

        if (!mutation_source)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "dedupPart: mutation part '{}' has no matching source part in visible_parts", new_part->name);

        dedupForMutation(new_part, mutation_source);
    }
    else if (source_delete_mark_snapshots.has_value())
    {
        /// Has delete mark snapshots — this is a locally-produced merge.
        /// The map may be empty when none of the source parts had delete marks,
        /// but has_value() == true distinguishes it from a fetched part.
        op_name = "merge";
        dedupForMerge(new_part, metadata_snapshot, source_parts, *source_delete_mark_snapshots);
    }
    else
    {
        /// Level > 0, no snapshots, no matching source part — a fetched part.
        /// Try optimized dual-iterator path; falls back to full dedup if
        /// source parts have block gaps.
        op_name = "fetch";
        dedupForFetch(new_part, metadata_snapshot, source_parts, all_visible_parts);
    }

    LOG_DEBUG(log, "dedupPart: part '{}', op={}, visible_parts={}, elapsed={}us",
        new_part->name, op_name, all_visible_parts.size(), dedup_watch.elapsedMicroseconds());
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

void MergeTreeDedupPartManager::SSTMergingIterator::seek(const rocksdb::Slice & target)
{
    /// Rebuild the heap: each SST iterator uses O(log N) binary search
    /// on index/data blocks, so total cost is O(K * log N_max + K * log K)
    /// where K = number of iterators.
    min_heap = MinHeap(Comparator(&iters));
    for (size_t i = 0; i < iters.size(); ++i)
    {
        iters[i]->Seek(target);
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
/// Cross-part dedup for a single partition via multi-way merge: walks all
/// SST iterators in key order. When the same key appears in multiple parts,
/// the entry with the highest version wins (ties broken by newer part).
/// Losers are marked as deleted immediately during the walk.
///
/// Intra-part dedup is unnecessary because:
/// - INSERT path already deduplicates the block via `deduplicateBlockByUniqueKey`.
/// - Merge path filters out delete-marked rows via `_row_exists` column,
///   so merged parts never contain duplicate keys.

void MergeTreeDedupPartManager::buildAllDeleteMarksForPartition(
    const DataPartsVector & partition_parts,
    const StorageMetadataPtr & metadata_snapshot)
{
    auto part_readers = openSSTReadersForParts(partition_parts, metadata_snapshot);
    if (part_readers.empty())
        return;

    /// Ensure SST buffer memory is released when done.
    SCOPE_EXIT({ releaseAllBufferMemory(part_readers); });

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
        auto entry = decodeUniqueValueEntry(val.data(), val.size());
        size_t idx = merge_iter.currentIndex();

        /// Cross-part dedup: compare entries sharing the same key.
        auto current_key = merge_iter.key();
        UInt64 current_version = getEffectiveVersion(parts[idx], entry, val.size());

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
    {
        const size_t num_partitions = parts_by_partition.size();
        const size_t pool_size = std::min(num_partitions, DEDUP_MAX_PARALLEL);

        ThreadPool partition_pool(
            CurrentMetrics::UniqueKeyDedupThreads,
            CurrentMetrics::UniqueKeyDedupThreadsActive,
            CurrentMetrics::UniqueKeyDedupThreadsScheduled,
            pool_size);

        ThreadPoolCallbackRunnerLocal<void> runner(partition_pool, ThreadName::UNIQUE_KEY_DEDUP);

        for (auto & [partition_id, partition_parts] : parts_by_partition)
        {
            total_parts_deduped += partition_parts.size();
            runner.enqueueAndKeepTrack(
                [this, &partition_parts, &metadata_snapshot, partition_id]
                {
                    buildAllDeleteMarksForPartition(partition_parts, metadata_snapshot);
                    LOG_DEBUG(log, "buildAllDeleteMarksOnStartup: partition '{}', parts={}",
                              partition_id, partition_parts.size());
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
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
