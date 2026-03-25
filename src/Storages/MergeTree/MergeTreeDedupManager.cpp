#include <Storages/MergeTree/MergeTreeDedupManager.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric MergeTreePartsCleanerThreads;
    extern const Metric MergeTreePartsCleanerThreadsActive;
    extern const Metric MergeTreePartsCleanerThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event UniqueProcessLockWaitMicroseconds;
    extern const Event UniqueProcessLockHoldMicroseconds;
    extern const Event UniqueKeyDeduplicateSkipPartsByPart;
    extern const Event UniqueKeyDeduplicateSkipPartsByKey;
    extern const Event UniqueKeyDeduplicateZeroSkip;
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
static constexpr size_t MIN_KEYS_FOR_PARALLEL = 1000;

/// Information about visible parts that have valid SST readers, shared (read-only) across threads.
struct VisiblePartInfo
{
    size_t index;
    SSTFileReaderPtr reader;
};

/// Per-thread delta bitmap: records rows to be deleted for each affected part.
using DeltaBitmaps = PartDeleteBitmapType;

/// Process a range of keys from the input SST (from iterator positioned at start, for `num_keys` entries)
/// and record dedup losers into thread-local delta_bitmaps.
static void dedupKeyRange(
    const DataPartPtr & input_part,
    const SSTFileReader & input_sst,
    const MergeTreeData::DataPartsVector & all_visible_parts,
    const std::vector<VisiblePartInfo> & visible_part_infos,
    const std::string & start_key,
    size_t num_keys,
    DeltaBitmaps & delta_bitmaps,
    std::atomic<size_t> & skip_by_key_counter)
{
    auto get_version = [](const DataPartPtr & part) -> UInt64
    {
        return static_cast<UInt64>(part->info.max_block);
    };

    rocksdb::ReadOptions opts;
    opts.fill_cache = true;
    auto input_iter = input_sst.newIterator(opts);
    input_iter->Seek(rocksdb::Slice(start_key));

    size_t processed = 0;
    for (; input_iter->Valid() && processed < num_keys; input_iter->Next(), ++processed)
    {
        if (unlikely(!input_iter->status().ok()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper new parts iterator has error {}", input_iter->status().ToString());

        if (unlikely(input_iter->key().empty() || input_iter->value().empty()))
            continue;

        const auto & value_slice = input_iter->value();
        auto input_entry = UniqueValueEntry::decode(value_slice.data(), value_slice.size());

        auto key = input_iter->key();
        const auto input_version = get_version(input_part);

        for (const auto & info : visible_part_infos)
        {
            if (!info.reader->mayContainKey(std::string_view(key.data(), key.size())))
            {
                skip_by_key_counter.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            /// Use point lookup (Get) instead of Seek for visible parts.
            std::string current_value;
            if (!info.reader->get(key, &current_value))
                continue;

            if (unlikely(current_value.empty()))
                continue;
            if (unlikely(current_value.size() != 8))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Unexpected unique projection value size: {} (expected 8)",
                    current_value.size());

            auto current_entry = UniqueValueEntry::decode(current_value.data(), current_value.size());

            const auto & current_part = all_visible_parts[info.index];
            auto current_delete_bitmap = current_part->getDeleteMarkBitmap();
            if (current_delete_bitmap && current_delete_bitmap->contains(current_entry.part_offset))
                continue;

            auto visible_version = get_version(current_part);
            if (input_version > visible_version
                || (input_version == visible_version && input_part->info.level > current_part->info.level))
            {
                /// Input part wins — mark the visible part's row as deleted.
                delta_bitmaps[current_part.get()].add(static_cast<uint32_t>(current_entry.part_offset));
            }
            else
            {
                /// Visible part wins — mark the input part's row as deleted.
                delta_bitmaps[input_part.get()].add(static_cast<uint32_t>(input_entry.part_offset));
            }
            /// Only one visible part can have the same key, break after first match.
            break;
        }
    }
}

/// Deduplicate keys from input_part against a set of visible parts using parallel threads.
///
/// The function splits the input SST key space into shards and processes each shard
/// in a separate thread. Each thread produces a thread-local DeltaBitmaps which are
/// merged at the end into the final cross_part_delete_marks.
///
/// The total key count is obtained from SST TableProperties (O(1)), and boundary keys
/// are sampled with a single sequential scan.
static void dedupKeysThroughNewCommitParts(
    const DataPartPtr & input_part,
    const SSTFileReader & input_sst,
    const MergeTreeData::DataPartsVector & all_visible_parts,
    const std::vector<SSTFileReaderPtr> & visible_sst_readers,
    PartDeleteBitmapType & cross_part_delete_marks)
{
    size_t skip_by_parts = 0;
    std::atomic<size_t> skip_by_key{0};
    size_t no_skip = 0;

    SCOPE_EXIT({
        size_t skip_by_key_val = skip_by_key.load(std::memory_order_relaxed);
        no_skip += (skip_by_parts + skip_by_key_val == 0);
        ProfileEvents::increment(ProfileEvents::UniqueKeyDeduplicateSkipPartsByPart, skip_by_parts);
        ProfileEvents::increment(ProfileEvents::UniqueKeyDeduplicateSkipPartsByKey, skip_by_key_val);
        ProfileEvents::increment(ProfileEvents::UniqueKeyDeduplicateZeroSkip, no_skip);
    });

    /// Build shared visible part info list (read-only, shared across threads).
    std::vector<VisiblePartInfo> visible_part_infos;
    visible_part_infos.reserve(all_visible_parts.size());
    for (size_t i = 0; i < all_visible_parts.size(); ++i)
    {
        if (visible_sst_readers[i])
            visible_part_infos.push_back({i, visible_sst_readers[i]});
        else
            skip_by_parts++;
    }

    /// Get total key count from SST properties — O(1), no scanning needed.
    auto props = input_sst.getProperties();
    const size_t total_keys = props ? props->num_entries : 0;
    if (total_keys == 0)
        return;

    /// For small key sets, fall back to single-threaded path to avoid thread overhead.
    const size_t num_threads = (total_keys < MIN_KEYS_FOR_PARALLEL) ? 1 : std::min(DEDUP_MAX_PARALLEL, total_keys);
    const size_t stride = total_keys / num_threads;

    /// One sequential scan to sample boundary keys for each shard.
    /// boundary_keys[i] is the start key for shard i; shard i processes `stride` keys
    /// (last shard processes the remainder).
    std::vector<std::string> boundary_keys;
    boundary_keys.reserve(num_threads);
    {
        rocksdb::ReadOptions opts;
        opts.fill_cache = false;
        auto scan_iter = input_sst.newIterator(opts);
        size_t idx = 0;
        for (scan_iter->SeekToFirst(); scan_iter->Valid(); scan_iter->Next(), ++idx)
        {
            if (idx % stride == 0 && boundary_keys.size() < num_threads)
                boundary_keys.push_back(scan_iter->key().ToString());
        }
    }

    const size_t actual_shards = boundary_keys.size();
    if (actual_shards == 0)
        return;

    /// Allocate per-shard delta bitmaps.
    std::vector<DeltaBitmaps> delta_bitmaps(actual_shards);

    if (actual_shards == 1)
    {
        /// Single-threaded: process all keys directly, no thread pool needed.
        dedupKeyRange(
            input_part, input_sst, all_visible_parts, visible_part_infos,
            boundary_keys[0], total_keys, delta_bitmaps[0], skip_by_key);
    }
    else
    {
        /// Create a local thread pool and runner following the project's standard pattern.
        ThreadPool dedup_pool(
            CurrentMetrics::MergeTreePartsCleanerThreads,
            CurrentMetrics::MergeTreePartsCleanerThreadsActive,
            CurrentMetrics::MergeTreePartsCleanerThreadsScheduled,
            actual_shards);

        ThreadPoolCallbackRunnerLocal<void> runner(dedup_pool, ThreadName::UNIQUE_KEY_DEDUP);

        for (size_t shard = 0; shard < actual_shards; ++shard)
        {
            /// Compute number of keys for this shard: last shard gets the remainder.
            size_t shard_keys = (shard == actual_shards - 1) ? (total_keys - stride * shard) : stride;

            runner.enqueueAndKeepTrack(
                [&input_part, &input_sst, &all_visible_parts, &visible_part_infos,
                 start_key = boundary_keys[shard], shard_keys,
                 &delta_bitmap = delta_bitmaps[shard], &skip_by_key]
                {
                    dedupKeyRange(
                        input_part, input_sst, all_visible_parts, visible_part_infos,
                        start_key, shard_keys, delta_bitmap, skip_by_key);
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }

    /// Merge all per-shard delta bitmaps into the final output.
    for (auto & shard_delta : delta_bitmaps)
    {
        for (auto & [part_ptr, bitmap] : shard_delta)
        {
            if (!bitmap.isEmpty())
                cross_part_delete_marks[part_ptr] |= bitmap;
        }
    }
}

/// Merge cross-part delete marks into each affected part's persistent delete mark bitmap.
static void applyCrossPartDeleteMarks(const PartDeleteBitmapType & cross_part_marks_map)
{
    for (const auto & [part_ptr, cross_part_marks] : cross_part_marks_map)
    {
        if (cross_part_marks.isEmpty())
            continue;

        auto existing_marks = part_ptr->getDeleteMarkBitmap();
        if (!existing_marks)
        {
            existing_marks = (part_ptr->rows_count <= std::numeric_limits<UInt32>::max())
                ? ProjectionIndexBitmap::create32()
                : ProjectionIndexBitmap::create64();
            part_ptr->setDeleteMarkBitmap(existing_marks);
        }

        for (const auto it : cross_part_marks)
            existing_marks->add(it);
    }
}

MergeTreeDedupManager::MergeTreeDedupManager(const MergeTreeData & storage_)
    : storage(storage_)
    , log(getLogger(fmt::format("{} (MergeTreeDedupManager)", storage.getStorageID().getNameForLogs())))
{
}

MergeTreeDedupManager::DedupSSTContext MergeTreeDedupManager::prepareSSTReadersForDedup(
    const DataPartPtr & input_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPartsVector & visible_parts)
{
    DedupSSTContext ctx;

    if (input_part->rows_count == 0)
        return ctx;

    /// Get cached SST reader for input part (idempotent open).
    ctx.input_sst = input_part->getOrOpenSSTReader(metadata_snapshot);
    if (!ctx.input_sst)
    {
        LOG_DEBUG(log, "part '{}' has no unique projection SST, skip dedup", input_part->name);
        return ctx;
    }

    /// Get cached SST readers for all visible parts.
    ctx.visible_sst_readers.reserve(visible_parts.size());
    for (const auto & visible_part : visible_parts)
        ctx.visible_sst_readers.push_back(visible_part->getOrOpenSSTReader(metadata_snapshot));

    return ctx;
}

ProjectionIndexBitmapPtr MergeTreeDedupManager::buildIntraPartDeleteMark(
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
            if (iter->value().size() == 8)
            {
                auto entry = UniqueValueEntry::decode(iter->value().data(), iter->value().size());
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

void MergeTreeDedupManager::optimizeVisiblePartsForMerge(
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
        LOG_TEST(log, "optimizeVisiblePartsForMerge: merge part '{}' effective rows match "
            "covered parts ({} rows), clearing visible parts to skip cross-part dedup",
            new_part->name, new_part->rows_count);
        visible_parts.clear();
    }
}

void MergeTreeDedupManager::dedupUniqueIndex(const DataPartPtr & new_part, MergeTreeData::CommitOperation op)
{
    if (!new_part)
        return;

    delta_deleted_rows_map.clear();
    Stopwatch dedup_watch;

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    /// Acquire a shared read lock to scan active parts in the same partition.
    auto shared_lock = storage.readLockParts();
    MergeTreeData::DataPartsVector all_visible_parts = storage.getDataPartsVectorInPartitionForInternalUsage(
        {MergeTreeData::DataPartState::Active}, new_part->info.getPartitionId(), shared_lock);

    /// Filter out the new part itself and empty parts.
    std::erase_if(all_visible_parts, [&](const auto & part)
    {
        return part->rows_count == 0 || part->name == new_part->name;
    });

    /// Apply per-operation optimizations that may reduce or clear visible_parts.
    if (op == MergeTreeData::CommitOperation::Merge)
        optimizeVisiblePartsForMerge(new_part, all_visible_parts);

    /// Cross-part dedup: only needed when visible_parts is non-empty.
    auto sst_ctx = prepareSSTReadersForDedup(new_part, metadata_snapshot, all_visible_parts);
    if (!sst_ctx.input_sst)
        return;

    if (!all_visible_parts.empty())
    {
        PartDeleteBitmapType cross_part_delta;
        dedupKeysThroughNewCommitParts(
            new_part, *sst_ctx.input_sst, all_visible_parts, sst_ctx.visible_sst_readers,
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
        LOG_DEBUG(log, "dedupUniqueIndex: part '{}' has {} intra-part duplicate rows",
            new_part->name, input_part_delete_mark->cardinality());
        roaring_bitmap_or_inplace(
            &delta_deleted_rows_map[new_part.get()].roaring, input_part_delete_mark->data.bitmap32);
    }

    LOG_DEBUG(log, "dedupUniqueIndex: part '{}', op={}, visible_parts={}, elapsed={}us",
        new_part->name, static_cast<int>(op), all_visible_parts.size(), dedup_watch.elapsedMicroseconds());
}

void MergeTreeDedupManager::commitDeleteMarkBuffers(const DataPartsLock & /*lock*/)
{
    applyCrossPartDeleteMarks(delta_deleted_rows_map);
    delta_deleted_rows_map.clear();
}

void MergeTreeDedupManager::rebuildAllDeleteMarks()
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

    LOG_INFO(log, "rebuildAllDeleteMarks: starting rebuild for {} active parts", all_parts.size());

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
        /// Sort by MergeTreePartInfo: older parts first (smaller block numbers).
        std::sort(partition_parts.begin(), partition_parts.end(),
            [](const DataPartPtr & a, const DataPartPtr & b) { return a->info < b->info; });

        for (size_t i = 0; i < partition_parts.size(); ++i)
        {
            /// Build visible_parts by swapping the current part to back and using a sub-range,
            /// avoiding O(N) copies per round.
            if (i != partition_parts.size() - 1)
                std::swap(partition_parts[i], partition_parts.back());

            DataPartsVector visible_parts(partition_parts.begin(), partition_parts.end() - 1);

            /// The part being deduped is now at the back (or was already the last element).
            const auto & current_part = partition_parts.back();

            auto sst_ctx = prepareSSTReadersForDedup(current_part, metadata_snapshot, visible_parts);
            if (!sst_ctx.input_sst)
            {
                if (i != partition_parts.size() - 1)
                    std::swap(partition_parts[i], partition_parts.back());
                continue;
            }

            PartDeleteBitmapType cross_part_delta;
            dedupKeysThroughNewCommitParts(
                current_part, *sst_ctx.input_sst, visible_parts, sst_ctx.visible_sst_readers,
                cross_part_delta);

            /// Intra-part dedup: detect rows that lost during SST construction.
            auto input_part_delete_mark = buildIntraPartDeleteMark(current_part, *sst_ctx.input_sst);
            if (input_part_delete_mark)
            {
                LOG_DEBUG(log, "rebuildAllDeleteMarks: part '{}' has {} intra-part duplicate rows",
                    current_part->name, input_part_delete_mark->cardinality());
                roaring_bitmap_or_inplace(
                    &cross_part_delta[current_part.get()].roaring, input_part_delete_mark->data.bitmap32);
            }

            /// Apply cross-part delete marks directly (no commit flow needed on startup).
            applyCrossPartDeleteMarks(cross_part_delta);

            /// Restore the original position after swap.
            if (i != partition_parts.size() - 1)
                std::swap(partition_parts[i], partition_parts.back());

            ++dedup_count;
        }

        LOG_DEBUG(log, "rebuildAllDeleteMarks: partition '{}', parts={}, dedup_rounds={}",
                  partition_id, partition_parts.size(), partition_parts.size());
    }

    LOG_INFO(log, "rebuildAllDeleteMarks: complete, performed {} dedup rounds, total elapsed={}ms",
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
