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
#include <Core/ServerSettings.h>
#include <base/scope_guard.h>
#include <Interpreters/Context.h>
#include <boost/range/join.hpp>
#include <boost/icl/interval_set.hpp>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

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
    extern const Event UniqueKeyDedupParallelProcessMicroseconds;
}

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 unique_key_dedup_max_parallel_threads;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Maximum number of keys to batch in a single MultiGet call.
static constexpr size_t MULTI_GET_BATCH_SIZE = 32;

using PartWithSSTReader = MergeTreeDedupPartManager::PartWithSSTReader;

/// Retrieve the effective version for a part/entry pair.
/// When row-level versioning is available (16-byte SST values), use the per-row
/// version stored in the entry; otherwise fall back to part-level max_block.
static UInt64 getRowVersion(
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
    const std::string & end_key,
    PartDeleteBitmapMap & delta_bitmaps,
    ProjectionIndexBitmapPtr & bucket_input_exists_rows,
    size_t bucket_id)
{
    /// Cached entry from the input SST iterator, used for batching.
    struct InputKeyEntry
    {
        std::string key;
        UniqueValueEntryFull entry;
    };

    Stopwatch bucket_watch;

    /// Track which row offsets exist in the input SST within this bucket's key range.
    /// The caller will merge all buckets' bitmaps and flip to get intra-block losers.
    bucket_input_exists_rows = ProjectionIndexBitmap::create(input.part->rows_count);

    rocksdb::ReadOptions opts;
    opts.fill_cache = false;
    auto input_iter = input.reader->newIterator(opts);
    input_iter->Seek(rocksdb::Slice(start_key));

    /// Delete mark bitmap for the input part. In reverse dedup (dedupForFetch),
    /// the input is a local visible part that may already have delete marks from
    /// earlier INSERTs. Keys marked as deleted must be skipped — otherwise a
    /// stale (deleted) key could incorrectly win the version comparison and
    /// produce wrong delete marks on the fetched part.
    auto input_delete_bitmap = input.part->getDeleteBitmap();

    /// Reusable batch buffer to avoid repeated allocations.
    std::vector<InputKeyEntry> batch;
    batch.reserve(MULTI_GET_BATCH_SIZE);

    /// Track which input keys have already been matched by a visible part
    /// (only one visible part can own a given unique key).
    std::vector<bool> matched;
    matched.reserve(MULTI_GET_BATCH_SIZE);

    auto reached_end = [&]() -> bool
    {
        if (end_key.empty())
            return false;
        return input_iter->key().compare(rocksdb::Slice(end_key)) >= 0;
    };

    size_t processed = 0;
    while (input_iter->Valid() && !reached_end())
    {
        /// Phase 1: Collect a batch of keys from the input iterator.
        batch.clear();
        for (; input_iter->Valid() && !reached_end() && batch.size() < MULTI_GET_BATCH_SIZE;
             input_iter->Next(), ++processed)
        {
            if (unlikely(!input_iter->status().ok()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper new parts iterator has error {}", input_iter->status().ToString());

            if (unlikely(input_iter->key().empty() || input_iter->value().empty()))
                continue;

            const auto & value_slice = input_iter->value();
            auto input_entry = decodeUniqueValueEntry(value_slice.data(), value_slice.size());

            /// Skip keys already marked as deleted in the input part.
            if (input_delete_bitmap && input_delete_bitmap->contains(input_entry.part_offset))
                continue;

            bucket_input_exists_rows->add(input_entry.part_offset);
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
            auto current_delete_bitmap = current_part->getDeleteBitmap();

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

                auto input_version = getRowVersion(input.part, input_entry, current_value.size());
                auto visible_version = getRowVersion(current_part, current_entry, current_value.size());

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
        "bucket={}, processed={}, elapsed={}us",
        bucket_id, processed, bucket_watch.elapsedMicroseconds());
}

/// Deduplicate keys from the input part against visible parts using parallel threads.
/// Splits the input SST key space into buckets, processes each in a separate thread,
/// then merges per-bucket delta bitmaps into a result map.
/// Returns a map from part pointer to the computed delete mark bitmap.
static PartDeleteBitmapMap dedupKeysThroughNewCommitParts(
    const PartWithSSTReader & input,
    const std::vector<PartWithSSTReader> & visible_parts,
    size_t max_parallel)
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

    /// Sample keys are pre-collected during SST write via SampleKeyCollector
    /// and stored in SST file properties. No scanning needed.
    auto all_sample_keys = input.reader->getSampleKeys();

    if (all_sample_keys.empty())
        return result;

    /// Downsample from all collected sample keys (up to 256) to the actual
    /// parallelism level. For example, if we have 256 sample keys but only
    /// 16 threads, pick every 16th sample key to get 16 evenly-spaced bucket
    /// boundaries.
    const size_t actual_buckets = std::min(all_sample_keys.size(), max_parallel);
    std::vector<std::string> bucket_start_keys;
    bucket_start_keys.reserve(actual_buckets);
    for (size_t i = 0; i < actual_buckets; ++i)
        bucket_start_keys.push_back(std::move(all_sample_keys[i * all_sample_keys.size() / actual_buckets]));

    /// Allocate per-bucket delta bitmaps and per-bucket input exists rows.
    std::vector<PartDeleteBitmapMap> delta_bitmaps(actual_buckets);
    std::vector<ProjectionIndexBitmapPtr> bucket_exists_rows(actual_buckets);
    Stopwatch parallel_process_watch;
    {
        /// Unified parallel path: even for a single bucket the thread pool
        /// overhead is negligible, and deduplicateKeyByBucket with an empty
        /// key range returns immediately so empty buckets are essentially free.
        ThreadPool dedup_pool(
            CurrentMetrics::UniqueKeyDedupThreads,
            CurrentMetrics::UniqueKeyDedupThreadsActive,
            CurrentMetrics::UniqueKeyDedupThreadsScheduled,
            actual_buckets);

        ThreadPoolCallbackRunnerLocal<void> runner(dedup_pool, ThreadName::UNIQUE_KEY_DEDUP);

        for (size_t bucket = 0; bucket < actual_buckets; ++bucket)
        {
            std::string next_sample_key = (bucket + 1 < actual_buckets) ? bucket_start_keys[bucket + 1] : std::string{};

            runner.enqueueAndKeepTrack(
                [&input,
                 &visible_parts,
                 start_key = bucket_start_keys[bucket],
                 end_key = std::move(next_sample_key),
                 &delta_bitmap = delta_bitmaps[bucket],
                 &exists_rows = bucket_exists_rows[bucket],
                 bucket]
                {
                    deduplicateKeyByBucket(
                        input, visible_parts, start_key, end_key, delta_bitmap, exists_rows, bucket);
                });
        }

        runner.waitForAllToFinishAndRethrowFirstError();
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyDedupParallelProcessMicroseconds, parallel_process_watch.elapsedMicroseconds());

    /// Merge per-bucket input_part_exists_rows into a single bitmap, then flip
    /// to get intra-block loser offsets (rows whose offset never appeared in SST
    /// across all buckets = block-level duplicates that lost during dedup).
    auto all_input_exists_rows = ProjectionIndexBitmap::create(input.part->rows_count);
    for (const auto & bw : bucket_exists_rows)
    {
        if (bw && !bw->empty())
            all_input_exists_rows->unionWith(*bw);
    }


    /// Merge all per-bucket delta bitmaps into the final output.
    /// Include the input part as well — it may have rows marked as deleted
    /// when a visible part wins the version comparison.
    auto input_range = boost::make_iterator_range(&input, &input + 1);
    for (const auto & [part, reader] : boost::join(visible_parts, input_range))
    {
        auto bitmap = ProjectionIndexBitmap::create(part->rows_count);
        for (const auto & shard_delta : delta_bitmaps)
        {
            auto it = shard_delta.find(part.get());
            if (it != shard_delta.end() && it->second && !it->second->empty())
                bitmap->unionWith(*it->second);
        }

        if (part.get() == input.part.get())
        {
            all_input_exists_rows->flipRange(0, input.part->rows_count);
            bitmap->unionWith(*all_input_exists_rows);
        }

        if (!bitmap->empty())
            result.emplace(part.get(), std::move(bitmap));
    }

    return result;
}

/// Merge cross-part delete bitmaps into each affected part's persistent delete bitmap.
/// Both writes (commitDeleteBitmapBuffers under DataPartsLock) and reads (createStorageSnapshot
/// under readLockParts) are serialized by the parts lock, so there is no data race.
/// We still use COW (Copy-On-Write) — always creating a new bitmap object — so that
/// snapshot readers holding a shared_ptr to the old bitmap are not affected by later updates.
static void applyCrossPartDeleteBitmaps(const PartDeleteBitmapMap & cross_part_marks_map)
{
    for (const auto & [part_ptr, cross_part_marks] : cross_part_marks_map)
    {
        if (!cross_part_marks || cross_part_marks->empty())
            continue;

        /// COW: create a fresh bitmap, copy existing marks if any, then add new ones.
        auto new_marks = ProjectionIndexBitmap::create(part_ptr->rows_count);
        if (auto existing_marks = part_ptr->getDeleteBitmap())
            new_marks->unionWith(*existing_marks);

        new_marks->unionWith(*cross_part_marks);

        /// Replace the bitmap pointer. Readers that already captured the old shared_ptr
        /// in a snapshot continue to use it; new snapshots will pick up the updated bitmap.
        part_ptr->replaceDeleteBitmap(new_marks);
    }
}

MergeTreeDedupPartManager::MergeTreeDedupPartManager(MergeTreeData & storage_)
    : storage(storage_)
    , log(getLogger(fmt::format("{} (MergeTreeDedupPartManager)", storage.getStorageID().getNameForLogs())))
{
    /// Initialize RocksDB for persistent delete bitmaps.
    auto data_paths = storage.getDataPaths();
    if (data_paths.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No data paths available for MergeTreeDedupPartManager");

    rocksdb_dir = std::filesystem::path(data_paths[0]) / "dedup_rocksdb";
    std::filesystem::create_directories(rocksdb_dir);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    options.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::DB * db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);
    if (!status.ok())
    {
        LOG_ERROR(log, "Failed to open RocksDB for table {} at {}: {}",
            storage.getStorageID().getFullTableName(), rocksdb_dir, status.ToString());
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Failed to open RocksDB path at: {}: {}", rocksdb_dir, status.ToString());
    }
    rocksdb_ptr.reset(db);
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

std::pair<MergeTreeDedupPartManager::PartWithSSTReader, std::vector<MergeTreeDedupPartManager::PartWithSSTReader>>
MergeTreeDedupPartManager::prepareSSTReadersForDedup(
    const DataPartPtr & input_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & other_parts)
{
    if (input_part->rows_count == 0)
        return {};

    /// Get cached SST reader for input part.
    auto input_reader = input_part->getOrOpenSSTReader(metadata_snapshot);
    if (!input_reader)
    {
        LOG_DEBUG(log, "part '{}' has no unique projection SST, skip dedup", input_part->name);
        return {};
    }

    PartWithSSTReader input{input_part, std::move(input_reader)};
    auto visible_parts = openSSTReadersForParts(other_parts, metadata_snapshot);
    return {std::move(input), std::move(visible_parts)};
}

void MergeTreeDedupPartManager::storeDeleteMarkBuffers(const std::unordered_map<std::string, DeleteBitmapPtr> & buffers)
{
    if (buffers.empty())
        return;

    WriteBufferFromOwnString out;
    rocksdb::WriteBatch batch;
    for (const auto & [part_name, bitmap] : buffers)
    {
        out.restart();
        /// Always write a record for every part — even when the bitmap is
        /// empty. On restart we check whether a RocksDB record exists to
        /// verify that the previous commit completed successfully.
        if (bitmap && !bitmap->empty())
            bitmap->serializePortable(out);
        else
            ProjectionIndexBitmap::create(0)->serializePortable(out);
        out.finalize();
        batch.Put(part_name, out.str());
    }

    auto status = rocksdb_ptr->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "RocksDB write error: {}", status.ToString());
}

ProjectionIndexBitmapPtr MergeTreeDedupPartManager::loadDeleteBitmapFromRocksDB(const std::string & part_name)
{
    std::string value;
    auto status = rocksdb_ptr->Get(rocksdb::ReadOptions{}, part_name, &value);
    if (!status.ok())
        return nullptr;

    ReadBufferFromString in(value);
    auto bitmap = ProjectionIndexBitmap::deserializePortable(in);
    /// Always return a non-null bitmap when RocksDB has a record — even
    /// when the bitmap is empty. This lets callers distinguish "no record
    /// in RocksDB" (nullptr) from "record exists but bitmap is empty".
    return bitmap ? bitmap : ProjectionIndexBitmap::create(0);
}

void MergeTreeDedupPartManager::removeDeleteBitmap(const std::string & part_name) const
{
    auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions{}, part_name);
    if (!status.ok())
        LOG_WARNING(log, "Failed to remove delete bitmap for part {} from RocksDB: {}", part_name, status.ToString());
}

void MergeTreeDedupPartManager::loadDeleteBitmapsAndCheckOnStartup()
{
    Stopwatch watch;

    /// Load persisted delete bitmaps from RocksDB for all active parts.
    /// This replaces the per-part loading that was previously called inside
    /// `loadColumnsChecksumsIndexes`, avoiding the virtual-dispatch issue
    /// where `supportsUpsert` returns false during parent-class construction.
    auto all_active_parts = storage.getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active});

    size_t loaded = 0;
    for (const auto & part : all_active_parts)
    {
        if (part->rows_count == 0)
            continue;

        auto bitmap = loadDeleteBitmapFromRocksDB(part->name);
        if (bitmap)
        {
            part->replaceDeleteBitmap(std::move(bitmap));
            ++loaded;
        }
    }

    LOG_INFO(log, "Loaded {} delete bitmaps from RocksDB for {} active parts, elapsed={}ms",
        loaded, all_active_parts.size(), watch.elapsedMilliseconds());

    /// Now check for parts that still have no bitmap (RocksDB had no record)
    /// and re-dedup them.
    checkAndDedupPartsOnStartup();
}

void MergeTreeDedupPartManager::checkAndDedupPartsOnStartup()
{
    Stopwatch watch;

    /// Mark startup as completed early so that `dedupPart` (which asserts
    /// `startup_completed`) can be called from within this function.
    startup_completed.store(true, std::memory_order_release);

    /// Get all active parts with non-zero rows.
    auto all_active_parts = storage.getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active});

    /// Phase 1: Collect parts whose delete bitmaps were not persisted to
    /// RocksDB. This can happen when the server restarted unexpectedly
    /// before the RocksDB write batch was committed.
    /// Delete bitmaps are already loaded from RocksDB during part loading,
    /// so a nullptr delete bitmap means RocksDB had no record for this part.
    ///
    /// We must collect first and process later because `commitDeleteBitmapBuffers`
    /// modifies in-memory delete bitmaps — if we checked and committed in the
    /// same loop, later iterations would see stale bitmap states.
    DataPartsVector parts_to_dedup;
    for (const auto & part : all_active_parts)
    {
        if (part->rows_count == 0)
            continue;

        if (!part->getDeleteBitmap())
        {
            LOG_TRACE(log, "Part '{}' has no persisted delete bitmap, will re-dedup", part->name);
            parts_to_dedup.push_back(part);
        }
    }

    /// Phase 2: Re-dedup and commit each collected part.
    for (const auto & part : parts_to_dedup)
    {
        dedupPart(part);
        auto parts_lock = storage.lockParts();
        commitDeleteBitmapBuffers(parts_lock);
    }

    LOG_INFO(log, "checkAndDedupPartsOnStartup: {} — re-deduped {} parts, elapsed={}ms",
        storage.getStorageID().getNameForLogs(), parts_to_dedup.size(), watch.elapsedMilliseconds());
}

void MergeTreeDedupPartManager::dedupForInsert(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & visible_parts)
{
    /// Open the input part's SST reader (from cache) for cross-part dedup.
    auto [input, other] = prepareSSTReadersForDedup(new_part, metadata_snapshot, visible_parts);
    if (!input.reader)
        return;

    /// Cross-part dedup against all visible parts.
    const size_t max_parallel = storage.getContext()->getServerSettings()[ServerSetting::unique_key_dedup_max_parallel_threads];
    delta_deleted_rows_map = dedupKeysThroughNewCommitParts(input, other, max_parallel);
}

void MergeTreeDedupPartManager::dedupForFetch(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & source_parts,
    const DataPartsVector & all_visible_parts)
{
    Stopwatch watch;

    /// If source parts on this replica do not fully cover the fetched part's
    /// block range, some level-0 INSERT parts have not arrived yet. The fetched
    /// (merged) part contains keys originating from those missing parts, so the
    /// snapshot_max_block optimisation is unsafe — fall back to full cross-part dedup.
    std::vector<MergeTreePartInfo> source_infos;
    source_infos.reserve(source_parts.size());
    for (const auto & part : source_parts)
        source_infos.push_back(part->info);

    if (!MergeTreePartInfo::areAllBlockNumbersCovered(new_part->info, std::move(source_infos)))
    {
        /// Filter out parts covered by the fetched part's block range —
        /// the fetched part already contains their data, so dedup against
        /// them is unnecessary and would produce incorrect delete marks.
        DataPartsVector non_covered_visible_parts;
        non_covered_visible_parts.reserve(all_visible_parts.size());
        for (const auto & part : all_visible_parts)
        {
            if (part->info.min_block >= new_part->info.min_block
                && part->info.max_block <= new_part->info.max_block)
                continue;
            non_covered_visible_parts.push_back(part);
        }

        LOG_DEBUG(log,
            "dedupForFetch: part '{}' has source-part block gaps, "
            "falling back to dedupForInsert for full cross-part dedup, "
            "visible_parts={}, non_covered_visible_parts={}",
            new_part->name,
            all_visible_parts.size(),
            non_covered_visible_parts.size());

        /// Discard the delete mark fetched from the source replica — it was
        /// computed under a different set of visible parts and is unreliable
        /// when source-part block gaps exist. dedupForInsert will recompute
        /// delete marks from scratch against the local visible parts.
        new_part->replaceDeleteBitmap(nullptr);
        dedupForInsert(new_part, metadata_snapshot, non_covered_visible_parts);
        return;
    }

    /// Filter visible parts:
    /// 1. Skip parts covered by the fetched part's block range (source parts).
    /// 2. Use fetch-time block ranges from source replica to skip parts that
    ///    were already visible on the source replica at fetch/merge time.
    DataPartsVector filtered_visible_parts;
    filtered_visible_parts.reserve(all_visible_parts.size());

    /// Fetch-time block ranges from source replica (obtained atomically
    /// with the delete mark at fetch time).
    const auto & block_ranges = new_part->fetch_dedup_block_ranges;
    bool has_block_ranges = !boost::icl::is_empty(block_ranges);

    for (const auto & part : all_visible_parts)
    {
        /// If block ranges are available, skip parts whose [min_block, max_block]
        /// is fully contained within the source replica's active block ranges.
        if (has_block_ranges
            && boost::icl::within(
                boost::icl::discrete_interval<Int64>::closed(part->info.min_block, part->info.max_block),
                block_ranges))
            continue;

        filtered_visible_parts.push_back(part);
    }

    if (filtered_visible_parts.empty())
        return;

    /// Reverse dedup: instead of scanning the large fetched part's SST and
    /// looking up each key in every local part (O(fetched_keys)), we flip
    /// the roles — scan each small local part's SST and MultiGet into the
    /// fetched part's SST (O(Σ local_keys)).
    ///
    /// `prepareSSTReadersForDedup` opens the fetched part as `.input` and
    /// the filtered local parts as `.visible_parts`. We swap the roles
    /// below: the fetched part becomes the lookup target, and each local
    /// part takes turns being the scan source.
    auto [input, local_parts] = prepareSSTReadersForDedup(new_part, metadata_snapshot, filtered_visible_parts);

    const size_t max_parallel = storage.getContext()->getServerSettings()[ServerSetting::unique_key_dedup_max_parallel_threads];

    /// The fetched part serves as the lookup target in every iteration.
    std::vector<PartWithSSTReader> fetched_as_target = {input};

    /// For each local part, scan its SST (as the "input" role in
    /// `dedupKeysThroughNewCommitParts`) and look up keys in the fetched
    /// part (as the "visible_parts" / lookup target role).
    ///
    /// The fetched merged part does not introduce new unique keys — it only
    /// combines data from source parts that have already been deduped on
    /// this replica. Therefore only the fetched part (`new_part`) can
    /// receive delete marks; local parts should never be marked as deleted.
    for (auto & local_part : local_parts)
    {
        auto per_result = dedupKeysThroughNewCommitParts(
            local_part, fetched_as_target, max_parallel);

        for (auto & [part_ptr, bitmap] : per_result)
        {
            if (!bitmap || bitmap->empty())
                continue;

            /// The merged part does not introduce new keys, so local parts
            /// should never lose the version comparison.
            if (part_ptr != new_part.get())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "dedupForFetch: local part '{}' unexpectedly received delete marks "
                    "during reverse dedup against fetched part '{}'",
                    local_part.part->name, new_part->name);

            auto & existing = delta_deleted_rows_map[part_ptr];
            if (!existing)
                existing = std::move(bitmap);
            else
                existing->unionWith(*bitmap);
        }
    }

    LOG_TEST(log,
        "dedupForFetch [REVERSE DEDUP]: part '{}', has_block_ranges={}, total_visible={}, filtered_local={}, elapsed={}us",
        new_part->name,
        has_block_ranges,
        all_visible_parts.size(),
        filtered_visible_parts.size(),
        watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::dedupForMerge(
    const DataPartPtr & new_part,
    const StorageMetadataPtr & metadata_snapshot,
    const DataPartsVector & source_parts,
    const MergeTreeData::DeleteBitmapSnapshotMap & source_delete_bitmap_snapshots,
    const DataPartsVector & /*all_visible_parts*/)
{
    /// Merge only combines existing data — it does not introduce new
    /// unique keys. The only dedup work needed is propagating delete marks:
    /// if a concurrent INSERT marked a row as deleted in a source part, the
    /// corresponding row in the merged part must also be marked.
    size_t source_effective_rows = 0;
    for (const auto & part : source_parts)
    {
        size_t deleted = 0;
        if (auto delete_bitmap = part->getDeleteBitmap())
            deleted = delete_bitmap->cardinality();
        source_effective_rows += (part->rows_count - deleted);
    }

    /// If the merged part's row count is no greater than the sum of source
    /// parts' effective rows (rows - delete marks), no concurrent INSERT has
    /// deleted any key in the source parts — nothing to propagate.
    /// Delete marks are monotonically increasing, so source_effective_rows
    /// can only be <= new_part->rows_count; using >= as a defensive check.
    if (!new_part->rows_count || source_effective_rows >= new_part->rows_count)
        return;

    dedupDeletedKeysFromSourceParts(new_part, source_parts, metadata_snapshot, source_delete_bitmap_snapshots);
}

void MergeTreeDedupPartManager::dedupForMutation(
    const DataPartPtr & new_part,
    const DataPartPtr & source_part)
{
    /// Mutation preserves row count, so row offsets are unchanged.
    /// Clone the delete mark bitmap directly from the source part.
    if (new_part->rows_count != source_part->rows_count)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "dedupForMutation: part '{}' has {} rows, but source part '{}' has {} rows",
            new_part->name, new_part->rows_count, source_part->name, source_part->rows_count);

    auto source_delete_bitmap = source_part->getMutableDeleteBitmap();
    if (!source_delete_bitmap || source_delete_bitmap->empty())
        return;

    delta_deleted_rows_map[new_part.get()] = source_delete_bitmap;
}

/// For a single source part, find keys in the diff bitmap (newly deleted
/// since merge started) and look them up in the merged part's SST to get
/// the corresponding merged-part offsets.
/// Returns a bitmap of merged-part offsets to mark as deleted, or nullptr.
static ProjectionIndexBitmapPtr dedupDeletedKeysForOneSourcePart(
    const PartWithSSTReader & source_part_with_sst_reader,
    const SSTFileReaderPtr & merged_sst_reader,
    const DeleteBitmapPtr & diff_bitmap)
{
    if (!diff_bitmap || diff_bitmap->cardinality() == 0)
        return nullptr;

    auto bitmap = ProjectionIndexBitmap::create(0);

    /// Reusable batch buffers to avoid repeated allocations.
    std::vector<std::string> batch;
    batch.reserve(MULTI_GET_BATCH_SIZE);

    rocksdb::ReadOptions opts;
    opts.fill_cache = false;

    size_t diff_remaining = diff_bitmap->cardinality();

    auto iter = source_part_with_sst_reader.reader->newIterator(opts);
    iter->SeekToFirst();

    while (iter->Valid() && diff_remaining > 0)
    {
        /// Phase 1: Collect a batch of deleted keys from the source SST.
        batch.clear();
        for (; iter->Valid() && diff_remaining > 0 && batch.size() < MULTI_GET_BATCH_SIZE;
             iter->Next())
        {
            if (unlikely(!iter->status().ok()))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "SST iterator error during delete mark propagation: {}", iter->status().ToString());

            auto val = iter->value();
            if (unlikely(val.empty()))
                continue;

            auto entry = decodeUniqueValueEntry(val.data(), val.size());
            if (!diff_bitmap->contains(entry.part_offset))
                continue;

            batch.push_back(iter->key().ToString());
            --diff_remaining;
        }

        if (batch.empty())
            break;

        /// Phase 2: Batch MultiGet into the merged part's SST.
        std::vector<rocksdb::Slice> key_slices;
        key_slices.reserve(batch.size());
        for (const auto & k : batch)
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
        }
    }

    return bitmap->empty() ? nullptr : bitmap;
}

static std::vector<DeleteBitmapPtr> computeDeleteBitmapDiffs(
    const std::vector<PartWithSSTReader> & source_part_readers,
    const MergeTreeData::DeleteBitmapSnapshotMap & snapshots)
{
    std::vector<DeleteBitmapPtr> diffs;
    diffs.reserve(source_part_readers.size());

    for (const auto & [part, reader] : source_part_readers)
    {
        auto current_marks = part->getDeleteBitmap();
        if (!current_marks || current_marks->empty())
        {
            diffs.push_back(nullptr);
            continue;
        }

        auto it = snapshots.find(part->name);
        if (it == snapshots.end() || !it->second)
        {
            /// No snapshot entry or snapshot is nullptr — the part had no
            /// delete marks when the merge started, so all current marks
            /// are new diffs produced by concurrent INSERTs.
            diffs.push_back(std::move(current_marks));
        }
        else
        {
            /// diff = current AND NOT snapshot.
            auto diff = ProjectionIndexBitmap::create(part->rows_count);
            diff->unionWith(*current_marks);
            diff->andNotWith(*it->second);
            diffs.push_back(std::move(diff));
        }
    }

    return diffs;
}

void MergeTreeDedupPartManager::dedupDeletedKeysFromSourceParts(
    const DataPartPtr & new_merged_part,
    const DataPartsVector & source_covered_parts,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData::DeleteBitmapSnapshotMap & delete_bitmap_snapshots)
{
    auto [input, source_part_readers] = prepareSSTReadersForDedup(new_merged_part, metadata_snapshot, source_covered_parts);
    SCOPE_EXIT({
        input.releaseBufferMemory();
        releaseAllBufferMemory(source_part_readers);
    });

    /// Compute which rows were newly deleted by concurrent INSERTs since
    /// the merge/mutation started (diff = current_marks - snapshot_marks).
    /// `propagateDeletedKeysForOneSourcePart` returns early when the diff
    /// bitmap is empty, so parts without new deletions are essentially free.
    auto diff_bitmaps = computeDeleteBitmapDiffs(source_part_readers, delete_bitmap_snapshots);
    const size_t num_parts = source_part_readers.size();
    std::vector<ProjectionIndexBitmapPtr> per_part_bitmaps(num_parts);

    ThreadPool dedup_pool(
        CurrentMetrics::UniqueKeyDedupThreads,
        CurrentMetrics::UniqueKeyDedupThreadsActive,
        CurrentMetrics::UniqueKeyDedupThreadsScheduled,
        num_parts);

    ThreadPoolCallbackRunnerLocal<void> runner(dedup_pool, ThreadName::UNIQUE_KEY_DEDUP);

    for (size_t i = 0; i < num_parts; ++i)
    {
        runner.enqueueAndKeepTrack(
            [&source_part_readers, &diff_bitmaps, &input, &per_part_bitmaps, i]
            {
                per_part_bitmaps[i] = dedupDeletedKeysForOneSourcePart(
                    source_part_readers[i], input.reader, diff_bitmaps[i]);
            });
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Merge per-part bitmaps into the final result.
    auto result_bitmap = ProjectionIndexBitmap::create(new_merged_part->rows_count);
    for (size_t i = 0; i < num_parts; ++i)
    {
        if (per_part_bitmaps[i] && !per_part_bitmaps[i]->empty())
            result_bitmap->unionWith(*per_part_bitmaps[i]);
    }

    if (!result_bitmap->empty())
        delta_deleted_rows_map[new_merged_part.get()] = std::move(result_bitmap);
}

void MergeTreeDedupPartManager::dedupPart(
    const DataPartPtr & new_part,
    const std::optional<MergeTreeData::DeleteBitmapSnapshotMap> & source_delete_bitmap_snapshots)
{
    if (!new_part)
        return;

    /// Defensive check: `checkAndDedupPartsOnStartup` must complete before
    /// any background merge/mutation/fetch calls `dedupPart`. If this fires,
    /// someone has reordered the startup sequence.
    chassert(startup_completed.load(std::memory_order_acquire));

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
    ///   level == 0 + mutation == 0                              → Insert optimize (full cross-part dedup)
    ///   mutation > 0 + source part                              → Mutation optimize
    ///   level > 0 + has snapshots (has_value)                   → Merge optimize
    ///   level > 0 + no snapshots (nullopt) + no source part     → Fetch optimize
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
    else if (source_delete_bitmap_snapshots.has_value())
    {
        /// Has delete bitmap snapshots — this is a locally-produced merge.
        /// The map may be empty when none of the source parts had delete bitmaps,
        /// but has_value() == true distinguishes it from a fetched part.
        op_name = "merge";
        dedupForMerge(new_part, metadata_snapshot, source_parts, *source_delete_bitmap_snapshots, all_visible_parts);
    }
    else
    {
        /// Level > 0, no snapshots, no matching source part — a fetched part.
        /// Try optimized dual-iterator path; falls back to full dedup if
        /// source parts have block gaps.
        op_name = "fetch";
        dedupForFetch(new_part, metadata_snapshot, source_parts, all_visible_parts);
    }

    /// Ensure the new part is always present in `delta_deleted_rows_map`
    /// so that `commitDeleteBitmapBuffers` will write a RocksDB record
    /// for it — even when no duplicate keys were found (empty bitmap).
    /// On restart we rely on the existence of a RocksDB record to verify
    /// that the previous commit completed successfully.
    if (delta_deleted_rows_map.find(new_part.get()) == delta_deleted_rows_map.end())
        delta_deleted_rows_map[new_part.get()] = nullptr;

    LOG_DEBUG(log, "dedupPart: part '{}', op={}, visible_parts={}, elapsed={}us",
        new_part->name, op_name, all_visible_parts.size(), dedup_watch.elapsedMicroseconds());
}

void MergeTreeDedupPartManager::commitDeleteBitmapBuffers(const DataPartsLock & /*lock*/)
{
    applyCrossPartDeleteBitmaps(delta_deleted_rows_map);

    /// Persist delete bitmaps to RocksDB for every part in the delta map.
    /// This includes the new part itself (guaranteed by `dedupPart`) even
    /// when its bitmap is empty — on restart we check for the existence
    /// of a RocksDB record to verify commit success.
    std::unordered_map<std::string, DeleteBitmapPtr> buffers;
    for (const auto & [part_ptr, bitmap] : delta_deleted_rows_map)
        buffers.emplace(part_ptr->name, part_ptr->getDeleteBitmap());
    storeDeleteMarkBuffers(buffers);

    delta_deleted_rows_map.clear();
}

std::any MergeTreeDedupPartManager::dedupPartsBeforeTransactionCommit(
    const MergeTreeData::MutableDataParts & parts,
    const MergeTreeData::BeforeCommitHookContext & before_commit_context)
{
    /// Serialize concurrent commits: only one thread computes delete bitmaps at a time.
    auto unique_lock = lockUniqueProcess();

    MergeTreeData::DeleteBitmapSnapshotMap delete_bitmap_snapshots;
    std::optional<MergeTreeData::DeleteBitmapSnapshotMap> opt_snapshots;
    if (before_commit_context.data.has_value())
    {
        delete_bitmap_snapshots = std::any_cast<MergeTreeData::DeleteBitmapSnapshotMap>(before_commit_context.data);
        opt_snapshots = std::move(delete_bitmap_snapshots);
    }

    for (const auto & part : parts)
    {
        dedupPart(part, opt_snapshots);
    }

    /// Wrap in shared_ptr because std::any requires CopyConstructible,
    /// but UniqueProcessLock is move-only.
    return std::make_shared<UniqueProcessLock>(std::move(unique_lock));
}

DedupMetadataForFetch MergeTreeDedupPartManager::collectDedupMetadataForFetch(const DataPartPtr & part) const
{
    DedupMetadataForFetch result;

    if (part->info.level == 0)
        return result;

    auto lock = storage.readLockParts();

    result.delete_bitmap = part->getMutableDeleteBitmap();

    auto partition_parts
        = storage.getDataPartsVectorInPartitionForInternalUsage(MergeTreeData::DataPartState::Active, part->info.getPartitionId(), lock);
    for (const auto & p : partition_parts)
        result.block_ranges += boost::icl::discrete_interval<Int64>::closed(p->info.min_block, p->info.max_block);

    return result;
}

void DedupMetadataForFetch::serialize(WriteBuffer & out) const
{
    /// Always serialize a bitmap — use the actual delete mark if present,
    /// otherwise serialize an empty bitmap so the deserializer can always
    /// call deserializePortable unconditionally.
    if (delete_bitmap && !delete_bitmap->empty())
        delete_bitmap->serializePortable(out);
    else
        ProjectionIndexBitmap::create(0)->serializePortable(out);

    /// Write the number of intervals, then each [lower, upper] pair.
    size_t interval_count = boost::icl::interval_count(block_ranges);
    writeIntText(interval_count, out);
    writeChar('\n', out);
    for (const auto & interval : block_ranges)
    {
        writeIntText(interval.lower(), out);
        writeChar(' ', out);
        writeIntText(interval.upper(), out);
        writeChar('\n', out);
    }
}

DedupMetadataForFetch DedupMetadataForFetch::deserialize(ReadBuffer & in)
{
    DedupMetadataForFetch result;

    /// Always deserialize a bitmap; discard it if empty.
    auto bitmap = ProjectionIndexBitmap::deserializePortable(in);
    if (bitmap && !bitmap->empty())
    result.delete_bitmap = std::move(bitmap);

    size_t interval_count = 0;
    readIntText(interval_count, in);
    assertChar('\n', in);
    for (size_t i = 0; i < interval_count; ++i)
    {
        Int64 lower = 0;
        Int64 upper = 0;
        readIntText(lower, in);
        assertChar(' ', in);
        readIntText(upper, in);
        assertChar('\n', in);
        result.block_ranges += boost::icl::discrete_interval<Int64>::closed(lower, upper);
    }

    return result;
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
