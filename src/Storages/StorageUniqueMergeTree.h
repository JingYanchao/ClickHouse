#pragma once

#include <Storages/StorageMergeTree.h>

#include <memory>

namespace DB
{


/// StorageUniqueMergeTree — MergeTree with built-in row-level deduplication.
///
/// It inherits from StorageMergeTree and injects dedup logic via
/// TransactionPreLockHook. During the no-arg Transaction::commit(),
/// the hook is called BEFORE lockParts(), ensuring lock ordering:
///   UniqueProcessLock -> DataPartsLock (read-friendly).
///
/// The hook:
///   1. Acquires the UniqueProcessLock via MergeTreeDedupManager to
///      serialize concurrent commits.
///   2. For each committing part, performs cross-part dedup against
///      existing active parts using MergeTreeDedupManager.
///   3. Commits delete mark buffers to persist dedup results.
///
/// For callers using the commit(DataPartsLock &) overload (e.g. merge,
/// mutation, partition operations), dedup must be handled externally
/// before acquiring the DataPartsLock.
///
/// The unique projection is identified by having a non-null
/// ProjectionIndexUnique index in the ProjectionDescription.
class StorageUniqueMergeTree final : public StorageMergeTree
{
public:
    StorageUniqueMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> storage_settings_,
        const String & unique_projection_name_ = {});

    std::string getName() const override { return "UniqueMergeTree"; }

    bool supportsUpsert() const override { return true; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return false; }

    /// Override startup to rebuild delete marks BEFORE background tasks start.
    /// This ensures no merge/mutate can run until dedup state is fully restored.
    void startup() override;

private:
    /// The name of the unique projection (from engine param or auto-detected).
    String unique_projection_name;

    /// The pre-lock hook: called by the no-arg Transaction::commit()
    /// BEFORE lockParts(), so that dedup does not block reads.
    /// Returns a std::any holding UniqueProcessLock to keep the dedup mutex
    /// locked until the caller finishes commit.
    std::any onPreLock(const MutableDataParts & parts, CommitOperation op);
};

}
