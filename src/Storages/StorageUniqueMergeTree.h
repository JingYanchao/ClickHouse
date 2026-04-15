#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDedupPartManager.h>
#include <memory>

namespace DB
{


/// StorageUniqueMergeTree — MergeTree with built-in row-level deduplication.
///
/// Inherits StorageMergeTree and injects dedup logic via
/// BeforeTransactionCommitHook. The hook is called by Transaction::commit
/// BEFORE lockParts, ensuring lock ordering:
///   UniqueProcessLock -> DataPartsLock (read-friendly).
///
/// The hook acquires UniqueProcessLock to serialize concurrent commits,
/// then deduplicates each committing part against existing active parts.
/// Delete mark buffers are committed inside Transaction::commit under
/// DataPartsLock to ensure atomicity with part state transitions.
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

    String getUniqueProjectionName() const override { return unique_projection_name; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return false; }

    /// Override startup to rebuild delete marks BEFORE background tasks start.
    /// This ensures no merge/mutate can run until dedup state is fully restored.
    void startup() override;

private:
    /// The name of the unique projection (from engine param or auto-detected).
    String unique_projection_name;
};

}
