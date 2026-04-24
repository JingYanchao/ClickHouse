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

    bool supportsLightweightDelete() const override { return false; }

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;

    /// DROP PART is not supported because dropping a single part would leave
    /// stale delete marks on other parts that reference the dropped part's keys.
    /// Use DROP PARTITION instead to remove an entire partition atomically.
    void dropPart(const String & part_name, bool detach, ContextPtr query_context) override;

    /// REPLACE PARTITION and MOVE PARTITION TO TABLE bypass the
    /// BeforeTransactionCommitHook (they call `Transaction::commit` with
    /// DataPartsLock directly), which would silently skip cross-part dedup.
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr query_context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr query_context) override;

    /// RESTORE bypasses the dedup hook; not yet supported.
    void attachRestoredParts(MutableDataPartsVector && parts) override;

    /// Override startup to rebuild delete marks BEFORE background tasks start.
    /// This ensures no merge/mutate can run until dedup state is fully restored.
    void startup() override;

private:
    /// The name of the unique projection (from engine param or auto-detected).
    String unique_projection_name;
};

}
