#pragma once

#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDedupPartManager.h>
#include <memory>

namespace DB
{

/// StorageReplicatedUniqueMergeTree — ReplicatedMergeTree with built-in row-level deduplication.
///
/// Inherits StorageReplicatedMergeTree and injects dedup logic via
/// BeforeTransactionCommitHook. The hook is called by Transaction::commit
/// BEFORE lockParts, ensuring lock ordering:
///   UniqueProcessLock -> DataPartsLock (read-friendly).
///
/// Each replica independently computes delete marks from SST data.
/// SST files are replicated along with parts, so all replicas converge
/// to the same dedup state deterministically.
///
/// The hook acquires UniqueProcessLock to serialize concurrent commits,
/// then deduplicates each committing part against existing active parts.
/// Delete mark buffers are committed inside Transaction::commit under
/// DataPartsLock to ensure atomicity with part state transitions.
class StorageReplicatedUniqueMergeTree final : public StorageReplicatedMergeTree
{
public:
    StorageReplicatedUniqueMergeTree(
        const TableZnodeInfo & zookeeper_info_,
        LoadingStrictnessLevel mode,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool need_check_structure,
        const ZooKeeperRetriesInfo & create_query_zookeeper_retries_info_,
        const String & unique_projection_name_ = {});

    std::string getName() const override { return "ReplicatedUniqueMergeTree"; }

    bool supportsUpsert() const override { return true; }

    String getUniqueProjectionName() const override { return unique_projection_name; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return false; }

    /// DROP PART is not supported for ReplicatedUniqueMergeTree because
    /// dropping a single part would leave stale delete marks on other parts
    /// that reference the dropped part's keys, leading to incorrect query
    /// results. DROP PARTITION is safe because it removes all parts (and
    /// their delete marks) in the partition atomically.
    void dropPart(const String & part_name, bool detach, ContextPtr query_context) override;

    /// REPLACE PARTITION and MOVE PARTITION TO TABLE are not yet supported
    /// for ReplicatedUniqueMergeTree because their commit paths bypass the
    /// BeforeTransactionCommitHook (they call Transaction::commit(DataPartsLock&)
    /// directly, which does not invoke the dedup hook). Enabling them would
    /// silently skip cross-part dedup, leading to incorrect query results.
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr query_context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr query_context) override;

private:
    /// The name of the unique projection (from engine param or auto-detected).
    String unique_projection_name;

    /// BeforeTransactionCommitHook implementation.
    std::any onBeforeTransactionCommit(const MutableDataParts & parts, const BeforeCommitHookContext & context);
};

}
