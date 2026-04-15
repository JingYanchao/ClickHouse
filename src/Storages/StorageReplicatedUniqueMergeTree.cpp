
#include <Storages/StorageReplicatedUniqueMergeTree.h>

#include <Storages/MergeTree/MergeTreeDedupPartManager.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Storages/ProjectionsDescription.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

/// Validate that the named unique projection exists in metadata and has the correct type.
/// Throws on failure.
static void validateUniqueProjection(
    const String & projection_name,
    const ProjectionsDescription & projections,
    const String & full_table_name)
{
    for (const auto & projection : projections)
    {
        if (projection.name != projection_name)
            continue;

        if (!projection.index)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "StorageReplicatedUniqueMergeTree: projection '{}' exists but has no index in table '{}'",
                projection_name, full_table_name);

        const auto * idx = dynamic_cast<const ProjectionIndexUnique *>(projection.index.get());
        if (!idx)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "StorageReplicatedUniqueMergeTree: projection '{}' has index but it is not of TYPE unique in table '{}'",
                projection_name, full_table_name);

        return;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "StorageReplicatedUniqueMergeTree requires a projection named '{}' with TYPE unique, "
        "but it was not found in table '{}' (total projections: {})",
        projection_name, full_table_name, projections.size());
}

StorageReplicatedUniqueMergeTree::StorageReplicatedUniqueMergeTree(
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
    const String & unique_projection_name_)
    : StorageReplicatedMergeTree(
          zookeeper_info_,
          mode,
          table_id_,
          relative_data_path_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(settings_),
          need_check_structure,
          create_query_zookeeper_retries_info_)
    , unique_projection_name(unique_projection_name_)
{
    if (unique_projection_name.empty())
        unique_projection_name = ProjectionIndexUnique::default_projection_name;

    validateUniqueProjection(
        unique_projection_name, metadata_.getProjections(), getStorageID().getFullTableName());

    dedup_manager = std::make_shared<MergeTreeDedupPartManager>(*this);

    setBeforeTransactionCommitHook(
        [this](const MutableDataParts & parts, const BeforeCommitHookContext & context) -> std::any
        {
            return dedup_manager->dedupPartsBeforeTransactionCommit(parts, context);
        });

    has_lightweight_delete_parts.store(true);
}

void StorageReplicatedUniqueMergeTree::dropPart(
    const String & part_name, bool /*detach*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "DROP PART is not supported for ReplicatedUniqueMergeTree (part: {}). "
        "Dropping a single part would leave stale delete marks on other parts. "
        "Use DROP PARTITION instead to remove an entire partition atomically",
        part_name);
}

void StorageReplicatedUniqueMergeTree::replacePartitionFrom(
    const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "REPLACE PARTITION is not supported for ReplicatedUniqueMergeTree. "
        "The operation bypasses the dedup commit hook and would produce incorrect results");
}

void StorageReplicatedUniqueMergeTree::movePartitionToTable(
    const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "MOVE PARTITION TO TABLE is not supported for ReplicatedUniqueMergeTree. "
        "The operation bypasses the dedup commit hook and would produce incorrect results");
}
}
