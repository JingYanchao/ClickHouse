
#include <Storages/StorageUniqueMergeTree.h>

#include <Storages/MergeTree/MergeTreeDedupPartManager.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Storages/MutationCommands.h>
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
                "StorageUniqueMergeTree: projection '{}' exists but has no index in table '{}'",
                projection_name, full_table_name);

        const auto * idx = dynamic_cast<const ProjectionIndexUnique *>(projection.index.get());
        if (!idx)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "StorageUniqueMergeTree: projection '{}' has index but it is not of TYPE unique in table '{}'",
                projection_name, full_table_name);

        return;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "StorageUniqueMergeTree requires a projection named '{}' with TYPE unique, "
        "but it was not found in table '{}' (total projections: {})",
        projection_name, full_table_name, projections.size());
}

/// Reject regular (non-index) projections for UniqueMergeTree engines.
/// Only projection indexes (with TYPE clause) are allowed because regular
/// projections cannot maintain consistency with the dedup delete bitmap.
static void validateNoRegularProjections(
    const ProjectionsDescription & projections,
    const String & full_table_name)
{
    for (const auto & projection : projections)
    {
        if (!projection.index)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Regular projections are not supported for UniqueMergeTree. "
                "Only projection indexes (with TYPE clause) are allowed. "
                "Projection '{}' in table '{}' does not have a TYPE clause",
                projection.name, full_table_name);
    }
}

StorageUniqueMergeTree::StorageUniqueMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    const String & unique_projection_name_)
    : StorageMergeTree(
          table_id_,
          relative_data_path_,
          metadata_,
          mode,
          context_,
          date_column_name,
          merging_params_,
          std::move(storage_settings_))
    , unique_projection_name(unique_projection_name_)
{
    if (unique_projection_name.empty())
        unique_projection_name = ProjectionIndexUnique::default_projection_name;

    validateUniqueProjection(
        unique_projection_name, metadata_.getProjections(), getStorageID().getFullTableName());

    validateNoRegularProjections(
        metadata_.getProjections(), getStorageID().getFullTableName());

    dedup_manager = std::make_shared<MergeTreeDedupPartManager>(*this);

    setBeforeTransactionCommitHook(
        [this](const MutableDataParts & parts, const BeforeCommitHookContext & context) -> std::any
        {
            return dedup_manager->dedupPartsBeforeTransactionCommit(parts, context);
        });

    has_lightweight_delete_parts.store(true);
}

void StorageUniqueMergeTree::startup()
{
    /// Load persisted delete bitmaps from RocksDB, then check and re-dedup
    /// parts whose bitmaps were not persisted (e.g. due to unexpected restart).
    dedup_manager->loadDeleteBitmapsAndCheckOnStartup();

    LOG_INFO(log, "Delete bitmaps checked/rebuilt for all active parts, starting background tasks");

    StorageMergeTree::startup();
}

void StorageUniqueMergeTree::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    StorageMergeTree::checkMutationIsPossible(commands, settings);

    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::DELETE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "ALTER DELETE is not supported for UniqueMergeTree. "
                "Row-level deletion bypasses the unique projection SST and would corrupt dedup");
    }
}

void StorageUniqueMergeTree::dropPart(
    const String & part_name, bool /*detach*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "DROP PART is not supported for UniqueMergeTree (part: {}). "
        "Dropping a single part would leave stale delete marks on other parts. "
        "Use DROP PARTITION instead to remove an entire partition atomically",
        part_name);
}

void StorageUniqueMergeTree::replacePartitionFrom(
    const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "REPLACE PARTITION is not supported for UniqueMergeTree. "
        "The operation bypasses the dedup commit hook and would produce incorrect results");
}

void StorageUniqueMergeTree::movePartitionToTable(
    const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "MOVE PARTITION TO TABLE is not supported for UniqueMergeTree. "
        "The operation bypasses the dedup commit hook and would produce incorrect results");
}

void StorageUniqueMergeTree::attachRestoredParts(MutableDataPartsVector && /*parts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "RESTORE is not supported for UniqueMergeTree. "
        "The operation bypasses the dedup commit hook and would produce incorrect results");
}

}
