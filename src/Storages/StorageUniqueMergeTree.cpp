
#include <Storages/StorageUniqueMergeTree.h>

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

    dedup_manager = std::make_shared<MergeTreeDedupPartManager>(*this);

    setBeforeTransactionCommitHook(
        [this](const MutableDataParts & parts, const BeforeCommitHookContext & context) -> std::any
        {
            return onBeforeTransactionCommit(parts, context);
        });

    has_lightweight_delete_parts.store(true);
}

std::any StorageUniqueMergeTree::onBeforeTransactionCommit(
    const MutableDataParts & parts, const BeforeCommitHookContext & before_commit_context)
{
    /// Serialize concurrent commits: only one thread computes delete bitmaps at a time.
    auto unique_lock = dedup_manager->lockUniqueProcess();

    DeleteMarkSnapshotMap delete_mark_snapshots;
    std::optional<DeleteMarkSnapshotMap> opt_snapshots;
    if (before_commit_context.data.has_value())
    {
        delete_mark_snapshots = std::any_cast<DeleteMarkSnapshotMap>(before_commit_context.data);
        opt_snapshots = std::move(delete_mark_snapshots);
    }

    for (const auto & part : parts)
    {
        dedup_manager->dedupPart(part, opt_snapshots);
    }

    /// Wrap in shared_ptr because std::any requires CopyConstructible,
    /// but UniqueProcessLock is move-only.
    return std::make_shared<UniqueProcessLock>(std::move(unique_lock));
}

void StorageUniqueMergeTree::startup()
{
    /// Rebuild delete marks for all active parts before background tasks start,
    /// so that merges/mutations observe correct dedup state.
    dedup_manager->buildAllDeleteMarksOnStartup();

    LOG_INFO(log, "Delete marks rebuilt for all active parts, starting background tasks");

    StorageMergeTree::startup();
}

}
