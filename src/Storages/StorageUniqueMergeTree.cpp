
#include <Storages/StorageUniqueMergeTree.h>

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeDedupManager.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static std::unique_ptr<MergeTreeSettings> forceEnableBlockNumberColumn(std::unique_ptr<MergeTreeSettings> settings)
{
    /// UniqueMergeTree relies on _block_number virtual column as default version,
    /// so we unconditionally enable its persistence.
    settings->set("enable_block_number_column", Field(true));
    return settings;
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
          forceEnableBlockNumberColumn(std::move(storage_settings_)))
    , unique_projection_name(unique_projection_name_)
{
    /// Default projection name to __unique_index if not specified.
    if (unique_projection_name.empty())
        unique_projection_name = ProjectionIndexUnique::default_projection_name;

    /// Validate that the named unique projection actually exists in metadata.
    const auto & projections = metadata_.getProjections();
    bool found = false;
    for (const auto & projection : projections)
    {
        if (projection.name == unique_projection_name)
        {
            if (!projection.index)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "StorageUniqueMergeTree: projection '{}' exists but has no index in table '{}'",
                    unique_projection_name, getStorageID().getFullTableName());

            auto * idx = dynamic_cast<const ProjectionIndexUnique *>(projection.index.get());
            if (!idx)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "StorageUniqueMergeTree: projection '{}' has index but it is not of TYPE unique in table '{}'",
                    unique_projection_name, getStorageID().getFullTableName());

            found = true;
            LOG_INFO(log, "StorageUniqueMergeTree initialized with unique projection '{}', version column '{}'",
                     unique_projection_name,
                     idx->getVersionColumnName().empty() ? "_block_number" : idx->getVersionColumnName());
            break;
        }
    }

    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "StorageUniqueMergeTree requires a projection named '{}' with TYPE unique, "
            "but it was not found in table '{}' (total projections: {})",
            unique_projection_name, getStorageID().getFullTableName(), projections.size());

    /// Create the dedup manager.
    dedup_manager = std::make_shared<MergeTreeDedupManager>(*this);

    /// Install the pre-lock hook for dedup.
    /// The hook is called by the no-arg Transaction::commit() BEFORE lockParts(),
    /// ensuring lock ordering: UniqueProcessLock -> DataPartsLock (read-friendly).
    /// For callers using commit(DataPartsLock &), dedup is handled externally.
    setTransactionPreLockHook(
        [this](const MutableDataParts & parts, CommitOperation op) -> std::any
        {
            return onPreLock(parts, op);
        });

    /// Create the dedup manager.
}

std::any StorageUniqueMergeTree::onPreLock(
    const MutableDataParts & parts, CommitOperation op)
{
    if (parts.empty())
        return {};

    /// Acquire the UniqueProcessLock to serialize concurrent commit dedup operations.
    /// This ensures that only one thread computes delete bitmaps at a time,
    /// preventing race conditions during cross-part dedup.
    auto unique_lock = dedup_manager->lockUniqueProcess();

    for (const auto & part : parts)
    {
        /// MutableDataPartPtr -> DataPartPtr (implicit const promotion).
        DataPartPtr const_part = part;

        dedup_manager->dedupUniqueIndex(const_part, op);

        LOG_DEBUG(log, "Part '{}': dedup completed, rows_count={}",
                  part->name, part->rows_count);
    }

    /// Apply delete mark bitmaps immediately after dedup, while still holding
    /// the UniqueProcessLock. The caller will then proceed to lockParts() and commit.

    /// Return the UniqueProcessLock wrapped in std::any via shared_ptr.
    /// std::any requires CopyConstructible, but UniqueProcessLock is move-only,
    /// so we wrap it in a shared_ptr to satisfy the constraint.
    /// The caller (Transaction::commit or MergeTreeSink::commitPart) holds this
    /// alive until commit finishes, keeping dedup_mutex locked the entire time.
    return std::make_shared<UniqueProcessLock>(std::move(unique_lock));
}

void StorageUniqueMergeTree::startup()
{
    /// Rebuild delete marks for all existing active parts BEFORE background tasks start.
    /// loadDataParts() was already called by the parent StorageMergeTree constructor,
    /// so all active parts are available. We must rebuild intra-part and cross-part
    /// delete marks from SST files before any merge/mutate can observe incomplete state.
    dedup_manager->rebuildAllDeleteMarks();

    LOG_INFO(log, "Delete marks rebuilt for all active parts, starting background tasks");

    /// Now start background tasks (merge, mutate, moves, etc.).
    /// All subsequent operations will see correct delete marks.
    StorageMergeTree::startup();
}

}
