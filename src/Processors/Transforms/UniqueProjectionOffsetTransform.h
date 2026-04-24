#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>

namespace DB
{

/// Filters and translates _unique_kv entries during unique projection merge.
///
/// For each row, extracts part_offset from the SST value, checks the parent
/// part's delete bitmap, and either drops the row (deleted) or translates
/// the offset via MergedPartOffsets.
///
/// This replaces the normal _row_exists / FilterTransform path which cannot
/// work for projection parts (projection rows don't correspond 1:1 to parent
/// rows by sequential position).
class UniqueProjectionOffsetTransform : public ISimpleTransform
{
public:
    UniqueProjectionOffsetTransform(
        const Block & header,
        MergedPartOffsetsPtr offsets_,
        size_t part_index_,
        size_t part_starting_offset_,
        DeleteBitmapPtr delete_bitmap_);

    String getName() const override { return "UniqueProjectionOffsetTransform"; }

    void transform(Chunk & chunk) override;

private:
    MergedPartOffsetsPtr offsets;
    size_t part_index;
    size_t part_starting_offset;
    DeleteBitmapPtr delete_bitmap;
    size_t kv_pos;
};

}
