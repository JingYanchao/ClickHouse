#pragma once

#include <Processors/ISimpleTransform.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>

namespace DB
{

/// Translates embedded part_offset values inside _unique_kv entries during
/// unique projection merge.
///
/// For each row, extracts part_offset from the SST value column and translates
/// it via MergedPartOffsets so the offsets refer to the merged parent part.
class UniqueProjectionOffsetTransform : public ISimpleTransform
{
public:
    UniqueProjectionOffsetTransform(
        const Block & header,
        MergedPartOffsetsPtr offsets_,
        size_t part_index_,
        size_t part_starting_offset_);

    String getName() const override { return "UniqueProjectionOffsetTransform"; }

    void transform(Chunk & chunk) override;

private:
    MergedPartOffsetsPtr offsets;
    size_t part_index;
    size_t part_starting_offset;
    size_t kv_pos;
};

}
