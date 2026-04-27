#include <Processors/Transforms/UniqueProjectionOffsetTransform.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

namespace DB
{

UniqueProjectionOffsetTransform::UniqueProjectionOffsetTransform(
    const Block & header,
    MergedPartOffsetsPtr offsets_,
    size_t part_index_,
    size_t part_starting_offset_,
    DeleteBitmapPtr delete_bitmap_)
    : ISimpleTransform(header, header, /*skip_empty_chunks=*/ true)
    , offsets(std::move(offsets_))
    , part_index(part_index_)
    , part_starting_offset(part_starting_offset_)
    , delete_bitmap(std::move(delete_bitmap_))
    , kv_pos(header.getPositionByName(ProjectionIndexUnique::kv_column_name))
{
}

void UniqueProjectionOffsetTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto columns = chunk.detachColumns();
    columns[kv_pos] = columns[kv_pos]->convertToFullColumnIfSparse();

    auto & outer_tuple = assert_cast<ColumnTuple &>(columns[kv_pos]->assumeMutableRef());
    auto & value_col = outer_tuple.getColumn(1).assumeMutableRef();

    /// Value column is either UInt64 (part_offset) or Tuple(UInt64 version, UInt64 part_offset).
    std::span<UInt64> offset_data;
    if (auto * inner_tuple = typeid_cast<ColumnTuple *>(&value_col))
        offset_data = assert_cast<ColumnUInt64 &>(inner_tuple->getColumn(1).assumeMutableRef()).getData();
    else
        offset_data = assert_cast<ColumnUInt64 &>(value_col).getData();

    /// Build a filter: keep rows whose part_offset is NOT in the delete bitmap.
    /// Simultaneously translate offsets for surviving rows.
    IColumn::Filter filter(offset_data.size(), 1);
    bool has_deleted = false;

    for (size_t j = 0; j < offset_data.size(); ++j)
    {
        if (delete_bitmap && delete_bitmap->contains(offset_data[j]))
        {
            filter[j] = 0;
            has_deleted = true;
            continue;
        }

        if (offsets && offsets->isMappingEnabled())
        {
            size_t filtered_index = offset_data[j];
            if (delete_bitmap)
                filtered_index -= delete_bitmap->rangeCardinality(0, offset_data[j]);
            offset_data[j] = (*offsets)[part_index, filtered_index];
        }
        else if (offsets)
        {
            offset_data[j] += part_starting_offset;
        }
    }

    if (has_deleted)
    {
        size_t remaining = 0;
        for (auto & col : columns)
        {
            col = col->filter(filter, -1);
            remaining = col->size();
        }
        chunk.setColumns(std::move(columns), remaining);
    }
    else
    {
        chunk.setColumns(std::move(columns), num_rows);
    }
}

}
