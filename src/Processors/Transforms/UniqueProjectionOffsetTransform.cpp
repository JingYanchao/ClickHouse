#include <Processors/Transforms/UniqueProjectionOffsetTransform.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>

namespace DB
{

UniqueProjectionOffsetTransform::UniqueProjectionOffsetTransform(
    const Block & header,
    MergedPartOffsetsPtr offsets_,
    size_t part_index_,
    size_t part_starting_offset_)
    : ISimpleTransform(header, header, /*skip_empty_chunks=*/ false)
    , offsets(std::move(offsets_))
    , part_index(part_index_)
    , part_starting_offset(part_starting_offset_)
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

    for (auto & offset : offset_data)
    {
        if (offsets && offsets->isMappingEnabled())
            offset = (*offsets)[part_index, offset];
        else if (offsets)
            offset += part_starting_offset;
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
