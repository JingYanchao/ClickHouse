
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/KeyDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <DataTypes/Serializations/SerializationSortedStringKV.h>

#include <Common/logger_useful.h>
#include <algorithm>
#include <bit>
#include <numeric>
#include <pdqsort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/// Serialize all key columns for all rows directly into a ColumnString.
/// Uses column-major batch serialization for better CPU cache locality.
static ColumnString::MutablePtr serializeAllKeysIntoColumn(
    const std::vector<const IColumn *> & key_col_ptrs,
    size_t num_rows)
{
    PaddedPODArray<UInt64> key_sizes(num_rows, 0);
    for (const auto * col : key_col_ptrs)
        col->collectSerializedValueSizes(key_sizes, nullptr, nullptr);

    auto key_column = ColumnString::create();
    auto & chars = key_column->getChars();
    auto & offsets = key_column->getOffsets();

    UInt64 total_chars = 0;
    offsets.resize(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        total_chars += key_sizes[i];
        offsets[i] = total_chars;
    }
    chars.resize(total_chars);

    VectorWithMemoryTracking<char *> memories(num_rows);
    memories[0] = reinterpret_cast<char *>(chars.data());
    for (size_t i = 1; i < num_rows; ++i)
        memories[i] = reinterpret_cast<char *>(chars.data() + offsets[i - 1]);

    for (const auto * col : key_col_ptrs)
        col->batchSerializeValueIntoMemory(memories, nullptr);

    for (size_t i = 0; i < num_rows; ++i)
        chassert(memories[i] == reinterpret_cast<char *>(chars.data() + offsets[i]));

    return key_column;
}

/// UniqueValueEntry<false> (non-versioned): 8-byte layout.
template <>
String UniqueValueEntry<false>::encode() const
{
    String result(8, '\0');
    UInt64 offset_be = std::byteswap(part_offset);
    memcpy(result.data(), &offset_be, 8);
    return result;
}

template <>
UniqueValueEntry<false> UniqueValueEntry<false>::decode(const char * data, size_t size)
{
    if (size != 8)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Non-versioned UniqueValueEntry expects 8 bytes, got {}", size);
    UInt64 offset_be;
    memcpy(&offset_be, data, 8);
    return UniqueValueEntry<false>{{}, std::byteswap(offset_be)};
}

/// UniqueValueEntry<true> (versioned): 16-byte layout.
template <>
String UniqueValueEntry<true>::encode() const
{
    String result(16, '\0');
    UInt64 version_be = std::byteswap(version);
    UInt64 offset_be = std::byteswap(part_offset);
    memcpy(result.data(), &version_be, 8);
    memcpy(result.data() + 8, &offset_be, 8);
    return result;
}

template <>
UniqueValueEntry<true> UniqueValueEntry<true>::decode(const char * data, size_t size)
{
    if (size != 16)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Versioned UniqueValueEntry expects 16 bytes, got {}", size);
    UInt64 version_be;
    UInt64 offset_be;
    memcpy(&version_be, data, 8);
    memcpy(&offset_be, data + 8, 8);
    UniqueValueEntry<true> entry;
    entry.version = std::byteswap(version_be);
    entry.part_offset = std::byteswap(offset_be);
    return entry;
}

/// Runtime decode: auto-detects format by size (8 or 16 bytes).
UniqueValueEntryFull decodeUniqueValueEntry(const char * data, size_t size)
{
    if (size == 16)
    {
        UInt64 version_be;
        UInt64 offset_be;
        memcpy(&version_be, data, 8);
        memcpy(&offset_be, data + 8, 8);
        UniqueValueEntryFull entry;
        entry.version = std::byteswap(version_be);
        entry.part_offset = std::byteswap(offset_be);
        return entry;
    }
    else if (size == 8)
    {
        UInt64 offset_be;
        memcpy(&offset_be, data, 8);
        UniqueValueEntryFull entry;
        entry.part_offset = std::byteswap(offset_be);
        return entry;
    }
    else
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unique projection SST value has unexpected size: expected 8 or 16 bytes, got {}", size);
    }
}

ProjectionIndexUnique::ProjectionIndexUnique(Names unique_key_columns_, String version_column_name_)
    : unique_key_columns(std::move(unique_key_columns_))
    , version_column_name(std::move(version_column_name_))
{
    if (unique_key_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unique projection index requires at least one key column");
}

ProjectionIndexPtr ProjectionIndexUnique::create(const ASTProjectionDeclaration & proj)
{
    /// Extract unique key column names from the INDEX expression.
    if (!proj.index)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unique projection index requires INDEX expression with key columns");

    /// Collect leaf identifiers from the INDEX expression.
    ASTs leaves;
    if (const auto * expr_list = proj.index->as<ASTExpressionList>())
        leaves = expr_list->children;
    else if (const auto * func = proj.index->as<ASTFunction>(); func && func->name == "tuple" && func->arguments)
        leaves = func->arguments->children;
    else
        leaves.push_back(proj.index);

    Names key_columns;
    key_columns.reserve(leaves.size());
    for (const auto & leaf : leaves)
    {
        if (const auto * id = leaf->as<ASTIdentifier>())
            key_columns.push_back(id->name());
        else
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Unique projection index key columns must be simple identifiers, got: {}",
                leaf->formatForErrorMessage());
    }

    /// Extract optional version column from TYPE unique('ver') arguments.
    String version_col;
    if (proj.type && proj.type->arguments && !proj.type->arguments->children.empty())
    {
        if (const auto * lit = proj.type->arguments->children[0]->as<ASTLiteral>())
        {
            if (lit->value.getType() == Field::Types::String)
                version_col = lit->value.safeGet<String>();
        }
    }

    return std::make_shared<ProjectionIndexUnique>(std::move(key_columns), std::move(version_col));
}

void ProjectionIndexUnique::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * /*index_expr*/,
    const ColumnsDescription & /*columns*/,
    ContextPtr query_context) const
{
    chassert(result.index.get() == this);
    /// 1. Build the SortedStringKV data type.
    const bool has_version = !version_column_name.empty();
    const auto kv_value_type = has_version
        ? ValueType::VersionedPartOffset
        : ValueType::PartOffset;
    /// Each ValueType maps to a registered DataType name.
    String type_name;
    switch (kv_value_type)
    {
        case ValueType::PartOffset:
            type_name = "SortedStringKV";
            break;
        case ValueType::VersionedPartOffset:
            type_name = "VersionedSortedStringKV";
            break;
    }
    auto sorted_kv_type = DataTypeFactory::instance().get(type_name);

    /// 2. Build sample_block with a single kv column.
    result.sample_block.clear();
    result.sample_block.insert(ColumnWithTypeAndName{sorted_kv_type->createColumn(), sorted_kv_type, kv_column_name});

    /// 3. Record required columns: unique key columns (and version column if present)
    result.required_columns = unique_key_columns;
    if (has_version)
        result.required_columns.push_back(version_column_name);

    /// 4. Mark that this projection uses _parent_part_offset.
    ///    During merge, if merge_may_reduce_rows, the projection is rebuilt
    ///    via `calculate`; otherwise it is merged via AggregatingSortedAlgorithm
    ///    with max() aggregation, and offsets are translated via MergedPartOffsets.
    result.with_parent_part_offset = true;

    /// 5. Aggregate type: SortedStringKV uses SimpleAggregateFunction(max, ...)
    ///    for value deduplication during merge.
    result.type = ProjectionDescription::Type::Aggregate;
    result.key_size = 1;

    /// 6. Build metadata: columns, sorting key, primary key
    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    /// Build ColumnsDescription from sample_block first (needed for getSortingKeyFromAST)
    NamesAndTypesList metadata_columns;
    for (const auto & col : result.sample_block)
        metadata_columns.emplace_back(col.name, col.type);
    ColumnsDescription projection_columns(metadata_columns);

    /// Sorting key: tupleElement(unique_kv, 1) extracts the key part.
    /// We construct the AST explicitly because ASTIdentifier-based approaches
    /// fail with SerializationSortedStringKV's subcolumn resolution.
    auto sorting_key_args = make_intrusive<ASTExpressionList>();
    sorting_key_args->children.push_back(make_intrusive<ASTIdentifier>(kv_column_name));
    sorting_key_args->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(1))));
    auto sorting_key_ast = make_intrusive<ASTFunction>();
    sorting_key_ast->name = "tupleElement";
    sorting_key_ast->arguments = sorting_key_args;
    sorting_key_ast->children.push_back(sorting_key_args);
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(sorting_key_ast, projection_columns, query_context, {});

    /// Primary key: empty — we don't need primary key index for unique projection
    metadata.primary_key = KeyDescription::buildEmptyKey();

    metadata.setColumns(projection_columns);
    /// Also fill sample_block_for_keys (used by Aggregate projections for GROUP BY keys)
    result.sample_block_for_keys.insert({nullptr, sorted_kv_type, kv_column_name});
    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
}

Block ProjectionIndexUnique::calculate(
    const ProjectionDescription & projection_desc,
    const Block & block,
    UInt64 starting_offset,
    ContextPtr /*context*/,
    const IColumnPermutation * perm_ptr) const
{
    const size_t num_rows = block.rows();
    if (num_rows == 0)
        return projection_desc.sample_block.cloneEmpty();

    const bool with_version = !version_column_name.empty();

    /// 1. Serialize unique key columns into a ColumnString.
    std::vector<const IColumn *> key_col_ptrs;
    key_col_ptrs.reserve(unique_key_columns.size());
    for (const auto & col_name : unique_key_columns)
        key_col_ptrs.push_back(block.getByName(col_name).column.get());

    auto all_keys = serializeAllKeysIntoColumn(key_col_ptrs, num_rows);

    /// 2. Build per-row metadata: version and part_offset.
    ///    We store these in parallel arrays indexed by original row index.
    const ColumnUInt64 * version_col = nullptr;
    if (with_version)
    {
        const auto & ver_col_with_type = block.getByName(version_column_name);
        version_col = assert_cast<const ColumnUInt64 *>(ver_col_with_type.column.get());
    }

    /// When perm_ptr is present (INSERT path), compute the inverse permutation:
    /// perm_ptr maps sorted_pos -> original_pos, but we need original_pos -> sorted_pos
    /// because part_offset must reflect the row's position in the sorted part on disk.
    /// When perm_ptr is absent, part_offset is a simple linear function of row index,
    /// so we avoid allocating the array entirely.
    std::vector<UInt64> part_offsets;
    auto get_part_offset = [&](size_t row_idx) -> UInt64
    {
        if (perm_ptr)
            return part_offsets[row_idx];
        return starting_offset + row_idx;
    };
    if (perm_ptr)
    {
        part_offsets.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k)
            part_offsets[(*perm_ptr)[k]] = starting_offset + k;
    }

    /// 3. Sort by key (lexicographic), then by (version, part_offset) descending
    ///    so that the first occurrence of each key is the winner.
    std::vector<size_t> indices(num_rows);
    std::iota(indices.begin(), indices.end(), 0);

    std::vector<std::string_view> key_refs(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        key_refs[i] = all_keys->getDataAt(i);

    const auto * ver_data = version_col ? version_col->getData().data() : nullptr;

    /// Split sort comparator by version mode to eliminate per-comparison branch.
    if (with_version)
    {
        ::pdqsort(indices.begin(), indices.end(), [&](size_t a, size_t b)
        {
            int cmp = key_refs[a].compare(key_refs[b]);
            if (cmp != 0)
                return cmp < 0;
            /// Same key: larger (version, part_offset) comes first.
            if (ver_data[a] != ver_data[b])
                return ver_data[a] > ver_data[b];
            return get_part_offset(a) > get_part_offset(b);
        });
    }
    else
    {
        ::pdqsort(indices.begin(), indices.end(), [&](size_t a, size_t b)
        {
            int cmp = key_refs[a].compare(key_refs[b]);
            if (cmp != 0)
                return cmp < 0;
            /// Same key: larger part_offset comes first.
            return get_part_offset(a) > get_part_offset(b);
        });
    }

    /// 4. Build the output block with a single SortedStringKV column.
    ///    Deduplicate inline: keep only the first index for each unique key
    ///    and write directly into the output columns, avoiding an intermediate vector.
    auto key_column = ColumnString::create();
    key_column->reserve(num_rows); /// upper bound; exact count unknown before dedup

    const auto & sample = projection_desc.sample_block;
    chassert(sample.columns() == 1);
    const auto & sample_tuple = assert_cast<const ColumnTuple &>(*sample.getByPosition(0).column);
    auto value_column_mut = sample_tuple.getColumn(1).cloneEmpty();

    /// Runtime-to-compile-time dispatch: deduplicate and write entries in a single pass.
    auto dedup_and_write = [&]<ValueType V>()
    {
        using Entry = typename ValueTraits<V>::Entry;
        for (size_t i = 0; i < indices.size(); ++i)
        {
            if (i > 0 && key_refs[indices[i]] == key_refs[indices[i - 1]])
                continue;

            const size_t idx = indices[i];
            key_column->insertFrom(*all_keys, idx);

            Entry entry;
            if constexpr (ValueTraits<V>::has_version)
                entry.version = ver_data[idx];
            entry.part_offset = get_part_offset(idx);
            ValueTraits<V>::writeEntry(*value_column_mut, entry);
        }
    };

    if (with_version)
        dedup_and_write.template operator()<ValueType::VersionedPartOffset>();
    else
        dedup_and_write.template operator()<ValueType::PartOffset>();

    auto tuple_column = ColumnTuple::create(Columns{std::move(key_column), std::move(value_column_mut)});

    Block result;
    result.insert(ColumnWithTypeAndName{std::move(tuple_column), sample.getByPosition(0).type, sample.getByPosition(0).name});

    return result;
}

std::shared_ptr<MergeTreeSettings> ProjectionIndexUnique::getDefaultSettings() const
{
    auto settings = std::make_shared<MergeTreeSettings>();
    settings->set("allow_tuple_element_aggregation", true);
    return settings;
}

}
