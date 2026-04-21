
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
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

#include <base/unaligned.h>
#include <algorithm>
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
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int UNKNOWN_IDENTIFIER;
}

/// Serialize all key columns for all rows into a single ColumnString.
/// Uses comparable serialization when available, normal serialization otherwise.
static ColumnString::MutablePtr serializeAllKeysIntoColumn(
    const std::vector<const IColumn *> & key_col_ptrs,
    size_t num_rows)
{
    std::vector<bool> col_comparable(key_col_ptrs.size());
    for (size_t c = 0; c < key_col_ptrs.size(); ++c)
        col_comparable[c] = key_col_ptrs[c]->supportsSerializeValueIntoMemoryAsComparable();

    PaddedPODArray<UInt64> key_sizes(num_rows, 0);
    UInt64 comparable_total = 0;
    for (size_t c = 0; c < key_col_ptrs.size(); ++c)
    {
        if (col_comparable[c])
            comparable_total += key_col_ptrs[c]->computeComparableRowFormatSize();
        else
            key_col_ptrs[c]->collectSerializedValueSizes(key_sizes, nullptr, nullptr);
    }

    auto key_column = ColumnString::create();
    auto & chars = key_column->getChars();
    auto & offsets = key_column->getOffsets();

    UInt64 total_estimated = comparable_total + std::reduce(key_sizes.begin(), key_sizes.end(), UInt64(0));
    chars.resize(total_estimated);

    offsets.resize(num_rows);

    UInt64 actual_total = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        char * pos = reinterpret_cast<char *>(chars.data() + actual_total);

        for (size_t c = 0; c < key_col_ptrs.size(); ++c)
        {
            if (col_comparable[c])
                pos = key_col_ptrs[c]->serializeValueIntoMemoryAsComparableRowFormat(i, pos);
            else
                pos = key_col_ptrs[c]->serializeValueIntoMemory(i, pos, nullptr);
        }

        actual_total = pos - reinterpret_cast<char *>(chars.data());
        offsets[i] = actual_total;

        chassert(actual_total <= total_estimated);
    }

    chars.resize(actual_total);

    return key_column;
}

/// UniqueValueEntry<false> (non-versioned): 8-byte big-endian layout [part_offset].
template <>
String UniqueValueEntry<false>::encode() const
{
    String result(sizeof(UInt64), '\0');
    unalignedStoreBigEndian<UInt64>(result.data(), part_offset);
    return result;
}

template <>
UniqueValueEntry<false> UniqueValueEntry<false>::decode(const char * data, size_t size)
{
    if (size != sizeof(UInt64))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Non-versioned UniqueValueEntry expects {} bytes, got {}", sizeof(UInt64), size);
    return UniqueValueEntry<false>{{}, unalignedLoadBigEndian<UInt64>(data)};
}

/// UniqueValueEntry<true> (versioned): 16-byte big-endian layout [version | part_offset].
template <>
String UniqueValueEntry<true>::encode() const
{
    String result(2 * sizeof(UInt64), '\0');
    unalignedStoreBigEndian<UInt64>(result.data(), version);
    unalignedStoreBigEndian<UInt64>(result.data() + sizeof(UInt64), part_offset);
    return result;
}

template <>
UniqueValueEntry<true> UniqueValueEntry<true>::decode(const char * data, size_t size)
{
    if (size != 2 * sizeof(UInt64))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Versioned UniqueValueEntry expects {} bytes, got {}", 2 * sizeof(UInt64), size);
    UniqueValueEntry<true> entry;
    entry.version = unalignedLoadBigEndian<UInt64>(data);
    entry.part_offset = unalignedLoadBigEndian<UInt64>(data + sizeof(UInt64));
    return entry;
}

/// Runtime decode: auto-detects format by buffer size.
UniqueValueEntryFull decodeUniqueValueEntry(const char * data, size_t size)
{
    UniqueValueEntryFull entry;
    if (size == 2 * sizeof(UInt64))
    {
        entry.version = unalignedLoadBigEndian<UInt64>(data);
        entry.part_offset = unalignedLoadBigEndian<UInt64>(data + sizeof(UInt64));
    }
    else if (size == sizeof(UInt64))
    {
        entry.part_offset = unalignedLoadBigEndian<UInt64>(data);
    }
    else
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unique projection SST value has unexpected size: expected {} or {} bytes, got {}",
            sizeof(UInt64), 2 * sizeof(UInt64), size);
    }
    return entry;
}

ProjectionIndexUnique::ProjectionIndexUnique(ASTPtr key_expression_list_, String version_column_name_)
    : version_column_name(std::move(version_column_name_))
{
    if (!key_expression_list_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unique projection index requires at least one key expression");

    const auto * expr_list = key_expression_list_->as<ASTExpressionList>();
    if (!expr_list || expr_list->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unique projection index requires at least one key expression");

    /// Park the raw AST inside `unique_key_desc`. `fillProjectionDescription`
    /// will later overwrite the whole struct with the compiled key (expression,
    /// sample_block, column_names, ...) once it has access to the parent columns
    /// and query context.
    unique_key_desc.expression_list_ast = std::move(key_expression_list_);
}

Names ProjectionIndexUnique::getUniqueKeyColumns() const
{
    if (unique_key_desc.expression)
        return unique_key_desc.expression->getRequiredColumns();
    return {};
}

ProjectionIndexPtr ProjectionIndexUnique::create(const ASTProjectionDeclaration & proj)
{
    /// Keep the raw key expression list. We intentionally do NOT restrict keys to
    /// simple identifiers here — arbitrary deterministic expressions are allowed
    /// (e.g. `x + y`, `lower(name)`). The final validation (column existence,
    /// result types, Nullable rejection, etc.) is performed later in
    /// `fillProjectionDescription` where we have access to the columns of the
    /// parent table.
    if (!proj.index)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unique projection index requires INDEX expression with key columns");

    ASTPtr key_expression_list;
    if (proj.index->as<ASTExpressionList>())
    {
        key_expression_list = proj.index->clone();
    }
    else
    {
        /// Defensive branch: the parser for `PROJECTION ... INDEX ...` always produces
        /// an ASTExpressionList, but wrap a single expression just in case.
        auto wrapper = make_intrusive<ASTExpressionList>();
        wrapper->children.push_back(proj.index->clone());
        key_expression_list = wrapper;
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

    return std::make_shared<ProjectionIndexUnique>(std::move(key_expression_list), std::move(version_col));
}

void ProjectionIndexUnique::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * /*index_expr*/,
    const ColumnsDescription & columns,
    ContextPtr query_context) const
{
    chassert(result.index.get() == this);

    /// Compile the key expression list into a full `KeyDescription`.
    /// Translate `UNKNOWN_IDENTIFIER` to `NO_SUCH_COLUMN_IN_TABLE` so that
    /// `AlterCommands::apply` correctly rejects DROP/RENAME of referenced columns.
    try
    {
        unique_key_desc = KeyDescription::getKeyFromAST(unique_key_desc.expression_list_ast, columns, query_context);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_IDENTIFIER)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "{}", e.message());
        throw;
    }

    Names unique_key_source_columns = unique_key_desc.expression->getRequiredColumns();

    const bool has_version = !version_column_name.empty();
    NameAndTypePair version_name_and_type;
    if (has_version)
    {
        version_name_and_type = columns.getPhysical(version_column_name);
        /// The versioned SortedStringKV layout encodes version as a fixed-width 8-byte
        /// big-endian UInt64 (see `UniqueValueEntry<true>::encode`). Anything other
        /// than UInt64 would silently corrupt the encoding, so reject it up front.
        if (!WhichDataType(version_name_and_type.type).isUInt64())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unique projection version column {} must have type UInt64, got {}",
                version_column_name,
                version_name_and_type.type->getName());
    }

    /// 1. Build the SortedStringKV data type.
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

    /// 3. Record required columns: physical columns referenced by the key
    ///    expressions (and the version column if present).
    result.required_columns = unique_key_source_columns;
    if (has_version
        && std::find(result.required_columns.begin(), result.required_columns.end(), version_column_name)
            == result.required_columns.end())
    {
        result.required_columns.push_back(version_column_name);
    }

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

    /// sample_block_for_keys exposes the (post-expression) key column types plus
    /// the optional version column. AlterCommands compares this block across old
    /// and new projections via blocksHaveEqualStructure, so any change in key
    /// expression result types (e.g. caused by MODIFY COLUMN on a referenced
    /// physical column) will correctly trigger ALTER_OF_COLUMN_IS_FORBIDDEN.
    for (size_t i = 0; i < unique_key_desc.sample_block.columns(); ++i)
    {
        const auto & col = unique_key_desc.sample_block.getByPosition(i);
        result.sample_block_for_keys.insert({nullptr, col.type, col.name});
    }
    if (has_version)
        result.sample_block_for_keys.insert({nullptr, version_name_and_type.type, version_name_and_type.name});

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

    /// Evaluate key expressions.
    Block key_block = block;
    if (unique_key_desc.expression)
        unique_key_desc.expression->execute(key_block);

    std::vector<const IColumn *> key_col_ptrs;
    key_col_ptrs.reserve(unique_key_desc.column_names.size());
    for (const auto & col_name : unique_key_desc.column_names)
        key_col_ptrs.push_back(key_block.getByName(col_name).column.get());

    auto serialized_keys = serializeAllKeysIntoColumn(key_col_ptrs, num_rows);

    const ColumnUInt64 * version_col = nullptr;
    if (with_version)
    {
        const auto & ver_col_with_type = block.getByName(version_column_name);
        version_col = assert_cast<const ColumnUInt64 *>(ver_col_with_type.column.get());
    }

    /// Invert perm_ptr (sorted_pos -> original_row_idx) to get original_row_idx -> part_offset.
    std::vector<UInt64> map_part_offsets;
    if (perm_ptr)
    {
        map_part_offsets.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k)
            map_part_offsets[(*perm_ptr)[k]] = starting_offset + k;
    }

    /// Sort by serialized key to group identical keys together.
    std::vector<size_t> indices(num_rows);
    std::iota(indices.begin(), indices.end(), 0);

    ::pdqsort(
        indices.begin(), indices.end(), [&](size_t a, size_t b) { return serialized_keys->getDataAt(a) < serialized_keys->getDataAt(b); });

    /// Deduplicate: for each group of identical keys, keep the row with
    /// the largest version (or largest part_offset as tiebreaker).
    auto key_column = ColumnString::create();
    key_column->reserve(num_rows);

    const auto & sample = projection_desc.sample_block;
    chassert(sample.columns() == 1);
    const auto & sample_tuple = assert_cast<const ColumnTuple &>(*sample.getByPosition(0).column);
    auto value_column_mut = sample_tuple.getColumn(1).cloneEmpty();

    const auto * ver_data = version_col ? version_col->getData().data() : nullptr;

    auto get_part_offset = [&](size_t row_idx) -> UInt64
    {
        if (perm_ptr)
            return map_part_offsets[row_idx];
        return starting_offset + row_idx;
    };

    auto dedup_and_write = [&]<ValueType V>()
    {
        using Entry = typename ValueTraits<V>::Entry;
        size_t last_max_key_idx = indices[0];
        for (size_t i = 1; i <= indices.size(); ++i)
        {
            if (i == indices.size() || serialized_keys->getDataAt(indices[i]) != serialized_keys->getDataAt(last_max_key_idx))
            {
                key_column->insertFrom(*serialized_keys, last_max_key_idx);

                Entry entry;
                if constexpr (ValueTraits<V>::has_version)
                    entry.version = ver_data[last_max_key_idx];
                entry.part_offset = get_part_offset(last_max_key_idx);
                ValueTraits<V>::writeEntry(*value_column_mut, entry);

                if (i < indices.size())
                    last_max_key_idx = indices[i];
            }
            else
            {
                const size_t cur_idx = indices[i];
                if constexpr (ValueTraits<V>::has_version)
                {
                    if (ver_data[cur_idx] > ver_data[last_max_key_idx]
                        || (ver_data[cur_idx] == ver_data[last_max_key_idx]
                            && get_part_offset(cur_idx) > get_part_offset(last_max_key_idx)))
                        last_max_key_idx = cur_idx;
                }
                else
                {
                    if (get_part_offset(cur_idx) > get_part_offset(last_max_key_idx))
                        last_max_key_idx = cur_idx;
                }
            }
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
