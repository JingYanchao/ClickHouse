
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexUnique.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/KeyDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

#include <Common/logger_useful.h>
#include <bit>
#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;

}

String ProjectionIndexUnique::serializeKeyColumns(const Block & block, const Names & key_columns, size_t row_index)
{
    WriteBufferFromOwnString buf;
    for (const auto & col_name : key_columns)
    {
        const auto & col = block.getByName(col_name);
        col.type->getDefaultSerialization()->serializeBinary(*col.column, row_index, buf, {});
    }
    return buf.str();
}

String UniqueValueEntry::encode() const
{
    /// Big-endian encoding so that lexicographic string comparison == numeric comparison.
    String result(8, '\0');
    UInt64 offset_be = std::byteswap(part_offset);
    memcpy(result.data(), &offset_be, 8);
    return result;
}

UniqueValueEntry UniqueValueEntry::decode(const char * data, size_t size)
{
    if (size < 8)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unique projection SST value too short: expected 8 bytes, got {}", size);

    UInt64 offset_be;
    memcpy(&offset_be, data, 8);
    return UniqueValueEntry{std::byteswap(offset_be)};
}

ProjectionIndexUnique::ProjectionIndexUnique(Names unique_key_columns_)
    : unique_key_columns(std::move(unique_key_columns_))
{
    if (unique_key_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unique projection index requires at least one key column");
}

ProjectionIndexPtr ProjectionIndexUnique::create(const ASTProjectionDeclaration & proj)
{
    /// Extract unique key column names from the INDEX expression.
    /// The INDEX expression is the AST node after "INDEX" keyword.
    /// For "INDEX id TYPE unique", index is ASTIdentifier("id").
    /// For "INDEX (id, name) TYPE unique", index is ASTFunction("tuple", [id, name])
    ///   or ASTExpressionList with two children.
    if (!proj.index)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unique projection index requires INDEX expression with key columns");

    /// Collect the leaf identifiers from the INDEX expression.
    /// Supports: INDEX id            => ASTIdentifier
    ///           INDEX id, name      => ASTExpressionList [id, name]
    ///           INDEX (id, name)    => ASTFunction("tuple", [id, name])
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

    return std::make_shared<ProjectionIndexUnique>(std::move(key_columns));
}

void ProjectionIndexUnique::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * /*index_expr*/,
    const ColumnsDescription & /*columns*/,
    ContextPtr query_context) const
{
    chassert(result.index.get() == this);
    chassert(!index);
    /// 1. Get the SortedStringKV type directly from DataTypeFactory.
    ///    SortedStringKV is Tuple(String, SimpleAggregateFunction(max, UInt64))
    ///    with sorted key semantics and automatic dedup during merge.
    auto sorted_kv_type = DataTypeFactory::instance().get("SortedStringKV");

    /// 2. Build sample_block with a single "unique_kv" column ;
    result.sample_block.clear();
    result.sample_block.insert(ColumnWithTypeAndName{sorted_kv_type->createColumn(), sorted_kv_type, kv_column_name});

    /// 3. Record required columns: unique key columns must come from the source table
    result.required_columns = unique_key_columns;

    /// 4. Mark that this projection uses _parent_part_offset. During Merge:
    ///    - If merge_may_reduce_rows (e.g. TTL, dedup engines), the projection is routed to
    ///      rebuild path (projections_to_rebuild) which calls `calculate` to regenerate SST data.
    ///    - Otherwise, the projection is routed to merge path (projections_to_merge) which uses
    ///      AggregatingSortedAlgorithm to merge SST entries by key, applying max() aggregation
    ///      on part_offset values. After merge, the offsets are translated using MergedPartOffsets
    ///      to reflect the new row positions in the merged data part.
    result.with_parent_part_offset = true;

    /// 5. Unique projection is Aggregate type — SortedStringKV column uses
    ///    SimpleAggregateFunction(max, UInt64) for value deduplication during merge.
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

    /// Sorting key: the first element of the SortedStringKV Tuple (the "key" part).
    /// We directly construct `tupleElement(unique_kv, 1)` as an ASTFunction because:
    /// - ASTIdentifier("unique_kv.1") is treated as a flat column name, not found in source columns.
    /// - Compound ASTIdentifier({"unique_kv","1"}) relies on tryGetSubcolumnType("1"), which fails
    ///   because SerializationSortedStringKV does not expose TupleElement substreams.
    /// - Explicit tupleElement() function works the same way as SQL `ORDER BY kv.1` after parsing.
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

    /// Ordered map for dedup + sorted output.
    /// key: serialized unique key columns
    /// value: part_offset (row position in sorted part)
    /// Last-write-wins within a block: keep the entry with the larger part_offset.
    std::map<String, UInt64> sorted_kvs;

    /// When perm_ptr is present (INSERT path), we need the inverse permutation.
    /// perm_ptr maps sorted_pos -> original_pos, but we need original_pos -> sorted_pos
    /// because:
    ///   - We iterate the original (unsorted) block by original row index i
    ///   - part_offset must be the row's position in the sorted part on disk
    std::vector<size_t> inv_perm;
    if (perm_ptr)
    {
        inv_perm.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k)
            inv_perm[(*perm_ptr)[k]] = k;
    }

    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Compute _parent_part_offset for this row.
        /// part_offset is the row's position in the sorted part on disk.
        UInt64 part_offset;
        if (perm_ptr)
        {
            /// INSERT path: inv_perm maps original position to sorted position in part
            part_offset = starting_offset + inv_perm[i];
        }
        else
        {
            /// Merge path: sequential offset
            part_offset = starting_offset + i;
        }

        /// Serialize the unique key
        String key = serializeKeyColumns(block, unique_key_columns, i);

        /// Dedup within block: last-write-wins — keep the larger part_offset.
        auto it = sorted_kvs.find(key);
        if (it == sorted_kvs.end() || it->second < part_offset)
            sorted_kvs[key] = part_offset;
    }

    /// Build the output block with a single SortedStringKV column
    /// The column is Tuple(key String, value UInt64)
    auto key_column = ColumnString::create();
    auto offset_column = ColumnUInt64::create();

    for (const auto & [k, offset] : sorted_kvs)
    {
        key_column->insertData(k.data(), k.size());
        offset_column->insertValue(offset);
    }

    auto tuple_column = ColumnTuple::create(Columns{std::move(key_column), std::move(offset_column)});

    /// Trace log: output the dedup result for debugging.
    {
        auto calc_log = getLogger("ProjectionIndexUnique");
        LOG_TRACE(calc_log, "[calculate] num_rows={}, starting_offset={}, has_perm={}, deduped_keys={}",
            num_rows, starting_offset, perm_ptr != nullptr, sorted_kvs.size());
        if (sorted_kvs.size() <= 50)
        {
            for (const auto & [k, off] : sorted_kvs)
            {
                std::string hex;
                for (size_t i = 0; i < k.size(); ++i)
                    hex += fmt::format("{:02x}", static_cast<unsigned char>(k[i]));
                LOG_TRACE(calc_log, "[calculate]   key_hex={}, part_offset={}", hex, off);
            }
        }
    }

    Block result;
    const auto & sample = projection_desc.sample_block;
    chassert(sample.columns() == 1);
    result.insert(ColumnWithTypeAndName{std::move(tuple_column), sample.getByPosition(0).type, sample.getByPosition(0).name});

    return result;
}

std::shared_ptr<MergeTreeSettings> ProjectionIndexUnique::getDefaultSettings() const
{
    auto settings = std::make_shared<MergeTreeSettings>();
    settings->set("allow_tuple_element_aggregation", true);
    return settings;
}

const IndexDescription * ProjectionIndexUnique::getIndexDescription() const
{
    return &index_description;
}

MergeTreeIndexPtr ProjectionIndexUnique::getIndex() const
{
    return std::static_pointer_cast<const IMergeTreeIndex>(index);
}

}
