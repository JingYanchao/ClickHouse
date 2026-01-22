#include <Storages/MergeTree/MergeTreeIndexSSTSet.h>

#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleSSTSet::MergeTreeIndexGranuleSSTSet(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_rows_sort_in_memory_)
    : index_name(index_name_)
    , block(index_sample_block_.cloneEmpty())
    , max_rows_sort_in_memory(max_rows_sort_in_memory_)
{
    size_t num_columns = block.columns();
}

void MergeTreeIndexGranuleSSTSet::serializeBinary(WriteBuffer & /*ostr*/) const
{
    /// SST Set index uses RocksDB SST file format and is written through MergeTreeIndexSSTSetWriter.
    /// It does not use the standard serializeBinary mechanism.
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Index with type 'sst_set' cannot be serialized with serializeBinary. "
        "SST Set index data is written directly to SST files via MergeTreeIndexSSTSetWriter.");
}

void MergeTreeIndexGranuleSSTSet::deserializeBinary(ReadBuffer & /*istr*/, MergeTreeIndexVersion /*version*/)
{
    /// SST Set index uses RocksDB SST file format and is read through RocksDB API.
    /// It does not use the standard deserializeBinary mechanism.
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Index with type 'sst_set' cannot be deserialized with deserializeBinary. "
        "SST Set index data is read directly from SST files via RocksDB API.");
}


MergeTreeIndexBulkGranulesSSTSet::MergeTreeIndexBulkGranulesSSTSet(
    const Block & index_sample_block_)
    : block(index_sample_block_.cloneEmpty()),
    block_for_reading(index_sample_block_.cloneEmpty())
{
    size_t num_columns = block.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = block.getByPosition(i).type->getDefaultSerialization();

    block.insert(ColumnWithTypeAndName
    {
        ColumnUInt64::create(),
        std::make_shared<DataTypeUInt64>(),
        "_granule_num"
    });
}


void MergeTreeIndexBulkGranulesSSTSet::deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    if (empty)
    {
        min_granule = granule_num;
        empty = false;
    }
    max_granule = granule_num;
}


MergeTreeIndexAggregatorSSTSet::MergeTreeIndexAggregatorSSTSet(const MergeTreeDataPartPtr & data_part, const String & index_name_, const Block & index_sample_block_, size_t max_rows_sort_in_memory_)
    : index_name(index_name_)
    , max_rows_sort_in_memory(max_rows_sort_in_memory_)
    , index_sample_block(index_sample_block_)
    , columns(index_sample_block_.cloneEmptyColumns())
{
    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(index_sample_block.columns());
    Columns materialized_columns;
    for (const auto & column : index_sample_block.getColumns())
    {
        materialized_columns.emplace_back(column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality());
        column_ptrs.emplace_back(materialized_columns.back().get());
    }

    data.init(ClearableSetVariants::chooseMethod(column_ptrs, key_sizes));
    index_writer = std::make_shared<MergeTreeIndexSSTSetWriter>(index_sample_block, max_rows_sort_in_memory);

    columns = index_sample_block.cloneEmptyColumns();
}

void MergeTreeIndexAggregatorSSTSet::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    *pos += rows_read;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSSTSet::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleSSTSet>(index_name, index_sample_block);

    switch (data.type)
    {
        case ClearableSetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case ClearableSetVariants::Type::NAME: \
            data.NAME->data.clear(); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    columns = index_sample_block.cloneEmptyColumns();

    return granule;
}

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

MergeTreeIndexGranulePtr MergeTreeIndexSSTSet::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSSTSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexBulkGranulesPtr MergeTreeIndexSSTSet::createIndexBulkGranules() const
{
    return std::make_shared<MergeTreeIndexBulkGranulesSSTSet>(index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSSTSet::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSSTSet>(index.name, index.sample_block, max_rows);
}

MergeTreeIndexConditionPtr MergeTreeIndexSSTSet::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionSet>(max_rows, filter_dag, context, index);
}

MergeTreeIndexPtr setIndexCreator(const IndexDescription & index)
{
    size_t max_rows = index.arguments[0].safeGet<size_t>();
    return std::make_shared<MergeTreeIndexSSTSet>(index, max_rows);
}

}
