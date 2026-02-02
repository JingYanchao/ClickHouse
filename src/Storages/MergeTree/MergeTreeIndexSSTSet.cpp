#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSet.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSetWriter.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Functions/FunctionFactory.h>
#include <Functions/indexHint.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Storages/MergeTree/MergeTreeIndexSet.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <rocksdb/sst_file_reader.h>
#include <filesystem>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleSSTSet::MergeTreeIndexGranuleSSTSet(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , block(index_sample_block_.cloneEmpty())
    , max_rows_sort_in_memory(0)
    , index_writer(nullptr)
{
}

MergeTreeIndexGranuleSSTSet::MergeTreeIndexGranuleSSTSet(
    const String & index_name_, const Block & index_sample_block_, MutableColumns && columns_)
    : index_name(index_name_)
    , block(index_sample_block_.cloneWithColumns(std::move(columns_)))
    , max_rows_sort_in_memory(0)
    , index_writer(nullptr)
{
}

MergeTreeIndexGranuleSSTSet::MergeTreeIndexGranuleSSTSet(
    const String & index_name_, const Block & index_sample_block_, MutableColumns && columns_, MergeTreeIndexSSTSetWriter * index_writer_)
    : index_name(index_name_)
    , block(index_sample_block_.cloneWithColumns(std::move(columns_)))
    , max_rows_sort_in_memory(0)
    , index_writer(index_writer_)
{
}

void MergeTreeIndexGranuleSSTSet::serializeBinary(WriteBuffer & ostr) const
{
    if (index_writer)
    {
        /// Get index_path from the writer
        const auto & index_path = index_writer->getIndexPath();
        index_writer->flushIndexFile(index_path, &ostr);
    }
}


void MergeTreeIndexGranuleSSTSet::deserializeBinary(ReadBuffer & /* istr */, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    /// This method should not be called for SST Set index
    /// Use deserializeBinaryWithMultipleStreams instead
    throw Exception(ErrorCodes::LOGICAL_ERROR, "deserializeBinary should not be called for SST Set index");
}

void MergeTreeIndexGranuleSSTSet::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & /* streams */, 
    MergeTreeIndexDeserializationState & state)
{
    /// Get the SST file path from the data part
    const auto & data_part_storage = state.part.getDataPartStorage();
    String sst_file_name = index_name + ".idx";
    
    /// Check if the SST file exists
    if (!data_part_storage.existsFile(sst_file_name))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SST index file {} not found in part {}", 
                        sst_file_name, data_part_storage.getFullPath());
    }
    
    /// Get the full path to the SST file
    String sst_file_path = std::string(std::filesystem::path(data_part_storage.getFullPath()) / sst_file_name);
    
    /// Use RocksDB SstFileReader to read the SST file directly
    rocksdb::Options options;
    rocksdb::SstFileReader sst_reader(options);
    
    auto status = sst_reader.Open(sst_file_path);
    if (!status.ok())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to open SST file {}: {}", 
                        sst_file_path, status.ToString());
    }
    
    /// Read all key-value pairs into memory
    std::unique_ptr<rocksdb::Iterator> iter(sst_reader.NewIterator(rocksdb::ReadOptions()));
    index_data.clear();
    
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        std::string key = iter->key().ToString();
        std::string value = iter->value().ToString();
        index_data[key] = value;
    }
    
    if (!iter->status().ok())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error iterating SST file {}: {}", 
                        sst_file_path, iter->status().ToString());
    }
}


MergeTreeIndexBulkGranulesSSTSet::MergeTreeIndexBulkGranulesSSTSet(const Block & index_sample_block_)
    : block(index_sample_block_.cloneEmpty())
    , block_for_reading(index_sample_block_.cloneEmpty())
{
    size_t num_columns = block.columns();
    serializations.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i] = block.getByPosition(i).type->getDefaultSerialization();

    block.insert(ColumnWithTypeAndName{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "_granule_num"});
}

MergeTreeIndexAggregatorSSTSet::MergeTreeIndexAggregatorSSTSet(
    const String & index_name_, const Block & index_sample_block_, size_t max_rows_sort_in_memory_)
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

    // TODO: Get index path from data part - need to pass data_part to constructor
    // For now, this will be set when the aggregator is properly initialized
    // String index_path = data_part->getDataPartStorage().getFullPath() + index_name_ + ".idx";
    // index_writer = createMergeTreeIndexSSTSetWriter(max_rows_sort_in_memory, index_path, data_part);
    columns = index_sample_block.cloneEmptyColumns();
}

void MergeTreeIndexAggregatorSSTSet::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. "
            "Position: {}, Block rows: {}.",
            *pos,
            block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    // Extract the columns for the index
    Block index_block;
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const auto & column_name = index_sample_block.getByPosition(i).name;
        if (block.has(column_name))
        {
            auto column = block.getByName(column_name).column;
            // Extract the range [*pos, *pos + rows_read)
            auto cut_column = column->cut(*pos, rows_read);
            index_block.insert(ColumnWithTypeAndName(cut_column, index_sample_block.getByPosition(i).type, column_name));
        }
    }

    // Write the block to the index writer
    if (index_block.rows() > 0)
    {
        index_writer->write(index_block);
    }

    *pos += rows_read;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSSTSet::getGranuleAndReset()
{
    // Create a granule with the index_writer reference so it can flush data when serialized
    auto granule = std::make_shared<MergeTreeIndexGranuleSSTSet>(index_name, index_sample_block, std::move(columns), index_writer.get());
    columns = index_sample_block.cloneEmptyColumns();
    return granule;
}

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

MergeTreeIndexGranulePtr MergeTreeIndexSSTSet::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSSTSet>(index.name, index.sample_block);
}

MergeTreeIndexBulkGranulesPtr MergeTreeIndexSSTSet::createIndexBulkGranules() const
{
    return std::make_shared<MergeTreeIndexBulkGranulesSSTSet>(index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSSTSet::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSSTSet>(index.name, index.sample_block, max_rows_sort_in_memory);
}

MergeTreeIndexConditionPtr MergeTreeIndexSSTSet::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionSet>(max_rows_sort_in_memory, filter_dag, context, index);
}

MergeTreeIndexPtr SSTSetIndexCreator(const IndexDescription & index)
{
    size_t max_rows_sort_in_memory = index.arguments[0].safeGet<size_t>();
    return std::make_shared<MergeTreeIndexSSTSet>(index, max_rows_sort_in_memory);
}

}
