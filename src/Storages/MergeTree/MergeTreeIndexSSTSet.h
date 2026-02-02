#pragma once
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSetWriter.h>
#include <rocksdb/sst_file_reader.h>

namespace DB
{

class MergeTreeIndexSSTSet;
class MergeTreeSSTFileReader
{
public:
    using IndexPropertiesPtr = std::shared_ptr<const rocksdb::TableProperties>;
    /// created from local file located at "file_path".
    explicit MergeTreeSSTFileReader(const IMergeTreeDataPart & part, const String & sst_file_name);

    /// Return an iterator over KVs in this file.
    /// Note: client should make sure the UniqueKeyIndex object lives longer than the returned iterator.
    std::unique_ptr<rocksdb::Iterator> newIterator(const rocksdb::ReadOptions & options) const;

    /// Search Value in sst file.
    bool get(const rocksdb::Slice & key, std::string * value_out) const;

    /// Check if the key range of this index file intersects with the key range of the other index file.
    bool keyRangeIntersects(const MergeTreeSSTFileReader & other) const;

    /// Check if the key may exist in the index file.
    bool mayContainKey(std::string_view key) const;

    /// Verify checksums for the index file.
    void verifyChecksums() const;

    /// Get index file properties.
    IndexPropertiesPtr getProperties() const;

    bool isEmpty() const;
    /// Release ReadBuffer memory (1MB per file) to save memory.
    /// After calling this, the index reader can still be used normally.
    void releaseBufferMemory() const;
private:
    using MinMax = std::pair<std::string, std::string>;
    std::unique_ptr<rocksdb::Env> sst_env = nullptr;
    std::unique_ptr<rocksdb::SstFileReader> index_reader = nullptr;
    MinMax key_range;
};
using MergeTreeSSTFileReaderPtr = std::shared_ptr<MergeTreeSSTFileReader>;

std::unique_ptr<rocksdb::Env> createReadSSTFileEnv(std::shared_ptr<const IDataPartStorage> storage);
std::unique_ptr<rocksdb::Env> createWriteSSTFileEnv(WriteBuffer * write_buffer);

struct MergeTreeIndexGranuleSSTSet final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleSSTSet(
        const String & index_name_,
        const Block & index_sample_block_);

    MergeTreeIndexGranuleSSTSet(
        const String & index_name_,
        const Block & index_sample_block_,
        MutableColumns && columns_);

    MergeTreeIndexGranuleSSTSet(
        const String & index_name_,
        const Block & index_sample_block_,
        MutableColumns && columns_,
        MergeTreeIndexSSTSetWriter * index_writer_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    size_t size() const { return block.rows(); }
    bool empty() const override { return !size(); }
    size_t memoryUsageBytes() const override { return block.bytes(); }

    ~MergeTreeIndexGranuleSSTSet() override = default;

    const String & index_name;

    Block block;
    const size_t max_rows_sort_in_memory;
    MergeTreeIndexSSTSetWriter * index_writer;
    MergeTreeSSTFileReaderPtr sst_reader;
};

struct MergeTreeIndexAggregatorSSTSet final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorSSTSet(
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_rows_sort_in_memory);

    ~MergeTreeIndexAggregatorSSTSet() override = default;

    bool empty() const override { return !index_writer->size(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String index_name;
    size_t max_rows_sort_in_memory;
    Block index_sample_block;
    MergeTreeIndexSSTSetWriterPtr index_writer;
    Sizes key_sizes;
    MutableColumns columns;
};


class MergeTreeIndexSSTSet final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSSTSet(
        const IndexDescription & index_,
        size_t max_rows_sort_in_memory_)
        : IMergeTreeIndex(index_)
        , max_rows_sort_in_memory(max_rows_sort_in_memory_)
    {}

    ~MergeTreeIndexSSTSet() override = default;

    bool supportsBulkFiltering() const override
    {
        return true;
    }

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;
private:
    size_t max_rows_sort_in_memory;
};

}

