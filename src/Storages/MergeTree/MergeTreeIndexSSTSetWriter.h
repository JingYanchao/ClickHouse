#pragma once
#include <memory>
#include <unordered_map>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/db.h>

namespace DB
{

class WriteBuffer;
class SortedKeyIterator
{
public:
    virtual ~SortedKeyIterator() = default;

    using Key = std::string_view;
    using Value = std::string_view;

    virtual bool valid() const = 0;
    virtual void next() = 0;
    virtual Key key() const = 0;
    virtual Value value() const = 0;
};

/// A implementation of `SortedUniqueKeyIterator` that iterates over a sorted unique key.
/// The lifetime of the iterator should be shorter than the lifetime of the key column.
class ColumnStringIterWrapper : public SortedKeyIterator
{
    using Permutation = const IColumn::Permutation &;
public:
    ColumnStringIterWrapper(
        const ColumnString & key_column_,
        const ColumnString & value_column_,
        const IColumn::Permutation & permutation_,
        UInt32 row_offset_)
            : key_column(key_column_)
            , value_column(value_column_)
            , permutation(permutation_)
            , row_offset(row_offset_)
    {
        update();
    }

    bool isFirst() const
    {
        return cur_idx == 0;
    }

    bool valid() const override
    {
        return cur_idx != key_column.size();
    }

    void next() override
    {
        cur_idx++;
        update();
    }

    Key key() const override
    {
        assert(valid());
        return key_column.getDataAt(cur_row);
    }

    Key value() const override
    {
        assert(valid());
        return value_column.getDataAt(cur_row);
    }
private:
    inline UInt32 getRowId() const
    {
        return row_offset + cur_row;
    }

    inline void update()
    {
        if (likely(valid()))
        {
            cur_row = static_cast<UInt32>(permutation[cur_idx]);
        }
    }

    const ColumnString & key_column;
    const ColumnString & value_column;
    Permutation permutation;

    const UInt32 row_offset = 0;
    UInt32 cur_row = 0;
    UInt32 cur_idx = 0;
};


class MergeTreeIndexSSTSetWriter
{
public:
    using KV = std::pair<std::string_view, std::string_view>;
    explicit MergeTreeIndexSSTSetWriter(
        size_t index_bucket_number, const String & index_path, const MergeTreeDataPartPtr & data_part, const StorageMetadataPtr & metadata_snapshot_);
    virtual ~MergeTreeIndexSSTSetWriter() = default;

    void write(const Block & block);
    void flushIndexFile(const std::vector<WriteBuffer*> & write_buffers);
    
    MergeTreeDataPartPtr part;
protected:
    virtual void processBlock(const Block & block) = 0;
    virtual void flushFileImpl() { throw Exception(ErrorCodes::LOGICAL_ERROR, "flushFileImpl is not implemented"); }

    inline void advanceRowOffset(size_t rows) { row_offset += rows; }
    bool compareKV(const KV & lkv, const KV & rkv) const;

    using PutFn = std::function<void(const std::string_view & key, const std::string_view & value)>;
    void processBlockImpl(
        const Block & block,
        ColumnString::MutablePtr & out_key_column,
        PutFn && put_fn) const;
public:
    void constructSerializedKey(const ColumnsWithTypeAndName & arguments, ColumnString::MutablePtr & out_key_column) const;

    /// A helper class to dedup sorted (by unique key) blocks and write them to sst index file.
    class SstFileWriterImpl
    {
    public:
        explicit SstFileWriterImpl(size_t index_bucket_number_, const IMergeTreeDataPart & part, const std::vector<WriteBuffer*> & write_buffers);
        using InputIter = SortedKeyIterator;
        using InputIterPtr = std::unique_ptr<InputIter>;
        /// Put a single key-value pair to the sst file.
        void put(const std::string_view & key, const std::string_view & value);
        /// Consume the iterator and write to the sst file.
        /// Deduplicate the unique key if needed.
        void writeAndFinish(InputIterPtr iter, size_t num_rows);
        void finish(size_t num_rows);
    private:
        using WriterImpl = rocksdb::SstFileWriter;
        using WriterImplPtr = std::unique_ptr<WriterImpl>;
        using WriterImpls = std::vector<std::pair<String, WriterImplPtr>>;
        std::hash<InputIter::Key> hash_func;

        const size_t index_bucket_number;
        WriterImpls index_writers;
        std::vector<bool> index_writers_has_written_key;
        std::vector<std::unique_ptr<rocksdb::Env>> envs;
    };
protected:
    std::unique_ptr<SstFileWriterImpl> writer;
    const size_t index_bucket_number;
    const String index_path;
    StorageMetadataPtr metadata_snapshot;
    Block index_sample_block;
    UInt32 row_offset = 0;
};
using MergeTreeIndexSSTSetWriterPtr = std::unique_ptr<MergeTreeIndexSSTSetWriter>;

/// A implementation of sst index writer for the case that
/// use RocksDB to sort and deduplicate unique keys.
class MergeTreeIndexSSTSetWriterRocksDB : public MergeTreeIndexSSTSetWriter
{
public:
    explicit MergeTreeIndexSSTSetWriterRocksDB(
        size_t index_bucket_number,
        const String & index_path,
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_);
    ~MergeTreeIndexSSTSetWriterRocksDB() override;
protected:
    void processBlock(const Block & block) override;
    void flushFileImpl() override;
private:
    void closeAndDestroy();
    const String db_path;
    std::unique_ptr<rocksdb::DB> db = nullptr;
};

/// A implementation of sst index writer for the case that
/// Use Vector In Memory to dedup
class MergeTreeIndexSSTSetWriterInMemory : public MergeTreeIndexSSTSetWriter
{
public:
    explicit MergeTreeIndexSSTSetWriterInMemory(
        size_t index_bucket_number_,
        const String & index_path,
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_);
protected:
    void processBlock(const Block & block) override;
    void flushFileImpl() override;
private:
    std::vector<ColumnString::Ptr> key_holder;
    std::vector<KV> index_keys;
};

}
