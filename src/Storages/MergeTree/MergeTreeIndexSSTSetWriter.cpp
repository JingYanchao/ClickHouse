#include <Storages/MergeTree/MergeTreeIndexSSTSetWriter.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSet.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include "simdjson/generic/ondemand/json_type.h"
namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_UNLINK;
    extern const int ROCKSDB_ERROR;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool sst_set_index_bucket_number;
}

MergeTreeIndexSSTSetWriter::MergeTreeIndexSSTSetWriter(
    size_t index_bucket_number_, const String & index_path, const MergeTreeDataPartPtr & data_part, const StorageMetadataPtr & metadata_snapshot_)
    : part(data_part)
    , index_path(index_path)
    , index_bucket_number(index_bucket_number_)
    , metadata_snapshot(metadata_snapshot_)
{
}

void MergeTreeIndexSSTSetWriter::write(const Block & block)
{
    auto block_copy = block;
    unique_key.expression->execute(block_copy);
    auto unique_key_block = getBlockAndPermute(block_copy, unique_key.sample_block.getNames(), permutation);
    ColumnPtr version_column;
    if (block.has(part->storage.merging_params.version_column))
    {
        Names version_column_name{part->storage.merging_params.version_column};
        auto version_block = getBlockAndPermute(block, version_column_name, permutation);
        version_column = version_block.getByName(part->storage.merging_params.version_column).column;
    }
    else
    {
        if (unlikely(part->storage.hasCustomizedVersionColumnForUniqueIndex() || part->info.level != 0))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Customized version column or non-level 0 part requires explicit version column");
        version_column = DataTypeUInt64().createColumnConst(block.rows(), part->info.min_block);
    }
    Stopwatch watch;
    processUniqueKeyBlock(unique_key_block, version_column);
    ProfileEvents::increment(ProfileEvents::UniqueKeyWriterProcessTimeMicroseconds, watch.elapsedMicroseconds());
    advanceRowOffset(block.rows());
}

void MergeTreeIndexSSTSetWriter::flushIndexFile(const std::vector<WriteBuffer*> & write_buffers)
{
    writer = std::make_unique<SstFileWriterImpl>(index_bucket_number, *part, write_buffers);
    flushFileImpl();
}

MergeTreeIndexSSTSetWriterPtr createMergeTreeIndexSSTSetWriter(
    size_t max_rows_sort_in_memory,
    size_t index_bucket_number,
    const String & index_path,
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot)
{
    if (data_part->rows_count <= max_rows_sort_in_memory)
    {
        LOG_TRACE(getLogger("MergeTreeIndexSSTSetWriter"), "Using sorted unique index writer for insert sink part {}", data_part->name);
        /// Use in-memory unique index writer for insert sink.
        return std::make_unique<MergeTreeIndexSSTSetWriterInMemory>(index_bucket_number, index_path, data_part, metadata_snapshot);
    }
    else
    {
        LOG_TRACE(getLogger("MergeTreeIndexSSTSetWriter"), "Using RocksDB unique index writer for other cases part {}", data_part->name);
        /// Use RocksDB unique index writer for other cases.
        return std::make_unique<MergeTreeIndexSSTSetWriterRocksDB>(index_bucket_number, index_path, data_part, metadata_snapshot);
    }
}

void MergeTreeIndexSSTSetWriter::constructSerializedKey(const ColumnsWithTypeAndName & arguments, ColumnString::MutablePtr & out_key_column) const
{
    if (arguments.empty())
        return;
    auto rows = arguments[0].column->size();
    if (rows == 0)
        return;
    /// Construct a column that contains the serialized key of the unique key block.
    size_t reserved_size = 0;
    for (const auto & col_with_name : arguments)
    {
        PaddedPODArray<UInt64> serialized_sizes;
        col_with_name.column->collectSerializedValueSizes(serialized_sizes, nullptr, nullptr);
        reserved_size += std::accumulate(serialized_sizes.begin(), serialized_sizes.end(), 0ULL);
    }
    /// Every element of ColumnString has a trailing byte 0.
    reserved_size += rows;
    /// Serialize unique key into the string column.
    auto & chars = out_key_column->getChars();
    auto & offsets = out_key_column->getOffsets();
    chars.resize_fill(chars.size() + reserved_size);
    offsets.reserve_exact(offsets.size() + rows);
    for (size_t row = 0; row < rows; ++row)
    {
        ssize_t index = row;
        /// offsets's index starts from -1.
        /// Look for detail in ColumnString::get() and ColumnString::offsetAt().
        auto * pos = reinterpret_cast<char *>(&chars[offsets[index - 1]]);
        size_t key_size = 0;
        for (const auto & col_with_name : arguments)
        {
            auto * new_pos = col_with_name.column->serializeValueIntoMemory(row, pos, nullptr);
            key_size += new_pos - pos;
            pos = new_pos;
        }
        /// Every element of ColumnString has a trailing byte 0.
        offsets.push_back(offsets.back() + key_size + 1);
    }
}

bool MergeTreeIndexSSTSetWriter::compareKV(const KV & lkv, const KV & rkv) const
{
    return lkv.first < rkv.first;
}

void MergeTreeIndexSSTSetWriter::processBlockImpl(
    const Block & block,
    ColumnString::MutablePtr & out_key_column,
    PutFn && put_fn) const
{
    if (block.rows() == 0)
        return;
    chassert(out_key_column->empty());
    constructSerializedKey(block.getColumnsWithTypeAndName(), out_key_column);
    /// Sort the serialized key column.
    IColumn::Permutation perm;
    auto value_column = ColumnString::create();
    ColumnStringIterWrapper iter(*out_key_column, value_column, perm, row_offset);
    std::string_view last_key = iter.key();
    std::string_view last_value = iter.value();
    iter.next();
    for (; iter.valid(); iter.next())
    {
        auto key = iter.key();
        auto value = iter.value();

        /// In most cases, there are few duplicate keys.
        if (likely(compareKV({last_key, last_value}, {key, value}) != 0))
        {
            put_fn(last_key, last_value);
            last_key = std::move(key);
            last_value = std::move(value);
        }
        else if (compareKV({last_key, last_value}, {key, value}) > 0)
        {
            last_key = std::move(key);
            last_value = std::move(value);
        }
    }
    put_fn(last_key, last_value);
}
MergeTreeIndexSSTSetWriter::SstFileWriterImpl::SstFileWriterImpl(
    size_t index_bucket_number_, const IMergeTreeDataPart & data_part, const std::vector<WriteBuffer *> & write_buffers)
    : index_bucket_number(index_bucket_number_)
    , index_writers(index_bucket_number)
    , index_writers_has_written_key(index_bucket_number, false)
    , envs(index_bucket_number)
{
    if (write_buffers.size() != index_bucket_number)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "write_buffers size {} doesn't match bucket_number {}", write_buffers.size(), index_bucket_number);

    for (size_t i = 0; i < index_bucket_number; ++i)
    {
        envs[i] = createDiskBasedUniqueIndexEnv(data_part.getDataPartStoragePtr(), write_buffers[i]);
        rocksdb::Options options;
        options.env = envs[i].get();
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));

        auto & [fs_path, writer_impl] = index_writers[i];
        writer_impl = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options);
        auto index_sub_path = getFullUniqueIndexPath(i);
        auto status = writer_impl->Open(index_sub_path);
        if (!status.ok())
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Error while opening file {}: {}", index_sub_path, status.ToString());
        fs_path = std::move(index_sub_path);
    }
}

using Writer = MergeTreeIndexSSTSetWriter::SstFileWriterImpl;

void Writer::put(const std::string_view & key, const std::string_view & value)
{
    auto bucket = hash_func(key) % index_bucket_number;
    auto status = index_writers[bucket].second->Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
    if (unlikely(!status.ok()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to write key-value to unique index {}: {}", bucket, status.ToString());
    index_writers_has_written_key[bucket] = true;
}

void Writer::finish(size_t num_rows)
{
    if (num_rows == 0)
    {
        /// Remove the index files if exists.
        for (size_t bucket = 0; bucket < index_bucket_number; ++bucket)
        {
            auto & [fs_path, writer_impl] = index_writers[bucket];
            writer_impl.reset();
            if (0 != unlink(fs_path.c_str()) && errno != ENOENT)
                ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, fs_path, "Cannot unlink file {}", fs_path);
        }
        return;
    }

    for (size_t bucket = 0; bucket < index_bucket_number; ++bucket)
    {
        auto & [fs_path, writer_impl] = index_writers[bucket];
        if (!index_writers_has_written_key[bucket])
        {
            /// if there is no key written to bucket i, then we just put a empty key-value pair into it,
            /// as SST file cannot be empty.
            auto status = writer_impl->Put(rocksdb::Slice(), rocksdb::Slice());
            if (!status.ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to write key-value to unique index {}: {}", bucket, status.ToString());
        }

        rocksdb::ExternalSstFileInfo file_info;
        auto status = writer_impl->Finish(&file_info);
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while finishing file {}: {}", file_info.file_path, status.ToString());
    }
}

void Writer::writeAndFinish(
    InputIterPtr iter, size_t num_rows)
{
    if (!iter->valid())
    {
        finish(num_rows);
        return;
    }
    for (; iter->valid(); iter->next())
    {
        put(iter->key(), iter->value());
    }
    finish(num_rows);
}

MergeTreeIndexSSTSetWriterRocksDB::MergeTreeIndexSSTSetWriterRocksDB(
    size_t index_bucket_number_, const String & index_path_, const MergeTreeDataPartPtr & data_part, const StorageMetadataPtr & metadata_snapshot_)
    : MergeTreeIndexSSTSetWriter(index_bucket_number_, index_path_, data_part, metadata_snapshot_)
    , db_path(index_path + ".tmp")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.avoid_flush_during_shutdown = true;
    options.persist_user_defined_timestamps = false;
    options.allow_concurrent_memtable_write = false;
    options.comparator = rocksdb::BytewiseComparatorWithU64Ts();

    rocksdb::DB * db_raw_ptr = nullptr;
    /// Reuse bucket-0's name to create a temporary RocksDB file.
    auto status = rocksdb::DB::Open(options, db_path, &db_raw_ptr);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open RocksDB: {}", status.ToString());
    db.reset(db_raw_ptr);
}

void MergeTreeIndexSSTSetWriterRocksDB::processBlock(const Block & block)
{
    if (block.rows() == 0)
        return;

    rocksdb::WriteBatch batch;
    {
        auto key_column = ColumnString::create();
        /// Sort and dedup for block-wide.
        processBlockImpl(
            block,
            key_column,
            [&](const std::string_view & key, const std::string_view & /* value */)
            {
                auto status = batch.Put(
                    db->DefaultColumnFamily(),
                    rocksdb::Slice(key.data(), key.size()),
                    rocksdb::Slice());
                if (unlikely(!status.ok()))
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to put batch to RocksDB: {}", status.ToString());
            });
        /// Release key_column as soon as possible.
    }
    /// Write to RocksDB to dedup for part-wide.
    auto options = rocksdb::WriteOptions();
    options.disableWAL = true;
    auto status = db->Write(options, &batch);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to write to RocksDB: {}", status.ToString());
}

class RocksDBIterWrapper : public SortedKeyIterator
{
public:
    explicit RocksDBIterWrapper(rocksdb::Iterator * iter_)
        : iter(iter_)
    {
        iter->SeekToFirst();
    }

    bool valid() const override
    {
        return iter->Valid();
    }
    void next() override { iter->Next(); }

    Key key() const override
    {
        return Key(iter->key().data(), iter->key().size());
    }

    Value value() const override
    {
        return Value(iter->value().data(), iter->value().size());
    }
private:
    std::unique_ptr<rocksdb::Iterator> iter;
};

void MergeTreeIndexSSTSetWriterRocksDB::flushFileImpl()
{
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.async_io = true;

    /// When using a comparator with timestamp, we **must** set the timestamp in ReadOptions.
    /// Set to max value to read all versions.
    WriteBufferFromOwnString max_version_buf;
    writeBinaryBigEndian(std::numeric_limits<UInt64>::max(), max_version_buf);
    rocksdb::Slice max_version_slice(max_version_buf.str());
    read_options.timestamp = &max_version_slice;
    writer->writeAndFinish(
        std::make_unique<RocksDBIterWrapper>(db->NewIterator(read_options)),
        row_offset);
    closeAndDestroy();
}

MergeTreeIndexSSTSetWriterRocksDB::~MergeTreeIndexSSTSetWriterRocksDB()
{
    try
    {
        closeAndDestroy();
    }
    catch (...)
    {
        tryLogCurrentException(
            "MergeTreeIndexSSTSetWriterRocksDB",
            fmt::format("Failed to close and destroy RocksDB ({})", db_path));
    }
}

void MergeTreeIndexSSTSetWriterRocksDB::closeAndDestroy()
{
    if (!db)
        return;
    auto status = db->Close();
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to close RocksDB ({}): {}", db_path, status.ToString());
    rocksdb::Options options;
    status = rocksdb::DestroyDB(db_path, options);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to destroy RocksDB ({}): {}", db_path, status.ToString());
    db.reset();
}

MergeTreeIndexSSTSetWriterInMemory::MergeTreeIndexSSTSetWriterInMemory(
    size_t index_bucket_number_, const String & index_path_, const MergeTreeDataPartPtr & data_part, const StorageMetadataPtr & metadata_snapshot_)
    : MergeTreeIndexSSTSetWriter(index_bucket_number_, index_path_, data_part, metadata_snapshot_)
{
}

void MergeTreeIndexSSTSetWriterInMemory::processBlock(const Block & unique_key_block)
{
    if (unique_key_block.rows() == 0)
        return;
    auto key_column = ColumnString::create();
    processBlockImpl(
        unique_key_block,
        key_column,
        [&](const std::string_view & key, const std::string_view & value) { index_keys.emplace_back(key, value); });
    key_holder.emplace_back(std::move(key_column));
}

class InMemoryBatchIterWrapper : public SortedKeyIterator
{
public:
    using ConstIter = std::vector<MergeTreeIndexSSTSetWriterInMemory::KV>::const_iterator;

    explicit InMemoryBatchIterWrapper(ConstIter begin_, ConstIter end_) : cur(begin_), end(end_) { }

    bool valid() const override { return cur != end; }

    void next() override
    {
        if (likely(cur != end))
            ++cur;
    }

    Key key() const override { return cur->first; }

    Value value() const override { return cur->second; }

private:
    ConstIter cur;
    ConstIter end;
};

void MergeTreeIndexSSTSetWriterInMemory::flushFileImpl()
{
    ::sort(
        index_keys.begin(),
        index_keys.end(),
        [](const KV & a, const KV & b) { return a.first < b.first; });

    auto iter = std::make_unique<InMemoryBatchIterWrapper>(index_keys.cbegin(), index_keys.cend());
    writer->writeAndFinish(std::move(iter), row_offset);
}

}

