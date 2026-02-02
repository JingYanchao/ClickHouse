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
namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_UNLINK;
    extern const int ROCKSDB_ERROR;
}

/// Temporary RocksDB path suffix for deduplication
static constexpr std::string_view ROCKSDB_TEMP_SUFFIX = ".tmp";

MergeTreeIndexSSTSetWriter::MergeTreeIndexSSTSetWriter(
    const String & index_path)
    : index_path(index_path)
{
}

void MergeTreeIndexSSTSetWriter::write(const Block & block)
{
    processBlock(block);
    advanceRowOffset(block.rows());
}

void MergeTreeIndexSSTSetWriter::flushIndexFile(const String & index_path, WriteBuffer* write_buffer)
{
    writer = std::make_unique<SstFileWriterImpl>(index_path, *part, write_buffer);
    flushFileImpl();
}

MergeTreeIndexSSTSetWriterPtr createMergeTreeIndexSSTSetWriter(
    size_t max_rows_sort_in_memory,
    const String & index_path)
{
    if (data_part->rows_count <= max_rows_sort_in_memory)
    {
        LOG_TRACE(getLogger("MergeTreeIndexSSTSetWriter"), "Using sorted unique index writer for insert sink part {}", data_part->name);
        /// Use in-memory unique index writer for insert sink.
        return std::make_unique<MergeTreeIndexSSTSetWriterInMemory>(index_path);
    }
    else
    {
        LOG_TRACE(getLogger("MergeTreeIndexSSTSetWriter"), "Using RocksDB unique index writer for other cases part {}", data_part->name);
        /// Use RocksDB unique index writer for other cases.
        return std::make_unique<MergeTreeIndexSSTSetWriterRocksDB>(index_path);
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
    out_key_column->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 0, perm);
    auto value_column = ColumnString::create();
    ColumnStringIterWrapper iter(*out_key_column, *value_column, perm, row_offset);
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

static String getIndexBucketPath(const String & index_path, size_t bucket_id)
{
    /// Convert "sst.idx" to "sst-{bucket_id}.idx"
    /// Find the last dot to locate the extension
    auto dot_pos = index_path.find_last_of('.');
    if (dot_pos == String::npos)
    {
        /// No extension found, just append the bucket_id
        return fmt::format("{}-{}", index_path, bucket_id);
    }
    else
    {
        /// Insert the bucket_id before the extension
        String base = index_path.substr(0, dot_pos);
        String extension = index_path.substr(dot_pos);
        return fmt::format("{}-{}{}", base, bucket_id, extension);
    }
}

MergeTreeIndexSSTSetWriter::SstFileWriterImpl::SstFileWriterImpl(
    const String & index_path, const IMergeTreeDataPart & data_part, WriteBuffer * write_buffers)
{
    env = createDiskBasedUniqueIndexEnv(data_part.getDataPartStoragePtr(), write_buffers);
    rocksdb::Options options;
    options.env = env.get();
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    index_writer = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options);
    auto status = index_writer->Open(index_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Error while opening file {}: {}", index_path, status.ToString());
}

using Writer = MergeTreeIndexSSTSetWriter::SstFileWriterImpl;

void Writer::put(const std::string_view & key, const std::string_view & value)
{
    auto status = index_writer->Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
    if (unlikely(!status.ok()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to write key-value to unique index: {}", status.ToString());
}

void Writer::finish()
{
    rocksdb::ExternalSstFileInfo file_info;
    auto status = index_writer->Finish(&file_info);
    if (!status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while finishing file {}: {}", file_info.file_path, status.ToString());
}

void Writer::writeAndFinish(
    InputIterPtr iter)
{
    if (!iter->valid())
    {
        finish();
        return;
    }
    for (; iter->valid(); iter->next())
    {
        put(iter->key(), iter->value());
    }
    finish();
}

MergeTreeIndexSSTSetWriterRocksDB::MergeTreeIndexSSTSetWriterRocksDB(
    const String & index_path_)
    : MergeTreeIndexSSTSetWriter(index_path_)
    , db_path(index_path_ + ".tmp")
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
        std::make_unique<RocksDBIterWrapper>(db->NewIterator(read_options)));
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

size_t MergeTreeIndexSSTSetWriterRocksDB::size()
{
    if (!db)
        return 0;
    
    UInt64 estimated_keys = 0;
    if (!db->GetIntProperty("rocksdb.estimate-num-keys", &estimated_keys))
        return 0;
    
    return estimated_keys;
}

MergeTreeIndexSSTSetWriterInMemory::MergeTreeIndexSSTSetWriterInMemory(
    const String & index_path_)
    : MergeTreeIndexSSTSetWriter(index_path_)
{
}

MergeTreeIndexSSTSetWriterInMemory::~MergeTreeIndexSSTSetWriterInMemory() = default;

void MergeTreeIndexSSTSetWriterInMemory::processBlock(const Block & block)
{
    if (block.rows() == 0)
        return;
    auto key_column = ColumnString::create();
    processBlockImpl(
        block,
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
    writer->writeAndFinish(std::move(iter));
}

}

