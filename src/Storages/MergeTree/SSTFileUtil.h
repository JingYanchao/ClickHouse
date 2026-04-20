#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <base/types.h>
#include <memory>
#include <unordered_map>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/iterator.h>
#include <rocksdb/db.h>

namespace ProfileEvents
{
    extern const Event SSTReaderCacheHits;
    extern const Event SSTReaderCacheMisses;
}

namespace DB
{

struct MergeTreeDataPartChecksums;
struct WriteSettings;
class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class SSTFileWriter;

/// SST data file extension for SST-based column types.
inline constexpr auto SST_DATA_FILE_EXTENSION = ".sst";

/// SST file write stream: manages FileBuffer + SSTFileWriter for a single column.
class SSTFileWriteStream
{
public:
    SSTFileWriteStream(
        const String & escaped_column_name_,
        const MutableDataPartStoragePtr & data_part_storage,
        size_t buf_size,
        const WriteSettings & query_write_settings);

    ~SSTFileWriteStream();

    SSTFileWriter & getSSTWriter();

    /// Finalize SST writer and record checksums
    void fillSSTChecksums(MergeTreeDataPartChecksums & checksums);
    void preFinalize();
    void finalize();
    void cancel() noexcept;
    void sync() const;
    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    HashingWriteBuffer & getWriteBuffer() { return *hashing; }

private:
    String escaped_column_name;
    std::unique_ptr<WriteBufferFromFileBase> plain_file;
    std::unique_ptr<HashingWriteBuffer> hashing;
    std::unique_ptr<SSTFileWriter> sst_writer;
    bool pre_finalized = false;
};

using SSTFileWriteStreams = std::unordered_map<String, std::unique_ptr<SSTFileWriteStream>>;

using SSTReadBuffers = std::unordered_map<String, std::unique_ptr<ReadBufferFromFileBase>>;


/// RocksDB Env helpers
std::unique_ptr<rocksdb::Env> createWriteBufferFileSystemEnv(WriteBuffer * write_buffer);
std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(const DataPartStoragePtr & storage);

class SSTFileReader
{
public:
    using IndexPropertiesPtr = std::shared_ptr<const rocksdb::TableProperties>;

    SSTFileReader(SeekableReadBuffer * read_buffer, uint64_t file_offset, uint64_t file_size);
    /// Construct from a DataPartStorage: automatically opens the SST file,
    /// reads file size, and manages the underlying ReadBuffer internally.
    /// This is the preferred constructor for dedup / part-level access.
    SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name);

    std::unique_ptr<rocksdb::Iterator> newIterator(const rocksdb::ReadOptions & options) const;
    void verifyChecksums() const;
    IndexPropertiesPtr getProperties() const;
    /// Single key lookup using MultiGet internally.
    bool get(const rocksdb::Slice & key, std::string * value_out) const;
    /// Batch key lookup using MultiGet. Returns a vector of statuses, one per key.
    /// For each key where status is OK, the corresponding entry in values_out is populated.
    std::vector<rocksdb::Status> multiGet(const std::vector<rocksdb::Slice> & keys, std::vector<std::string> * values_out) const;
    /// Release ReadBuffer memory (1MB per file) to save memory.
    /// After calling this, the index reader can still be used normally
    /// (bloom filter and index blocks remain pinned in the TableReader).
    void releaseBufferMemory() const;

    /// Retrieve pre-sampled keys from SST table properties.
    /// Sample keys are collected during SST write time via
    /// SampleKeyCollector and stored as user-collected properties.
    std::vector<std::string> getSampleKeys() const;

private:
    /// Common initialization logic shared by both constructors.
    void init(const String & file_name);

    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileReader> index_reader;
};

using SSTFileReaderPtr = std::shared_ptr<const SSTFileReader>;
using SSTFileReaders = std::vector<SSTFileReaderPtr>;
using SSTFileReaderCacheValue = const SSTFileReader;

class SSTFileReaderCache
    : public CacheBase<UInt128, SSTFileReaderCacheValue, UInt128TrivialHash>
{
private:
    using Base = CacheBase<UInt128, SSTFileReaderCacheValue, UInt128TrivialHash>;

public:
    SSTFileReaderCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio)
        : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, Base::NO_MAX_COUNT, size_ratio)
    {
    }

    /// Compute cache key from table UUID and part name.
    static UInt128 hash(const UUID & table_uuid, const String & part_name)
    {
        SipHash s;
        s.update(reinterpret_cast<const char *>(&table_uuid), sizeof(table_uuid));
        s.update(part_name.data(), part_name.size());
        return s.get128();
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load));
        if (result.second)
            ProfileEvents::increment(ProfileEvents::SSTReaderCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::SSTReaderCacheHits);
        return result.first;
    }
};
using SSTFileReaderCachePtr = std::shared_ptr<SSTFileReaderCache>;

class SSTFileWriter
{
public:
    explicit SSTFileWriter(WriteBuffer * write_buffer);

    void put(const rocksdb::Slice & key, const rocksdb::Slice & value);
    void finish();
    uint64_t fileSize() const;

    UInt64 getWrittenRowCount() const { return written_row_counter; }
    void addWrittenRowCount(UInt64 count) { written_row_counter += count; }

private:
    UInt64 written_row_counter = 0;
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileWriter> writer;
    bool finished = false;
    bool has_entries = false;
};

class SSTFileReadStream : public MergeTreeReaderStreamSingleColumnWholePart
{
public:
    template <typename... Args>
    explicit SSTFileReadStream(Args &&... args)
        : MergeTreeReaderStreamSingleColumnWholePart{std::forward<Args>(args)...}
    {
    }
};

}
