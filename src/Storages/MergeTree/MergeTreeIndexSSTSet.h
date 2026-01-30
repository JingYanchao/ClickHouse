#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <rocksdb/table.h>
#include <rocksdb/file_system.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSetWriter.h>
#include <IO/WriteBuffer.h>
#include <unordered_map>

namespace DB
{

class MergeTreeIndexSSTSet;

/// WritableFile implementation that writes to a provided WriteBuffer
class WriteBufferWritableFile : public rocksdb::FSWritableFile
{
public:
    explicit WriteBufferWritableFile(WriteBuffer & write_buffer_)
        : write_buffer(write_buffer_)
        , file_size(0)
    {
    }

    rocksdb::IOStatus Append(
        const rocksdb::Slice & data,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        try
        {
            write_buffer.write(data.data(), data.size());
            file_size += data.size();
            return rocksdb::IOStatus::OK();
        }
        catch (...)
        {
            auto error_msg = getCurrentExceptionMessage(true);
            return rocksdb::IOStatus::IOError("Failed to write data: " + error_msg);
        }
    }

    rocksdb::IOStatus Close(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        write_buffer.preFinalize();
        write_buffer.finalize();
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Flush(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        /// write buffer flush by itself
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Sync(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        write_buffer.sync();
        return rocksdb::IOStatus::OK();
    }

    uint64_t GetFileSize(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return file_size;
    }

private:
    WriteBuffer & write_buffer;
    uint64_t file_size;
};

class ReadBufferBasedSequentialFile : public rocksdb::FSSequentialFile
{
public:
    explicit ReadBufferBasedSequentialFile(std::unique_ptr<ReadBufferFromFileBase> file_)
        : file(std::move(file_))
    {
    }

    rocksdb::IOStatus Read(
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) override
    {
        auto read = file->read(scratch, n);
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Skip(uint64_t n) override
    {
        file->ignore(n);
        return rocksdb::IOStatus::OK();
    }
private:
    std::unique_ptr<ReadBufferFromFileBase> file;
};

class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
{
public:
    explicit ReadBufferBasedRandomAccessFile(std::unique_ptr<ReadBufferFromFileBase> file_)
        : file(std::move(file_))
    {
    }

    rocksdb::IOStatus Read(
        uint64_t offset,
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) const override
    {
        file->seek(offset, SEEK_SET);
        auto read = file->read(scratch, n);
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }
private:
    std::unique_ptr<ReadBufferFromFileBase> file;
};

class DataPartStorageBasedFileSystem : public rocksdb::FileSystem
{
public:
    explicit DataPartStorageBasedFileSystem(const DataPartStoragePtr & storage_, WriteBuffer * write_buffer_)
        : storage(storage_), write_buffer(write_buffer_)
    {
    }

    const char* Name() const override { return "DataPartStorageBasedFileSystem"; }

    rocksdb::IOStatus NewSequentialFile(
        const std::string & f,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSSequentialFile> * r,
        rocksdb::IODebugContext *) override
    {
        auto file = storage->readFile(f, ReadSettings(), std::nullopt);
        *r = std::make_unique<ReadBufferBasedSequentialFile>(std::move(file));
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus NewRandomAccessFile(
        const std::string & f,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> * r,
        rocksdb::IODebugContext *) override
    {
        auto file = storage->readFile(f, ReadSettings(), std::nullopt);
        *r = std::make_unique<ReadBufferBasedRandomAccessFile>(std::move(file));
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus FileExists(
        const std::string & f,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        if (storage->exists(f))
            return rocksdb::IOStatus::OK();
        else
            return rocksdb::IOStatus::NotFound();
    }

    rocksdb::IOStatus GetFileSize(
        const std::string & f,
        const rocksdb::IOOptions &,
        uint64_t * res,
        rocksdb::IODebugContext *) override
    {
        *res = storage->getFileSize(f);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus NewWritableFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSWritableFile> * r,
        rocksdb::IODebugContext *) override
    {
        if (!write_buffer)
            return rocksdb::IOStatus::InvalidArgument("WriteBuffer not set");

        *r = std::make_unique<WriteBufferWritableFile>(*write_buffer);
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods:
    rocksdb::IOStatus NewDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        std::unique_ptr<rocksdb::FSDirectory> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetChildren(
        const std::string &,
        const rocksdb::IOOptions &,
        std::vector<std::string> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDirIfMissing(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileModificationTime(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetAbsolutePath(
        const std::string &,
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus RenameFile(
        const std::string &,
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus LockFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::FileLock **,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus UnlockFile(
        rocksdb::FileLock *,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetTestDirectory(
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus IsDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        bool *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
private:
    DataPartStoragePtr storage;
    WriteBuffer* write_buffer = nullptr;
};

inline std::unique_ptr<rocksdb::Env> createDiskBasedUniqueIndexEnv(std::shared_ptr<const IDataPartStorage> storage, WriteBuffer * write_buffer)
{
    return rocksdb::NewCompositeEnv(std::make_shared<DataPartStorageBasedFileSystem>(std::move(storage), write_buffer));
}

struct MergeTreeIndexGranuleSSTSet final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleSSTSet(
        const String & index_name_,
        const Block & index_sample_block_);

    MergeTreeIndexGranuleSSTSet(
        const String & index_name_,
        const Block & index_sample_block_,
        MutableColumns && columns_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    size_t size() const { return block.rows(); }
    bool empty() const override { return !size(); }
    size_t memoryUsageBytes() const override { return block.bytes(); }

    ~MergeTreeIndexGranuleSSTSet() override = default;

    const String & index_name;

    Block block;
    const size_t max_rows_sort_in_memory;
};


struct MergeTreeIndexBulkGranulesSSTSet final : public IMergeTreeIndexBulkGranules
{
    explicit MergeTreeIndexBulkGranulesSSTSet(const Block & index_sample_block_);
    void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) override;

    size_t min_granule = 0;
    size_t max_granule = 0;
    Block block;
    Block block_for_reading;
    Serializations serializations;
    bool empty = true;
};


struct MergeTreeIndexAggregatorSSTSet final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorSSTSet(
        const MergeTreeDataPartPtr & data_part,
        const String & index_name_,
        const Block & index_sample_block_,
        size_t max_rows_sort_in_memory);

    ~MergeTreeIndexAggregatorSSTSet() override = default;

    bool empty() const override { return !size(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String index_name;
    size_t max_rows_sort_in_memory;
    size_t index_bucket_number;
    Block index_sample_block;
    MergeTreeIndexSSTSetWriterPtr index_writer;
    Sizes key_sizes;
    MutableColumns columns;
};


class MergeTreeIndexSSTSet final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSSTSet(
        const MergeTreeDataPartPtr & data_part_,
        const IndexDescription & index_,
        size_t index_bucket_number_,
        size_t max_rows_sort_in_memory_)
        : IMergeTreeIndex(index_)
        , data_part(data_part_)
        , max_rows_sort_in_memory(max_rows_sort_in_memory_)
        , index_bucket_number(index_bucket_number_)
    {}

    ~MergeTreeIndexSSTSet() override = default;

    bool supportsBulkFiltering() const override
    {
        return true;
    }

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexBulkGranulesPtr createIndexBulkGranules() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;
private:
    MergeTreeDataPartPtr data_part;
    size_t max_rows_sort_in_memory;
    size_t index_bucket_number;
};

}

