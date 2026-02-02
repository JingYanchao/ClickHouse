#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSet.h>
#include <Storages/MergeTree/MergeTreeIndexSSTSetWriter.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Functions/FunctionFactory.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Storages/MergeTree/MergeTreeIndexSet.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/file_system.h>
#include <filesystem>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
}

namespace
{
class ReadBufferWrapper
{
public:
    explicit ReadBufferWrapper(std::unique_ptr<ReadBufferFromFileBase> buffer_, const String & file_path_, DataPartStoragePtr storage_)
        : buffer(std::move(buffer_)), file_path(file_path_), storage(storage_)
    {
    }

    void release()
    {
        std::scoped_lock lock(mutex);
        buffer.reset();
    }

    size_t read(char * to, size_t n) const
    {
        std::scoped_lock lock(mutex);
        checkOrCreateReadBuffer();
        return buffer->read(to, n);
    }

    void ignore(uint64_t n) const
    {
        std::scoped_lock lock(mutex);
        checkOrCreateReadBuffer();
        buffer->ignore(n);
    }

    void seek(off_t offset, int whence) const
    {
        std::scoped_lock lock(mutex);
        checkOrCreateReadBuffer();
        buffer->seek(offset, whence);
    }

    void checkOrCreateReadBuffer() const
    {
        if (!buffer)
            buffer = storage->readFile(file_path, ReadSettings(), std::nullopt);
    }

private:
    mutable std::unique_ptr<ReadBufferFromFileBase> buffer;
    String file_path;
    DataPartStoragePtr storage;
    mutable std::mutex mutex;
};

using ReadBufferWrapperPtr = std::shared_ptr<ReadBufferWrapper>;
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
    explicit ReadBufferBasedSequentialFile(ReadBufferWrapperPtr wrapper_)
        : wrapper(std::move(wrapper_))
    {
    }

    rocksdb::IOStatus Read(
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) override
    {
        auto read = wrapper->read(scratch, n);
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Skip(uint64_t n) override
    {
        wrapper->ignore(n);
        return rocksdb::IOStatus::OK();
    }
private:
    ReadBufferWrapperPtr wrapper;
};

class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
{
public:
    explicit ReadBufferBasedRandomAccessFile(ReadBufferWrapperPtr wrapper_)
        : wrapper(std::move(wrapper_))
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
        wrapper->seek(offset, SEEK_SET);
        auto read = wrapper->read(scratch, n);
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }
private:
    ReadBufferWrapperPtr wrapper;
};

std::string formatToString(const char * format, va_list ap)
{
    va_list ap_copy;
    va_copy(ap_copy, ap);

    /// Disable compile error caused by '-Wformat-nonliteral'
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

    // Get size
    int size = vsnprintf(nullptr, 0, format, ap_copy);
    va_end(ap_copy);

    if (size < 0)
        return "";  /// Error

    // Create string buffer
    std::string result(size + 1, '\0');
    // Copy to the buffer
    if (vsnprintf(result.data(), size + 1, format, ap) < 0)
        return "";  /// Error

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

    return result;
}

class CHLoggerWrapper : public rocksdb::Logger
{
public:
    CHLoggerWrapper() : logger(getLogger("DiskBasedUniqueIndexEnv")) {}

    rocksdb::Status Close() override
    {
        logger.reset();
        return rocksdb::Status();
    }

    void Logv(
        const rocksdb::InfoLogLevel log_level,
        const char * format,
        va_list ap) override
    {
        auto msg = formatToString(format, ap);
        switch (log_level)
        {
            case rocksdb::InfoLogLevel::DEBUG_LEVEL:
                LOG_DEBUG(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::INFO_LEVEL:
                LOG_INFO(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::WARN_LEVEL:
                LOG_WARNING(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::ERROR_LEVEL:
                LOG_ERROR(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::FATAL_LEVEL:
                LOG_FATAL(logger, "{}", msg);
                break;
            default:
                LOG_INFO(logger, "{}", msg);
                break;
        }
    }
private:
    LoggerPtr logger;
};

class WriteBufferBasedFileSystem : public rocksdb::FileSystem
{
public:
    explicit WriteBufferBasedFileSystem(WriteBuffer * write_buffer_) : write_buffer(write_buffer_)
    {
    }
    const char* Name() const override { return "WriteBufferBasedFileSystem"; }
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

    rocksdb::IOStatus NewLogger(
        const std::string&,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*) override
    {
        *result = std::make_shared<CHLoggerWrapper>();
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods:
    rocksdb::IOStatus NewSequentialFile(
    const std::string &,
    const rocksdb::FileOptions &,
    std::unique_ptr<rocksdb::FSSequentialFile> *,
    rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported();}
    rocksdb::IOStatus NewRandomAccessFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus FileExists(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileSize(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
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
    WriteBuffer* write_buffer = nullptr;
};

class ReadDataPartStorageBasedFileSystem : public rocksdb::FileSystem
{
public:
    explicit ReadDataPartStorageBasedFileSystem(const DataPartStoragePtr & storage_)
        : storage(storage_)
    {
    }

    /// Release all ReadBuffer memory from created files
    void releaseAllBufferMemory()
    {
        std::scoped_lock lock(read_buffers_manage_mutex);
        for (const auto & wrapper : created_buffer_wrappers)
        {
            if (wrapper)
                wrapper->release();
        }
    }

    const char* Name() const override { return "ReadDataPartStorageBasedFileSystem"; }

    rocksdb::IOStatus NewSequentialFile(
        const std::string & f,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSSequentialFile> * r,
        rocksdb::IODebugContext *) override
    {
        return createFile<ReadBufferBasedSequentialFile>(f, r);
    }

    rocksdb::IOStatus NewRandomAccessFile(
        const std::string & f,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> * r,
        rocksdb::IODebugContext *) override
    {
        return createFile<ReadBufferBasedRandomAccessFile>(f, r);
    }

    rocksdb::IOStatus FileExists(
        const std::string & f,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        if (storage->existsFile(f))
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

    rocksdb::IOStatus NewLogger(
        const std::string&,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*) override
    {
        *result = std::make_shared<CHLoggerWrapper>();
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods:
    rocksdb::IOStatus NewWritableFile(
    const std::string &,
    const rocksdb::FileOptions &,
    std::unique_ptr<rocksdb::FSWritableFile> *,
    rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
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
    template <typename ReadBufferFileType>
    rocksdb::IOStatus createFile(
        const std::string & file_path,
        auto * result)
    {
        auto read_buffer = storage->readFile(file_path, ReadSettings(), std::nullopt);
        auto read_buffer_wrapper = std::make_shared<ReadBufferWrapper>(std::move(read_buffer), file_path, storage);
        {
            std::scoped_lock lock(read_buffers_manage_mutex);
            created_buffer_wrappers.emplace_back(read_buffer_wrapper);
        }
        *result = std::make_unique<ReadBufferFileType>(read_buffer_wrapper);
        return rocksdb::IOStatus::OK();
    }
    DataPartStoragePtr storage;
    std::mutex read_buffers_manage_mutex;
    std::vector<ReadBufferWrapperPtr> created_buffer_wrappers;
};
}

std::unique_ptr<rocksdb::Env> createReadSSTFileEnv(std::shared_ptr<const IDataPartStorage> storage)
{
    return rocksdb::NewCompositeEnv(std::make_shared<ReadDataPartStorageBasedFileSystem>(std::move(storage)));
}

std::unique_ptr<rocksdb::Env> createWriteSSTFileEnv(WriteBuffer * write_buffer)
{
    return rocksdb::NewCompositeEnv(std::make_shared<WriteBufferBasedFileSystem>(write_buffer));
}

MergeTreeSSTFileReader::MergeTreeSSTFileReader(const IMergeTreeDataPart & part, const String & sst_file_name)
{
    if (unlikely(part.isEmpty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty part {} is not allowed to create SST index", part.info.getPartNameForLogs());

    Stopwatch watch;
    auto part_storage = part.getDataPartStoragePtr();

    rocksdb::Options options;
    String file_path;
    
    if (part_storage->isStoredOnRemoteDisk())
    {
        /// Use relative file path for remote disk
        file_path = sst_file_name;
        /// Use ClickHouse disk for SST index on remote disk (object store)
        sst_env = createReadSSTFileEnv(part_storage);
        options.env = sst_env.get();
    }
    else
    {
        /// Use absolute file path for local disk
        file_path = std::string(std::filesystem::path(part_storage->getFullPath()) / sst_file_name);
    }
    
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    
    auto local_reader = std::make_unique<rocksdb::SstFileReader>(options);
    auto status = local_reader->Open(file_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Failed to open SST index file {}: {}", file_path, status.ToString());
    
    index_reader = std::move(local_reader);
    
    /// Load min max key
    rocksdb::ReadOptions read_opts;
    read_opts.fill_cache = false;
    auto iter = newIterator(read_opts);
    iter->SeekToFirst();
    auto min_key = iter->key().ToString();
    iter->SeekToLast();
    auto max_key = iter->key().ToString();

    if (min_key.empty() && !max_key.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "If min key is empty, all the keys should be empty which indicates the index is empty");

    key_range = std::make_pair(std::move(min_key), std::move(max_key));
    LOG_TEST(getLogger(part.storage.getLogName()), "Load SST index of part {} in {}ms",
        part.info.getPartNameForLogs(), watch.elapsedMilliseconds());
}

std::unique_ptr<rocksdb::Iterator> MergeTreeSSTFileReader::newIterator(const rocksdb::ReadOptions & options) const
{
    if (!index_reader)
        return std::unique_ptr<rocksdb::Iterator>(rocksdb::NewEmptyIterator());
    std::unique_ptr<rocksdb::Iterator> res;
    res.reset(index_reader->NewIterator(options));
    return res;
}

// bool MergeTreeSSTFileReader::get(const rocksdb::Slice & key, std::string * value_out) const
// {
//     auto status = index_reader->Get(rocksdb::ReadOptions(), key, value_out, false);
//     if (status.ok())
//         return true;
//     else if (status.IsNotFound())
//         return false;
//     else
//         throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to get key from unique index: {}", status.ToString());
// }

bool MergeTreeSSTFileReader::keyRangeIntersects(const MergeTreeSSTFileReader & other) const
{
    if (isEmpty() || other.isEmpty())
        return false;
    if (other.key_range.second < key_range.first)
        return false;
    if (other.key_range.first > key_range.second)
        return false;
    return true;
}

bool MergeTreeSSTFileReader::mayContainKey(std::string_view key) const
{
    return key >= key_range.first && key <= key_range.second;
}

void MergeTreeSSTFileReader::verifyChecksums() const
{
    auto status = index_reader->VerifyChecksum(rocksdb::ReadOptions{});
    if (!status.ok())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Failed to verify checksums of index: {}", status.ToString());
}

MergeTreeSSTFileReader::IndexPropertiesPtr MergeTreeSSTFileReader::getProperties() const
{
    return index_reader->GetTableProperties();
}

bool MergeTreeSSTFileReader::isEmpty() const
{
    return key_range.first.empty() && key_range.second.empty();
}

void MergeTreeSSTFileReader::releaseBufferMemory() const
{
    if (!sst_env)
        return;

    // Get the ReadDataPartStorageBasedFileSystem from the Env
    auto * fs = static_cast<ReadDataPartStorageBasedFileSystem *>(sst_env->GetFileSystem().get());

    if (fs)
    {
        // Release all ReadBuffer memory (1MB per file)
        // Bloom Filter and Block Cache remain
        fs->releaseAllBufferMemory();
    }
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
    try
    {
        /// Create MergeTreeSSTFileReader to read the SST file
        /// No bucket parameter needed anymore
        sst_reader = std::make_shared<MergeTreeSSTFileReader>(state.part, index_name);
        
        /// Check if the index is empty
        if (sst_reader->isEmpty())
        {
            return;
        }
        
        /// The SST file reader is now stored in sst_reader member variable
        /// and can be used later for querying without loading all data into memory
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
                        "Failed to deserialize SST Set index '{}' for part {}: {}", 
                        index_name, state.part.name, e.message());
    }
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

MergeTreeIndexGranulePtr MergeTreeIndexSSTSet::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSSTSet>(index.name, index.sample_block);
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

void SSTSetIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.arguments.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SST Set index must have exactly one argument");
    if (index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SST Set index argument must be positive integer");
}

}
