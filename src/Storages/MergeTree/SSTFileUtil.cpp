#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/file_system.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

namespace
{
class ReadBufferWrapper
{
public:
    /// Storage mode: owns the buffer, can release and re-create it.
    ReadBufferWrapper(std::unique_ptr<ReadBufferFromFileBase> buffer_, const String & file_path_, DataPartStoragePtr storage_)
        : owned_buffer(std::move(buffer_)), file_path(file_path_), storage(std::move(storage_))
    {
    }

    /// Raw mode: wraps a non-owning SeekableReadBuffer with a base offset and region size.
    /// The caller is responsible for the lifetime of raw_buffer.
    ReadBufferWrapper(SeekableReadBuffer * raw_buffer, uint64_t base_offset, uint64_t region_size)
        : raw_read_buffer(raw_buffer), raw_base_offset(base_offset), raw_region_size(region_size)
    {
    }

    void release()
    {
        std::scoped_lock lock(mutex);
        /// Only release owned buffers; raw mode is non-owning.
        if (raw_read_buffer)
            return;
        owned_buffer.reset();
    }

    /// Atomic seek + read: both operations are performed under a single lock
    /// to prevent race conditions when multiple threads share the same buffer
    /// (e.g. RocksDB's RandomAccessFile::Read from parallel dedup threads).
    size_t seekAndRead(off_t offset, int whence, char * to, size_t n) const
    {
        std::scoped_lock lock(mutex);
        seekImpl(offset, whence);
        if (raw_read_buffer)
            return raw_read_buffer->read(to, n);
        checkOrCreateReadBuffer();
        return owned_buffer->read(to, n);
    }

private:
    void seekImpl(off_t offset, int whence) const
    {
        if (raw_read_buffer)
        {
            /// In raw mode, all offsets are relative to raw_base_offset.
            if (whence == SEEK_SET)
                raw_read_buffer->seek(raw_base_offset + offset, SEEK_SET);
            else if (whence == SEEK_END)
                raw_read_buffer->seek(raw_base_offset + raw_region_size + offset, SEEK_SET);
            else
                raw_read_buffer->seek(offset, whence);
            return;
        }
        checkOrCreateReadBuffer();
        owned_buffer->seek(offset, whence);
    }

    void checkOrCreateReadBuffer() const
    {
        if (!owned_buffer && storage)
            owned_buffer = storage->readFile(file_path, ReadSettings(), std::nullopt);
    }

    mutable std::unique_ptr<ReadBufferFromFileBase> owned_buffer;
    String file_path;
    DataPartStoragePtr storage;

    /// Raw (non-owning) mode fields
    SeekableReadBuffer * raw_read_buffer = nullptr;
    uint64_t raw_base_offset = 0;
    uint64_t raw_region_size = 0;

    mutable std::mutex mutex;
};
using ReadBufferWrapperPtr = std::shared_ptr<ReadBufferWrapper>;
class ReadBufferBasedSequentialFile : public rocksdb::FSSequentialFile
{
public:
    explicit ReadBufferBasedSequentialFile(const ReadBufferWrapperPtr & read_buffer_)
        : read_buffer(read_buffer_)
        , position(0)
    {
    }

    rocksdb::IOStatus Read(
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) override
    {
        auto bytes_read = read_buffer->seekAndRead(position, SEEK_SET, scratch, n);
        position += bytes_read;
        *result = rocksdb::Slice(scratch, bytes_read);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Skip(uint64_t n) override
    {
        position += n;
        return rocksdb::IOStatus::OK();
    }

private:
    ReadBufferWrapperPtr read_buffer;
    uint64_t position;
};

class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
{
public:
    explicit ReadBufferBasedRandomAccessFile(const ReadBufferWrapperPtr & read_buffer_)
        : read_buffer(read_buffer_)
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
        auto bytes_read = read_buffer->seekAndRead(offset, SEEK_SET, scratch, n);
        *result = rocksdb::Slice(scratch, bytes_read);
        return rocksdb::IOStatus::OK();
    }

private:
    ReadBufferWrapperPtr read_buffer;
};

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
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Flush(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Sync(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
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

/// Suppress -Wformat-nonliteral for the entire helper because the format
/// string is forwarded from rocksdb::Logger::Logv and is never a literal.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"

std::string formatToString(const char * format, va_list ap)
{
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int size = vsnprintf(nullptr, 0, format, ap_copy);
    va_end(ap_copy);

    if (size < 0)
        return {};

    std::string result(static_cast<size_t>(size), '\0');
    vsnprintf(result.data(), static_cast<size_t>(size) + 1, format, ap);
    return result;
}

#pragma GCC diagnostic pop


class CHLoggerWrapper : public rocksdb::Logger
{
public:
    CHLoggerWrapper() : logger(getLogger("SSTFileEnv")) {}

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


class ReadBufferFileSystem : public rocksdb::FileSystem
{
public:
    /// Construct from DataPartStorage: files are opened by name through the storage layer.
    explicit ReadBufferFileSystem(const DataPartStoragePtr & storage_)
        : storage(storage_)
    {
    }

    /// Construct from a raw SeekableReadBuffer: a single pre-opened buffer is used for all reads.
    /// FileExists always succeeds; GetFileSize returns the supplied region_size.
    ReadBufferFileSystem(SeekableReadBuffer * read_buffer_, uint64_t base_offset_, uint64_t region_size_)
        : raw_read_buffer(read_buffer_), raw_base_offset(base_offset_), raw_region_size(region_size_)
    {
    }

    const char* Name() const override { return "ReadBufferFileSystem"; }

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

    rocksdb::IOStatus NewLogger(
        const std::string&,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*) override
    {
        *result = std::make_shared<CHLoggerWrapper>();
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus FileExists(
        const std::string & f,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        if (storage)
        {
            if (storage->existsFile(f))
                return rocksdb::IOStatus::OK();
            else
                return rocksdb::IOStatus::NotFound();
        }
        /// Raw buffer mode: the file always "exists"
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus GetFileSize(
        const std::string & f,
        const rocksdb::IOOptions &,
        uint64_t * res,
        rocksdb::IODebugContext *) override
    {
        if (storage)
            *res = storage->getFileSize(f);
        else
            *res = raw_region_size;
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
        ReadBufferWrapperPtr read_buffer_wrapper;
        if (storage)
        {
            auto buf = storage->readFile(file_path, ReadSettings(), std::nullopt);
            read_buffer_wrapper = std::make_shared<ReadBufferWrapper>(std::move(buf), file_path, storage);
        }
        else
        {
            /// Raw buffer mode: wrap the pre-opened SeekableReadBuffer (non-owning).
            read_buffer_wrapper = std::make_shared<ReadBufferWrapper>(raw_read_buffer, raw_base_offset, raw_region_size);
        }
        {
            std::scoped_lock lock(read_buffers_manage_mutex);
            created_buffer_wrappers.emplace_back(read_buffer_wrapper);
        }
        *result = std::make_unique<ReadBufferFileType>(read_buffer_wrapper);
        return rocksdb::IOStatus::OK();
    }
    DataPartStoragePtr storage;
    SeekableReadBuffer * raw_read_buffer = nullptr;
    uint64_t raw_base_offset = 0;
    uint64_t raw_region_size = 0;
    std::mutex read_buffers_manage_mutex;
    std::vector<ReadBufferWrapperPtr> created_buffer_wrappers;
};

class WriteBufferFileSystem : public rocksdb::FileSystem
{
public:
    explicit WriteBufferFileSystem(WriteBuffer * write_buffer_)
        : write_buffer(write_buffer_)
    {
    }

    const char* Name() const override { return "WriteBufferFileSystem"; }
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
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
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

}

SSTFileWriteStream::SSTFileWriteStream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    size_t buf_size,
    const WriteSettings & query_write_settings)
    : escaped_column_name(escaped_column_name_)
    , plain_file(data_part_storage->writeFile(escaped_column_name_ + SST_DATA_FILE_EXTENSION, buf_size, query_write_settings))
    , hashing(std::make_unique<HashingWriteBuffer>(*plain_file))
    , sst_writer(std::make_unique<SSTFileWriter>(&getWriteBuffer()))
{
}

SSTFileWriteStream::~SSTFileWriteStream() = default;

void SSTFileWriteStream::preFinalize()
{
    if (pre_finalized)
        return;
    pre_finalized = true;

    hashing->finalize();
    plain_file->preFinalize();
}

void SSTFileWriteStream::finalize()
{
    preFinalize();
    plain_file->finalize();
}

void SSTFileWriteStream::cancel() noexcept
{
    hashing->cancel();
    plain_file->cancel();
}

void SSTFileWriteStream::sync() const
{
    plain_file->sync();
}

SSTFileWriter & SSTFileWriteStream::getSSTWriter()
{
    if (!sst_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTFileWriter is not initialized in SSTFileWriteStream");
    return *sst_writer;
}

void SSTFileWriteStream::fillSSTChecksums(MergeTreeDataPartChecksums & checksums)
{
    if (sst_writer)
        sst_writer->finish();

    preFinalize();
    addToChecksums(checksums);
}

void SSTFileWriteStream::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + SST_DATA_FILE_EXTENSION].is_compressed = false;
    checksums.files[name + SST_DATA_FILE_EXTENSION].file_size = hashing->count();
    checksums.files[name + SST_DATA_FILE_EXTENSION].file_hash = hashing->getHash();
}

std::unique_ptr<rocksdb::Env> createWriteBufferFileSystemEnv(WriteBuffer * write_buffer)
{
    return rocksdb::NewCompositeEnv(std::make_shared<WriteBufferFileSystem>(write_buffer));
}

std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(const DataPartStoragePtr & storage)
{
    return rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(storage));
}

void SSTFileReader::init(const String & file_name)
{
    rocksdb::Options options;
    options.env = sst_env.get();

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    auto local_reader = std::make_unique<rocksdb::SstFileReader>(options);
    auto status = local_reader->Open(file_name);
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to open SST reader for {}: {}", file_name, status.ToString());

    status = local_reader->VerifyChecksum();
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to verify SST checksum for {}: {}", file_name, status.ToString());

    index_reader = std::move(local_reader);

    /// Load min/max key range
    rocksdb::ReadOptions read_opts;
    read_opts.fill_cache = false;
    auto iter = newIterator(read_opts);
    iter->SeekToFirst();
    if (iter->Valid())
    {
        auto min_key = iter->key().ToString();
        iter->SeekToLast();
        auto max_key = iter->key().ToString();
        key_range = std::make_pair(std::move(min_key), std::move(max_key));
    }
}

SSTFileReader::SSTFileReader(SeekableReadBuffer * read_buffer, uint64_t base_offset, uint64_t region_size)
{
    sst_env = rocksdb::NewCompositeEnv(
        std::make_shared<ReadBufferFileSystem>(read_buffer, base_offset, region_size));
    init("");
}

SSTFileReader::SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name)
{
    sst_env = rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(storage));
    init(sst_file_name);
}

void SSTFileReader::releaseBufferMemory() const
{
    if (!sst_env)
        return;

    auto * fs = static_cast<ReadBufferFileSystem *>(sst_env->GetFileSystem().get());
    if (fs)
    {
        /// Release all ReadBuffer memory (1MB per file).
        /// Bloom filter and block cache remain.
        fs->releaseAllBufferMemory();
    }
}

bool SSTFileReader::get(const rocksdb::Slice & key, std::string * value_out) const
{
    std::vector<rocksdb::Slice> keys = {key};
    std::vector<std::string> values;
    auto statuses = index_reader->MultiGet(rocksdb::ReadOptions(), keys, &values);

    if (statuses.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "MultiGet returned empty statuses for single key lookup");

    if (statuses[0].ok())
    {
        if (value_out)
            *value_out = std::move(values[0]);
        return true;
    }
    else if (statuses[0].IsNotFound())
    {
        return false;
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to get key from unique index: {}", statuses[0].ToString());
    }
}

std::unique_ptr<rocksdb::Iterator> SSTFileReader::newIterator(const rocksdb::ReadOptions & options) const
{
    if (!index_reader)
        return std::unique_ptr<rocksdb::Iterator>(rocksdb::NewEmptyIterator());
    std::unique_ptr<rocksdb::Iterator> res;
    res.reset(index_reader->NewIterator(options));
    return res;
}

bool SSTFileReader::mayContainKey(std::string_view key) const
{
    return key >= key_range.first && key <= key_range.second;
}

void SSTFileReader::verifyChecksums() const
{
    auto status = index_reader->VerifyChecksum(rocksdb::ReadOptions{});
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to verify checksums of SST: {}", status.ToString());
}

SSTFileReader::IndexPropertiesPtr SSTFileReader::getProperties() const
{
    return index_reader->GetTableProperties();
}

bool SSTFileReader::isEmpty() const
{
    return key_range.first.empty() && key_range.second.empty();
}

SSTFileWriter::SSTFileWriter(WriteBuffer * write_buffer)
{
    sst_env = createWriteBufferFileSystemEnv(write_buffer);
    rocksdb::Options options;
    options.env = sst_env.get();

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    writer = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options);
    auto status = writer->Open("");
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to open SST writer: {}", status.ToString());
}

void SSTFileWriter::put(const rocksdb::Slice & key, const rocksdb::Slice & value)
{
    auto status = writer->Put(key, value);
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to put key-value to SST: {}", status.ToString());
    has_entries = true;
}

void SSTFileWriter::finish()
{
    if (finished)
        return;
    finished = true;

    if (!writer)
        return;

    /// Skip empty SST files (RocksDB returns error for empty finish)
    if (!has_entries)
        return;

    auto status = writer->Finish();
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to finish SST file: {}", status.ToString());
}

uint64_t SSTFileWriter::fileSize() const
{
    if (!writer)
        return 0;
    return writer->FileSize();
}

}
