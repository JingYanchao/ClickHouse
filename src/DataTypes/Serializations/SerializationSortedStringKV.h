#pragma once

#include <DataTypes/Serializations/SerializationTuple.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>

namespace DB
{

class IMergeTreeDataPart;
class IDataPartStorage;
class WriteBuffer;
class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;
class SSTFileWriter;
class SSTFileReader;
template <bool WithVersion> struct UniqueValueEntry;

/// Extensible enum for different SortedStringKV value layouts.
enum class ValueType : uint8_t
{
    /// value = UInt64 (part_offset only)
    PartOffset = 0,

    /// value = Tuple(UInt64, UInt64) i.e. (version, part_offset)
    VersionedPartOffset = 1,
};

/// Compile-time traits that encapsulate how to read/write UniqueValueEntry
/// from/to the value subcolumn(s) of a SortedStringKV Tuple.
///
/// Each ValueType has a corresponding specialization.
/// To add a new value type, specialize this template and add the enum entry.
template <ValueType V>
struct ValueTraits;

/// PartOffset: value column is a plain ColumnUInt64 (part_offset only).
template <>
struct ValueTraits<ValueType::PartOffset>
{
    static constexpr bool has_version = false;
    using Entry = UniqueValueEntry<has_version>;

    static Entry readEntry(const IColumn & value_column, size_t row);
    static void writeEntry(IColumn & value_column, const Entry & entry);
};

/// VersionedPartOffset: value column is ColumnTuple(ColumnUInt64, ColumnUInt64)
/// i.e. Tuple(version, part_offset).
template <>
struct ValueTraits<ValueType::VersionedPartOffset>
{
    static constexpr bool has_version = true;
    using Entry = UniqueValueEntry<has_version>;

    static Entry readEntry(const IColumn & value_column, size_t row);
    static void writeEntry(IColumn & value_column, const Entry & entry);
};


/// SortedStringKV: KV pairs stored in SST files with offset-based seeking.
///
/// Template parameter V selects the value layout at compile time:
///   - `PartOffset`:          value = UInt64
///   - `VersionedPartOffset`: value = Tuple(UInt64, UInt64)
///
/// File layout:
///   column.offsets.bin  - per-granule row offsets for seeking
///   column.sst          - SST file with all KV pairs (global per part)
///
/// The SST stream is managed externally by MergeTreeDataPartWriter and injected via getters.
template <ValueType V>
class SerializationSortedStringKV final : public SerializationTuple
{
public:
    /// Write state: holds reference to SSTFileWriter for streaming KV pairs
    struct SerializeBinaryBulkStateSST : public SerializeBinaryBulkState
    {
        SSTFileWriter * sst_file_writer = nullptr;
    };

    /// Read state: SST reader with lazy initialization (clone-safe)
    struct DeserializeBinaryBulkStateSortedStringKV : public DeserializeBinaryBulkState
    {
        std::shared_ptr<const SSTFileReader> sst_file_reader;
        std::unique_ptr<rocksdb::Iterator> sst_file_iterator;
        UInt64 current_row_position = 0;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            /// Return a fresh state — iterator and reader are NOT carried over.
            /// The cloned state will lazily re-init from the SST stream getter,
            /// starting from SeekToFirst with current_row_position = 0.
            return std::make_shared<DeserializeBinaryBulkStateSortedStringKV>();
        }
    };

    SerializationSortedStringKV(
        const ElementSerializations & elems_,
        bool has_explicit_names_)
        : SerializationTuple(elems_, has_explicit_names_)
    {
    }

    static constexpr ValueType value_type = V;
    static constexpr bool has_version = ValueTraits<V>::has_version;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
};

/// Extern template declarations — definitions are in the .cpp file.
extern template class SerializationSortedStringKV<ValueType::PartOffset>;
extern template class SerializationSortedStringKV<ValueType::VersionedPartOffset>;

}
