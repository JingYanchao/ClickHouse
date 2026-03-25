#pragma once

#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/SSTFileUtil.h>

#include <memory>

namespace DB
{

/// Big-endian encoded value stored in the SortedStringKV column.
///
/// Two layouts are supported:
///   Without version: [8 bytes part_offset BE]             = 8 bytes
///   With version:    [8 bytes version BE][8 bytes part_offset BE] = 16 bytes
///
/// Lexicographic comparison on the encoded string == numeric comparison,
/// which is required by SimpleAggregateFunction(max, ...) dedup semantics.
/// For the versioned layout, tuple(version, part_offset) comparison is
/// equivalent to comparing version first, then part_offset as tie-breaker.
struct UniqueValueEntry
{
    UInt64 version = 0;
    UInt64 part_offset = 0;

    /// Encode into big-endian string.
    /// Without version: 8-byte BE(part_offset)
    /// With version: 16-byte BE(version) ++ BE(part_offset)
    String encode(bool with_version = false) const;

    /// Decode from a raw value buffer. Automatically detects format by size:
    ///   8 bytes  -> without version (version = 0)
    ///   16 bytes -> with version
    static UniqueValueEntry decode(const char * data, size_t size);

    /// Convenience overload for StringRef / std::string_view.
    static UniqueValueEntry decode(std::string_view sv) { return decode(sv.data(), sv.size()); }
};

/// Unique Projection Index: deduplicates rows based on user-specified unique key columns.
///
/// Each projection part stores a single SortedStringKV column mapping
/// unique_key -> _parent_part_offset.
///
/// DDL syntax:
///   PROJECTION unique_idx INDEX id TYPE unique
///   PROJECTION unique_idx INDEX (id, name) TYPE unique
///   PROJECTION unique_idx INDEX id TYPE unique('ver')   -- with version column
class ProjectionIndexUnique : public IProjectionIndex
{
public:
    static constexpr auto name = "unique";

    /// Centralized naming constants for unique projection infrastructure.
    /// All code that references these names should use these constants
    /// instead of hardcoding strings to avoid inconsistencies.
    static constexpr auto kv_column_name = "_unique_kv";
    static constexpr auto default_projection_name = "__unique_index";
    /// SST file name stored inside the projection part directory.
    /// Derived from kv_column_name + SST_DATA_FILE_EXTENSION.
    static inline const String sst_file_name = String(kv_column_name) + SST_DATA_FILE_EXTENSION;

    /// Create from AST: extracts unique key column names from the INDEX expression.
    static ProjectionIndexPtr create(const ASTProjectionDeclaration & proj);

    explicit ProjectionIndexUnique(Names unique_key_columns_, String version_column_name_ = {});

    String getName() const override { return name; }

    /// Build projection metadata: single SortedStringKV column, empty primary key, sorting by kv key.
    void fillProjectionDescription(
        ProjectionDescription & result,
        const IAST * index_expr,
        const ColumnsDescription & columns,
        ContextPtr query_context) const override;

    /// Calculate projection block from source block: serialize unique keys,
    /// compute version + part_offset, perform in-block dedup.
    Block calculate(
        const ProjectionDescription & projection_desc,
        const Block & block,
        UInt64 starting_offset,
        ContextPtr context,
        const IColumnPermutation * perm_ptr) const override;

    std::shared_ptr<MergeTreeSettings> getDefaultSettings() const override;

    const IndexDescription * getIndexDescription() const override;

    MergeTreeIndexPtr getIndex() const override;

    UInt64 getMaxRows() const override { return std::numeric_limits<UInt32>::max(); }

    const String & getVersionColumnName() const { return version_column_name; }

private:
    /// Serialize unique key columns from a block row into a single comparable string.
    static String serializeKeyColumns(const Block & block, const Names & key_columns, size_t row_index);
    Names unique_key_columns;
    String version_column_name;
    IndexDescription index_description;
    std::shared_ptr<const IMergeTreeIndex> index;
};

}
