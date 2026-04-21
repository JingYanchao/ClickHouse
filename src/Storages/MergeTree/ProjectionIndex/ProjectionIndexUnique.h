#pragma once

#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <DataTypes/Serializations/SerializationSortedStringKV.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <fmt/format.h>

#include <memory>

namespace DB
{

/// Big-endian encoded value stored in the SortedStringKV column.
///
/// Template parameter `WithVersion` controls the layout:
///   UniqueValueEntry<false>:  [8 bytes part_offset]                      =  8 bytes
///   UniqueValueEntry<true>:   [8 bytes version] + [8 bytes part_offset]  = 16 bytes
///
/// Lexicographic comparison on the encoded bytes == numeric comparison,
/// which is required by SimpleAggregateFunction(max, ...) dedup semantics.
namespace detail
{
    template <bool WithVersion>
    struct VersionBase { UInt64 version = 0; };

    template <>
    struct VersionBase<false> {};
}

template <bool WithVersion>
struct UniqueValueEntry : detail::VersionBase<WithVersion>
{
    UInt64 part_offset = 0;

    /// Encode into big-endian bytes for SST storage.
    String encode() const;

    /// Decode from raw value buffer.
    static UniqueValueEntry decode(const char * data, size_t size);
    static UniqueValueEntry decode(std::string_view sv) { return decode(sv.data(), sv.size()); }

    /// Helper to get version value (returns 0 when WithVersion=false).
    UInt64 getVersion() const
    {
        if constexpr (WithVersion)
            return this->version;
        else
            return 0;
    }
};

/// Non-template alias for runtime decode (e.g. dedup manager).
/// Always uses the versioned layout; for 8-byte values, version = 0.
using UniqueValueEntryFull = UniqueValueEntry<true>;

/// Runtime decode that auto-detects format by value size (8 or 16 bytes).
UniqueValueEntryFull decodeUniqueValueEntry(const char * data, size_t size);
inline UniqueValueEntryFull decodeUniqueValueEntry(std::string_view sv) { return decodeUniqueValueEntry(sv.data(), sv.size()); }

/// Unique Projection Index: deduplicates rows based on unique key columns.
///
/// Each projection part stores a single SortedStringKV column mapping
/// unique_key -> part_offset (or (version, part_offset) with version column).
///
/// DDL examples:
///   PROJECTION p INDEX id TYPE unique
///   PROJECTION p INDEX (id, name) TYPE unique
///   PROJECTION p INDEX (x + y) TYPE unique           -- expression keys are allowed
///   PROJECTION p INDEX id TYPE unique('ver')         -- with version column
class ProjectionIndexUnique : public IProjectionIndex
{
public:
    static constexpr auto name = "unique";

    /// Naming constants for unique projection infrastructure.
    static constexpr auto kv_column_name = "_unique_kv";
    static constexpr auto default_projection_name = "__unique_index";
    /// SST file name stored inside the projection part directory.
    static String getSSTFileName()
    {
        return fmt::format("{}{}", kv_column_name, SST_DATA_FILE_EXTENSION);
    }

    /// Create from AST: keep the raw key expression list. Expression resolution and
    /// validation are deferred to `fillProjectionDescription`, which has access to
    /// the columns of the parent table.
    static ProjectionIndexPtr create(const ASTProjectionDeclaration & proj);

    explicit ProjectionIndexUnique(ASTPtr key_expression_list_, String version_column_name_ = {});

    String getName() const override { return name; }

    /// Build projection metadata: SortedStringKV column, sorting key, etc.
    void fillProjectionDescription(
        ProjectionDescription & result,
        const IAST * index_expr,
        const ColumnsDescription & columns,
        ContextPtr query_context) const override;

    /// Calculate projection block from source block: serialize unique keys
    /// and perform in-block dedup.
    Block calculate(
        const ProjectionDescription & projection_desc,
        const Block & block,
        UInt64 starting_offset,
        ContextPtr context,
        const IColumnPermutation * perm_ptr) const override;

    std::shared_ptr<MergeTreeSettings> getDefaultSettings() const override;

    UInt64 getMaxRows() const override { return std::numeric_limits<UInt32>::max(); }

    const String & getVersionColumnName() const { return version_column_name; }
    /// Returns the physical source columns the unique key expressions depend on.
    /// Valid only after `fillProjectionDescription` has compiled the key expression;
    /// before that, the key description only holds the raw AST and this returns {}.
    Names getUniqueKeyColumns() const;
    const KeyDescription & getUniqueKeyDescription() const { return unique_key_desc; }

private:
    String version_column_name;

    /// Holds the unique key expression list. Right after construction only
    /// `expression_list_ast` is populated; `fillProjectionDescription` later
    /// overwrites this with a fully-compiled `KeyDescription` (expression,
    /// sample_block, column_names, ...), which `calculate` then uses.
    mutable KeyDescription unique_key_desc;
};

}
