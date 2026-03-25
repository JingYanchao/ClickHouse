#pragma once

#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/Serializations/SerializationSortedStringKV.h>

namespace DB
{

/// Non-template base class for all SortedStringKV custom type names.
/// Reader/writer code uses `dynamic_cast<const IDataTypeSortedStringKV *>`
/// to detect any SortedStringKV variant uniformly.
class IDataTypeSortedStringKV : public IDataTypeCustomName
{
};

/// Template custom name parameterised on ValueType.
///   DataTypeSortedStringKV<PartOffset>          -> "SortedStringKV"
///   DataTypeSortedStringKV<VersionedPartOffset> -> "VersionedSortedStringKV"
template <ValueType V>
class DataTypeSortedStringKV : public IDataTypeSortedStringKV
{
public:
    String getName() const override;
};

template <>
inline String DataTypeSortedStringKV<ValueType::PartOffset>::getName() const { return "SortedStringKV"; }

template <>
inline String DataTypeSortedStringKV<ValueType::VersionedPartOffset>::getName() const { return "VersionedSortedStringKV"; }

}
