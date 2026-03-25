#include <DataTypes/DataTypeSortedStringKV.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/Serializations/SerializationSortedStringKV.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// Build the value DataType for a given ValueType.
///   PartOffset          -> SimpleAggregateFunction(max, UInt64)
///   VersionedPartOffset -> SimpleAggregateFunction(max, Tuple(UInt64, UInt64))
template <ValueType V>
static DataTypePtr makeValueType()
{
    AggregateFunctionProperties properties;
    auto action = NullsAction::EMPTY;

    if constexpr (V == ValueType::PartOffset)
    {
        auto uint64_type = std::make_shared<DataTypeUInt64>();
        DataTypes arg_types = {uint64_type};
        auto max_func = AggregateFunctionFactory::instance().get("max", action, arg_types, {}, properties);
        return createSimpleAggregateFunctionType(max_func, arg_types, {});
    }
    else if constexpr (V == ValueType::VersionedPartOffset)
    {
        /// Tuple max uses lexicographic comparison:
        /// first compares version, then part_offset as tie-breaker.
        auto inner_tuple_type = std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()});
        DataTypes arg_types = {inner_tuple_type};
        auto max_func = AggregateFunctionFactory::instance().get("max", action, arg_types, {}, properties);
        return createSimpleAggregateFunctionType(max_func, arg_types, {});
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported SortedStringKV ValueType");
    }
}

/// Build a SortedStringKV DataType with the specified value layout.
/// The key is always String; the value structure is determined by template parameter V.
template <ValueType V>
static DataTypePtr createSortedStringKVType()
{
    auto key_type = std::make_shared<DataTypeString>();
    auto value_type_dt = makeValueType<V>();

    DataTypePtr type = std::make_shared<DataTypeTuple>(
        DataTypes{key_type, value_type_dt},
        Strings{"key", "value"});

    auto key_ser = std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(key_type->getDefaultSerialization(), "key", SubstreamType::TupleElement));
    auto val_ser = std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(value_type_dt->getDefaultSerialization(), "value", SubstreamType::TupleElement));
    typename SerializationSortedStringKV<V>::ElementSerializations elems = {key_ser, val_ser};

    auto custom_name = std::make_unique<DataTypeSortedStringKV<V>>();

    type->setCustomization(std::make_unique<DataTypeCustomDesc>(
        std::move(custom_name),
        std::make_shared<SerializationSortedStringKV<V>>(elems, true /* has_explicit_names */)));

    return type;
}

static DataTypePtr create(const ASTPtr & /* arguments */)
{
    return createSortedStringKVType<ValueType::PartOffset>();
}

static DataTypePtr createVersioned(const ASTPtr & /* arguments */)
{
    return createSortedStringKVType<ValueType::VersionedPartOffset>();
}

void registerDataTypeSortedStringKV(DataTypeFactory & factory)
{
    factory.registerDataType("SortedStringKV", create);
    factory.registerDataType("VersionedSortedStringKV", createVersioned);
}

}
