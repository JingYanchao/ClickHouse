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

static DataTypePtr create(const ASTPtr & /* arguments */)
{
    auto key_type = std::make_shared<DataTypeString>();

    /// Value type: SimpleAggregateFunction(max, UInt64)
    /// Stores _parent_part_offset. Using max semantics ensures that during
    /// merge the entry with the highest offset wins. Cross-part dedup is
    /// handled by the rebuild path (with_parent_part_offset = true), so
    /// no separate version field is needed.
    /// At serialization time, the UInt64 is encoded into UniqueValueEntry
    /// (8-byte big-endian format) for SST storage.
    auto uint64_type = std::make_shared<DataTypeUInt64>();

    AggregateFunctionProperties properties;
    auto action = NullsAction::EMPTY;
    DataTypes arg_types = {uint64_type};
    auto max_func = AggregateFunctionFactory::instance().get("max", action, arg_types, {}, properties);
    auto value_type = createSimpleAggregateFunctionType(max_func, arg_types, {});

    DataTypePtr type = std::make_shared<DataTypeTuple>(
        DataTypes{key_type, value_type},
        Strings{"key", "value"});

    auto key_ser = std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(key_type->getDefaultSerialization(), "key", SubstreamType::TupleElement));
    auto val_ser = std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(value_type->getDefaultSerialization(), "value", SubstreamType::TupleElement));
    SerializationSortedStringKV::ElementSerializations elems = {key_ser, val_ser};

    type->setCustomization(std::make_unique<DataTypeCustomDesc>(
        std::make_unique<DataTypeSortedStringKV>(),
        std::make_shared<SerializationSortedStringKV>(elems, true /* has_explicit_names */)));

    return type;
}

void registerDataTypeSortedStringKV(DataTypeFactory & factory)
{
    factory.registerDataType("SortedStringKV", create);
}

}
