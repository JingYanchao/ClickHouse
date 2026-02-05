SET optimize_throw_if_noop = 1;

-- Test 1: Basic SimpleAggregateFunction subcolumn test
DROP TABLE IF EXISTS test_simple_subcolumn;

CREATE TABLE test_simple_subcolumn (
    id UInt64,
    metrics Tuple(
        val SimpleAggregateFunction(sum, Double)
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- First insert
INSERT INTO test_simple_subcolumn SELECT number, tuple(number) FROM system.numbers LIMIT 10;

-- Type check
SELECT 'Basic test - type check:';
SELECT toTypeName(metrics.val) FROM test_simple_subcolumn LIMIT 1;

-- Second insert to test merge behavior
INSERT INTO test_simple_subcolumn SELECT number, tuple(number) FROM system.numbers LIMIT 10;

-- Test with FINAL to verify immediate aggregation
SELECT 'Basic test - with FINAL (after two inserts):';
SELECT * FROM test_simple_subcolumn FINAL ORDER BY id;

-- Test OPTIMIZE to verify on-disk aggregation
OPTIMIZE TABLE test_simple_subcolumn FINAL;

SELECT 'Basic test - after OPTIMIZE:';
SELECT * FROM test_simple_subcolumn ORDER BY id;

DROP TABLE test_simple_subcolumn;

-- Test 2: Comprehensive SimpleAggregateFunction types in subcolumns
DROP TABLE IF EXISTS test_comprehensive_simple;

CREATE TABLE test_comprehensive_simple (
    id UInt64,
    agg_tuple Tuple(
        sum_val SimpleAggregateFunction(sum, UInt64),
        min_val SimpleAggregateFunction(min, UInt64),
        max_val SimpleAggregateFunction(max, UInt64),
        any_val SimpleAggregateFunction(any, String),
        any_last_val SimpleAggregateFunction(anyLast, String),
        nullable_str SimpleAggregateFunction(anyLast, Nullable(String)),
        nullable_str_respect_nulls SimpleAggregateFunction(anyLastRespectNulls, Nullable(String)),
        low_str SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
        ip_addr SimpleAggregateFunction(anyLast, IPv4),
        status SimpleAggregateFunction(groupBitOr, UInt32),
        bit_and SimpleAggregateFunction(groupBitAnd, UInt32),
        bit_xor SimpleAggregateFunction(groupBitXor, UInt32),
        arr SimpleAggregateFunction(groupArrayArray, Array(Int32)),
        uniq_arr SimpleAggregateFunction(groupUniqArrayArray, Array(Int32))
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- Insert first batch
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        100, 10, 50, 'first', 'first_last', '1', '1', '1', '192.168.1.1', 
        1, 15, 5, [1,2], [1,2]
    )
);

-- Insert second batch with same key (should trigger aggregation)
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        200, 5, 100, 'second', 'second_last', null, null, '2', '192.168.1.2', 
        2, 7, 3, [2,3,4], [2,3,4]
    )
);

-- Insert third batch
INSERT INTO test_comprehensive_simple VALUES(
    1, 
    tuple(
        50, 15, 75, 'third', 'third_last', '3', '3', '3', '192.168.1.3', 
        4, 3, 7, [5], [5,6]
    )
);

-- Insert data for id=10 with long string (longer than MAX_SMALL_STRING_SIZE)
INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        1000, 100, 500, 'ten', 'ten_last', '10', null, '10', '10.0.0.1', 
        8, 255, 15, [], []
    )
);

INSERT INTO test_comprehensive_simple VALUES(
    10, 
    tuple(
        2000, 50, 1000, 'twenty', '2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222', 
        '2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222', 
        '10', '20', '10.0.0.2', 
        16, 127, 31, [], []
    )
);

-- Test immediate aggregation with FINAL
SELECT 'Comprehensive test - with FINAL:';
SELECT * FROM test_comprehensive_simple FINAL ORDER BY id;

-- Type check
SELECT 'Comprehensive test - type check:';
SELECT 
    toTypeName(agg_tuple.sum_val),
    toTypeName(agg_tuple.nullable_str),
    toTypeName(agg_tuple.nullable_str_respect_nulls),
    toTypeName(agg_tuple.low_str),
    toTypeName(agg_tuple.ip_addr),
    toTypeName(agg_tuple.status),
    toTypeName(agg_tuple.arr),
    toTypeName(agg_tuple.uniq_arr)
FROM test_comprehensive_simple LIMIT 1;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_comprehensive_simple FINAL;

SELECT 'Comprehensive test - after OPTIMIZE:';
SELECT * FROM test_comprehensive_simple ORDER BY id;

DROP TABLE test_comprehensive_simple;

-- Test 3: Complex nested types with SimpleAggregateFunction
DROP TABLE IF EXISTS test_complex_types;

CREATE TABLE test_complex_types (
    id UInt64,
    complex_agg Tuple(
        sum_map SimpleAggregateFunction(sumMap, Tuple(Array(Int32), Array(Int64))),
        min_map SimpleAggregateFunction(minMap, Tuple(Array(Int32), Array(Int64))),
        max_map SimpleAggregateFunction(maxMap, Tuple(Array(Int32), Array(Int64))),
        map_uniq_arr SimpleAggregateFunction(groupUniqArrayArrayMap, Map(Int32, Array(Int64)))
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

INSERT INTO test_complex_types VALUES(
    1,
    tuple(
        ([1,2], [10,20]),
        ([1,2], [5,10]),
        ([1,2], [15,25]),
        map(1, [100,200], 2, [300,400])
    )
);

INSERT INTO test_complex_types VALUES(
    1,
    tuple(
        ([1,3], [15,30]),
        ([1,3], [3,8]),
        ([1,3], [20,35]),
        map(1, [2,3], 2, [4,5,6])
    )
);

INSERT INTO test_complex_types VALUES(
    2,
    tuple(
        ([2,3], [100,200]),
        ([2,3], [50,100]),
        ([2,3], [150,250]),
        map(3,[7,8])
    )
);

-- Test immediate aggregation with FINAL
SELECT 'Complex types test - with FINAL:';
SELECT * FROM test_complex_types FINAL ORDER BY id;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_complex_types FINAL;

SELECT 'Complex types test - after OPTIMIZE:';
SELECT * FROM test_complex_types ORDER BY id;

DROP TABLE test_complex_types;

-- Test 4: AggregateFunction subcolumns (state-based aggregation)
DROP TABLE IF EXISTS test_agg_function_subcolumn;

CREATE TABLE test_agg_function_subcolumn (
    id UInt64,
    agg_states Tuple(
        uniq_state AggregateFunction(uniq, UInt64),
        uniq_exact_state AggregateFunction(uniqExact, String),
        sum_state AggregateFunction(sum, UInt64),
        avg_state AggregateFunction(avg, Float64),
        count_state AggregateFunction(count),
        min_state AggregateFunction(min, Int64),
        max_state AggregateFunction(max, Int64),
        arg_min_state AggregateFunction(argMin, String, UInt64),
        arg_max_state AggregateFunction(argMax, String, UInt64),
        quantile_state AggregateFunction(quantile(0.5), Float64),
        group_array_state AggregateFunction(groupArray, String)
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- Insert first batch with aggregate states
INSERT INTO test_agg_function_subcolumn
SELECT
    number % 3 AS id,
    tuple(
        uniqState(toUInt64(number)),
        uniqExactState(toString(number)),
        sumState(toUInt64(number * 10)),
        avgState(toFloat64(number)),
        countState(),
        minState(toInt64(number)),
        maxState(toInt64(number)),
        argMinState(concat('val_', toString(number)), toUInt64(number)),
        argMaxState(concat('val_', toString(number)), toUInt64(number)),
        quantileState(0.5)(toFloat64(number)),
        groupArrayState(toString(number))
    ) AS agg_states
FROM numbers(10)
GROUP BY id;

-- Insert second batch with same keys (should trigger state merging)
INSERT INTO test_agg_function_subcolumn
SELECT
    number % 3 AS id,
    tuple(
        uniqState(toUInt64(number + 100)),
        uniqExactState(concat('str_', toString(number))),
        sumState(toUInt64(number * 10)),
        avgState(toFloat64(number + 50)),
        countState(),
        minState(toInt64(number - 5)),
        maxState(toInt64(number + 5)),
        argMinState(concat('new_', toString(number)), toUInt64(number + 10)),
        argMaxState(concat('new_', toString(number)), toUInt64(number + 10)),
        quantileState(0.5)(toFloat64(number + 10)),
        groupArrayState(concat('new_', toString(number)))
    ) AS agg_states
FROM numbers(10)
GROUP BY id;

-- Test state merging before OPTIMIZE
SELECT 'AggregateFunction test - before OPTIMIZE:';
SELECT
    id,
    uniqMerge(agg_states.uniq_state) AS unique_count,
    uniqExactMerge(agg_states.uniq_exact_state) AS unique_exact_count,
    sumMerge(agg_states.sum_state) AS total_sum,
    avgMerge(agg_states.avg_state) AS avg_val,
    countMerge(agg_states.count_state) AS total_count,
    minMerge(agg_states.min_state) AS min_val,
    maxMerge(agg_states.max_state) AS max_val,
    argMinMerge(agg_states.arg_min_state) AS arg_min_val,
    argMaxMerge(agg_states.arg_max_state) AS arg_max_val,
    quantileMerge(0.5)(agg_states.quantile_state) AS median_val,
    length(groupArrayMerge(agg_states.group_array_state)) AS array_length
FROM test_agg_function_subcolumn
GROUP BY id
ORDER BY id;

-- Test state merging after OPTIMIZE
OPTIMIZE TABLE test_agg_function_subcolumn FINAL;

SELECT 'AggregateFunction test - after OPTIMIZE:';
SELECT
    id,
    uniqMerge(agg_states.uniq_state) AS unique_count,
    uniqExactMerge(agg_states.uniq_exact_state) AS unique_exact_count,
    sumMerge(agg_states.sum_state) AS total_sum,
    avgMerge(agg_states.avg_state) AS avg_val,
    countMerge(agg_states.count_state) AS total_count,
    minMerge(agg_states.min_state) AS min_val,
    maxMerge(agg_states.max_state) AS max_val,
    argMinMerge(agg_states.arg_min_state) AS arg_min_val,
    argMaxMerge(agg_states.arg_max_state) AS arg_max_val,
    quantileMerge(0.5)(agg_states.quantile_state) AS median_val,
    length(groupArrayMerge(agg_states.group_array_state)) AS array_length
FROM test_agg_function_subcolumn
GROUP BY id
ORDER BY id;

DROP TABLE test_agg_function_subcolumn;

-- Test 5: Mixed SimpleAggregateFunction and AggregateFunction subcolumns
DROP TABLE IF EXISTS test_mixed_agg;

CREATE TABLE test_mixed_agg (
    key UInt32,
    mixed_agg Tuple(
        simple_sum SimpleAggregateFunction(sum, UInt64),
        simple_max SimpleAggregateFunction(max, UInt64),
        simple_min SimpleAggregateFunction(min, UInt64),
        simple_any_last SimpleAggregateFunction(anyLast, String),
        agg_uniq AggregateFunction(uniq, String),
        agg_avg AggregateFunction(avg, Float64),
        agg_count AggregateFunction(count),
        agg_quantiles AggregateFunction(quantiles(0.5, 0.9, 0.99), Float64)
    )
) ENGINE = AggregatingMergeTree() ORDER BY key;

-- Insert test data
INSERT INTO test_mixed_agg
SELECT
    1 AS key,
    tuple(
        100,
        50,
        10,
        'first',
        uniqState('value1'),
        avgState(10.5),
        countState(),
        quantilesState(0.5, 0.9, 0.99)(5.0)
    ) AS mixed_agg;

INSERT INTO test_mixed_agg
SELECT
    1 AS key,
    tuple(
        200,
        75,
        5,
        'second',
        uniqState('value2'),
        avgState(20.5),
        countState(),
        quantilesState(0.5, 0.9, 0.99)(15.0)
    ) AS mixed_agg;

INSERT INTO test_mixed_agg
SELECT
    2 AS key,
    tuple(
        50,
        30,
        15,
        'alpha',
        uniqState('alpha'),
        avgState(5.5),
        countState(),
        quantilesState(0.5, 0.9, 0.99)(3.0)
    ) AS mixed_agg;

-- Test mixed aggregate types before OPTIMIZE
SELECT 'Mixed aggregate types - before OPTIMIZE:';
SELECT
    key,
    mixed_agg.simple_sum AS sum_val,
    mixed_agg.simple_max AS max_val,
    mixed_agg.simple_min AS min_val,
    mixed_agg.simple_any_last AS any_last_val,
    uniqMerge(mixed_agg.agg_uniq) AS unique_strings,
    avgMerge(mixed_agg.agg_avg) AS avg_val,
    countMerge(mixed_agg.agg_count) AS count_val,
    quantilesMerge(0.5, 0.9, 0.99)(mixed_agg.agg_quantiles) AS quantiles_val
FROM test_mixed_agg
GROUP BY key
ORDER BY key;

-- Test after OPTIMIZE
OPTIMIZE TABLE test_mixed_agg FINAL;

SELECT 'Mixed aggregate types - after OPTIMIZE:';
SELECT
    key,
    mixed_agg.simple_sum AS sum_val,
    mixed_agg.simple_max AS max_val,
    mixed_agg.simple_min AS min_val,
    mixed_agg.simple_any_last AS any_last_val,
    uniqMerge(mixed_agg.agg_uniq) AS unique_strings,
    avgMerge(mixed_agg.agg_avg) AS avg_val,
    countMerge(mixed_agg.agg_count) AS count_val,
    quantilesMerge(0.5, 0.9, 0.99)(mixed_agg.agg_quantiles) AS quantiles_val
FROM test_mixed_agg
GROUP BY key
ORDER BY key;

DROP TABLE test_mixed_agg;

-- Test 6: With overflow
DROP TABLE IF EXISTS test_with_overflow;

CREATE TABLE test_with_overflow (
    id UInt64,
    overflow_agg Tuple(
        sum_overflow SimpleAggregateFunction(sumWithOverflow, UInt8)
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

INSERT INTO test_with_overflow SELECT 1, tuple(1) FROM numbers(256);

OPTIMIZE TABLE test_with_overflow FINAL;

SELECT 'with_overflow', * FROM test_with_overflow ORDER BY id;

DROP TABLE test_with_overflow;

-- Test 7: Multiple sorting keys with subcolumn aggregation
DROP TABLE IF EXISTS test_multi_key;

CREATE TABLE test_multi_key (
    key1 UInt32,
    key2 String,
    metrics Tuple(
        counter SimpleAggregateFunction(sum, UInt64),
        gauge SimpleAggregateFunction(anyLast, Float64),
        status SimpleAggregateFunction(groupBitOr, UInt32)
    )
) ENGINE = AggregatingMergeTree() ORDER BY (key1, key2);

INSERT INTO test_multi_key VALUES
    (1, 'a', tuple(100, 1.5, 1)),
    (1, 'a', tuple(200, 2.5, 2)),
    (1, 'b', tuple(50, 3.5, 4)),
    (2, 'a', tuple(75, 4.5, 8)),
    (2, 'a', tuple(25, 5.5, 16));

SELECT 'Multi-key test:';
SELECT
    key1,
    key2,
    metrics.counter AS total_count,
    metrics.gauge AS last_gauge,
    metrics.status AS combined_status
FROM test_multi_key FINAL
ORDER BY key1, key2;

DROP TABLE test_multi_key;

-- Test 8: Nullable type subcolumn aggregation
DROP TABLE IF EXISTS test_nullable_subcolumn;

CREATE TABLE test_nullable_subcolumn (
    id UInt64,
    nullable_agg Tuple(
        nullable_sum SimpleAggregateFunction(sum, Nullable(UInt64)),
        nullable_max SimpleAggregateFunction(max, Nullable(Int64)),
        nullable_min SimpleAggregateFunction(min, Nullable(Int64)),
        nullable_any SimpleAggregateFunction(any, Nullable(String)),
        nullable_any_last SimpleAggregateFunction(anyLast, Nullable(String)),
        nullable_any_last_respect_nulls SimpleAggregateFunction(anyLastRespectNulls, Nullable(String)),
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- Test 1: Insert with non-NULL values
INSERT INTO test_nullable_subcolumn VALUES(
    1,
    tuple(100, 50, 10, 'first', 'first_last', 'first_respect')
);

-- Test 2: Insert with some NULL values (should aggregate with previous)
INSERT INTO test_nullable_subcolumn VALUES(
    1,
    tuple(200, null, 5, null, 'second_last', null)
);

-- Test 3: Insert with more NULL values
INSERT INTO test_nullable_subcolumn VALUES(
    1,
    tuple(null, 75, null, 'third', null, 'third_respect')
);

-- Test 4: Insert data for id=2 with mixed NULL/non-NULL
INSERT INTO test_nullable_subcolumn VALUES(
    2,
    tuple(500, 100, 50, 'alpha', 'alpha_last', null)
);

INSERT INTO test_nullable_subcolumn VALUES(
    2,
    tuple(null, null, null, null, null, 'beta_respect')
);

-- Test 5: Insert data for id=3 with all NULLs
INSERT INTO test_nullable_subcolumn VALUES(
    3,
    tuple(null, null, null, null, null, null)
);

INSERT INTO test_nullable_subcolumn VALUES(
    3,
    tuple(null, null, null, null, null, null)
);

-- Type check
SELECT 'Nullable subcolumn test - type check:';
SELECT
    toTypeName(nullable_agg.nullable_sum),
    toTypeName(nullable_agg.nullable_max),
    toTypeName(nullable_agg.nullable_any_last),
    toTypeName(nullable_agg.nullable_any_last_respect_nulls)
FROM test_nullable_subcolumn LIMIT 1;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_nullable_subcolumn FINAL;

SELECT 'Nullable subcolumn test - after OPTIMIZE:';
SELECT * FROM test_nullable_subcolumn ORDER BY id;

-- Verify FINAL produces same result as after OPTIMIZE
SELECT 'Nullable subcolumn test - with FINAL:';
SELECT * FROM test_nullable_subcolumn FINAL ORDER BY id;

DROP TABLE test_nullable_subcolumn;

-- Test 9: Nullable with AggregateFunction subcolumns
DROP TABLE IF EXISTS test_nullable_agg_function;

CREATE TABLE test_nullable_agg_function (
    id UInt64,
    nullable_states Tuple(
        uniq_nullable AggregateFunction(uniq, Nullable(String)),
        sum_nullable AggregateFunction(sum, Nullable(UInt64)),
        avg_nullable AggregateFunction(avg, Nullable(Float64)),
        min_nullable AggregateFunction(min, Nullable(Int64)),
        max_nullable AggregateFunction(max, Nullable(Int64)),
        count_nullable AggregateFunction(count, Nullable(UInt64))
    )
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- Insert first batch with mixed NULL/non-NULL values
INSERT INTO test_nullable_agg_function
SELECT
    1 AS id,
    tuple(
        uniqState(if(number % 3 = 0, null, concat('val_', toString(number)))),
        sumState(if(number % 2 = 0, null, toUInt64(number * 10))),
        avgState(if(number % 4 = 0, null, toFloat64(number))),
        minState(if(number % 5 = 0, null, toInt64(number))),
        maxState(if(number % 6 = 0, null, toInt64(number))),
        countState(if(number % 7 = 0, null, toUInt64(1)))
    ) AS nullable_states
FROM numbers(20);

-- Insert second batch with different NULL patterns
INSERT INTO test_nullable_agg_function
SELECT
    1 AS id,
    tuple(
        uniqState(if(number % 2 = 0, null, concat('new_', toString(number)))),
        sumState(if(number % 3 = 0, null, toUInt64(number * 20))),
        avgState(if(number % 5 = 0, null, toFloat64(number + 10))),
        minState(if(number % 4 = 0, null, toInt64(number - 5))),
        maxState(if(number % 3 = 0, null, toInt64(number + 5))),
        countState(if(number % 8 = 0, null, toUInt64(1)))
    ) AS nullable_states
FROM numbers(20);

-- Insert data for id=2
INSERT INTO test_nullable_agg_function
SELECT
    2 AS id,
    tuple(
        uniqState(if(number < 5, concat('id2_', toString(number)), null)),
        sumState(if(number < 3, toUInt64(number * 100), null)),
        avgState(if(number < 4, toFloat64(number * 2), null)),
        minState(if(number < 6, toInt64(number), null)),
        maxState(if(number < 7, toInt64(number * 3), null)),
        countState(toUInt64(1))
    ) AS nullable_states
FROM numbers(10);

-- Test nullable AggregateFunction state merging before OPTIMIZE
SELECT 'Nullable AggregateFunction test - before OPTIMIZE:';
SELECT
    id,
    uniqMerge(nullable_states.uniq_nullable) AS unique_count,
    sumMerge(nullable_states.sum_nullable) AS total_sum,
    avgMerge(nullable_states.avg_nullable) AS avg_val,
    minMerge(nullable_states.min_nullable) AS min_val,
    maxMerge(nullable_states.max_nullable) AS max_val,
    countMerge(nullable_states.count_nullable) AS count_val
FROM test_nullable_agg_function
GROUP BY id
ORDER BY id;

-- Test after OPTIMIZE
OPTIMIZE TABLE test_nullable_agg_function FINAL;

SELECT 'Nullable AggregateFunction test - after OPTIMIZE:';
SELECT
    id,
    uniqMerge(nullable_states.uniq_nullable) AS unique_count,
    sumMerge(nullable_states.sum_nullable) AS total_sum,
    avgMerge(nullable_states.avg_nullable) AS avg_val,
    minMerge(nullable_states.min_nullable) AS min_val,
    maxMerge(nullable_states.max_nullable) AS max_val,
    countMerge(nullable_states.count_nullable) AS count_val
FROM test_nullable_agg_function
GROUP BY id
ORDER BY id;

DROP TABLE test_nullable_agg_function;

-- Test 10: Direct subcolumns with various data types (not nested in tuples)
DROP TABLE IF EXISTS test_direct_subcolumns;

CREATE TABLE test_direct_subcolumns (
    id UInt64,
    -- 1. Nullable types directly as subcolumns (not in tuple)
    nullable_sum SimpleAggregateFunction(sum, Nullable(UInt64)),
    nullable_avg SimpleAggregateFunction(avg, Nullable(Float64)),
    nullable_any SimpleAggregateFunction(anyLast, Nullable(String)),
    nullable_bit_or SimpleAggregateFunction(groupBitOr, Nullable(UInt32)),
    
    -- 2. LowCardinality types
    low_card_str SimpleAggregateFunction(anyLast, LowCardinality(String)),
    low_card_nullable SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String))),
    
    -- 3. DateTime types
    date_val SimpleAggregateFunction(min, Date),
    datetime_val SimpleAggregateFunction(max, DateTime),
    datetime64_val SimpleAggregateFunction(anyLast, DateTime64(3)),
    
    -- 4. Decimal types
    decimal32_val SimpleAggregateFunction(sum, Decimal32(4)),
    decimal64_val SimpleAggregateFunction(sum, Decimal64(8)),
    
    -- 5. Enum types
    enum_val SimpleAggregateFunction(anyLast, Enum8('red' = 1, 'green' = 2, 'blue' = 3)),
    
    -- 6. FixedString
    fixed_str SimpleAggregateFunction(anyLast, FixedString(16)),
    
    -- 7. IPv6 type
    ipv6_val SimpleAggregateFunction(anyLast, IPv6),
    
    -- 8. Simple types without wrapper
    direct_sum SimpleAggregateFunction(sum, UInt64),
    direct_avg SimpleAggregateFunction(avg, Float64),
    direct_any SimpleAggregateFunction(anyLast, String)
) ENGINE = AggregatingMergeTree() ORDER BY id;

-- Insert first batch
INSERT INTO test_direct_subcolumns VALUES (
    1,
    -- Nullable values
    100, 10.5, 'first', 1,
    
    -- LowCardinality values
    'low1', 'low_null1',
    
    -- DateTime values
    '2023-01-01', '2023-01-01 10:00:00', '2023-01-01 10:00:00.123',
    
    -- Decimal values
    12.3456, 12345.6789,
    
    -- Enum value
    'red',
    
    -- FixedString value
    'fixed_string_01',
    
    -- IPv6 value
    '2001:db8::1',
    
    -- Direct simple values
    1000, 50.5, 'direct_first'
);

-- Insert second batch with same id (should aggregate)
INSERT INTO test_direct_subcolumns VALUES (
    1,
    -- Nullable values (some NULL)
    200, NULL, NULL, 2,
    
    -- LowCardinality values
    'low2', 'low_null2',
    
    -- DateTime values
    '2023-01-02', '2023-01-02 14:30:00', '2023-01-02 14:30:00.456',
    
    -- Decimal values
    23.4567, 23456.7890,
    
    -- Enum value
    'green',
    
    -- FixedString value
    'fixed_string_02',
    
    -- IPv6 value
    '2001:db8::2',
    
    -- Direct simple values
    2000, 75.5, 'direct_second'
);

-- Insert data for id=2
INSERT INTO test_direct_subcolumns VALUES (
    2,
    -- Nullable values
    500, 25.5, 'second_first', 4,
    
    -- LowCardinality values
    'low3', 'low_null3',
    
    -- DateTime values
    '2023-02-01', '2023-02-01 08:00:00', '2023-02-01 08:00:00.789',
    
    -- Decimal values
    34.5678, 34567.8901,
    
    -- Enum value
    'blue',
    
    -- FixedString value
    'fixed_string_03',
    
    -- IPv6 value
    '2001:db8::3',
    
    -- Direct simple values
    3000, 100.5, 'second_direct'
);

-- Type check for direct subcolumns
SELECT 'Direct subcolumns test - type check:';
SELECT 
    toTypeName(nullable_sum),
    toTypeName(nullable_any),
    toTypeName(low_card_str),
    toTypeName(date_val),
    toTypeName(decimal32_val),
    toTypeName(enum_val),
    toTypeName(fixed_str),
    toTypeName(ipv6_val),
    toTypeName(direct_sum)
FROM test_direct_subcolumns LIMIT 1;

-- Test immediate aggregation with FINAL
SELECT 'Direct subcolumns test - with FINAL:';
SELECT * FROM test_direct_subcolumns FINAL ORDER BY id;

-- Test on-disk aggregation with OPTIMIZE
OPTIMIZE TABLE test_direct_subcolumns FINAL;

SELECT 'Direct subcolumns test - after OPTIMIZE:';
SELECT * FROM test_direct_subcolumns ORDER BY id;

DROP TABLE test_direct_subcolumns;