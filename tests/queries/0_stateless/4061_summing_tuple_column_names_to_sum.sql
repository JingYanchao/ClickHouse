-- Test: SummingMergeTree with explicit column_names_to_sum and allow_tuple_element_aggregation
-- When a Tuple column is specified in SummingMergeTree(column_name), the flattened
-- leaf columns (e.g. value.a, value.b) should still be aggregated.

DROP TABLE IF EXISTS test_summing_explicit_col;
DROP TABLE IF EXISTS test_summing_explicit_col_nested;
DROP TABLE IF EXISTS test_summing_explicit_col_mixed;

-- Test 1: Basic - specify Tuple column name in SummingMergeTree(value)
SELECT '=== Test 1: Explicit column_names_to_sum with Tuple ===';

CREATE TABLE test_summing_explicit_col (
    key UInt32,
    value Tuple(a UInt64, b UInt64)
) ENGINE = SummingMergeTree(value) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_explicit_col VALUES (1, (10, 20));
INSERT INTO test_summing_explicit_col VALUES (1, (30, 40));

SELECT 'Before OPTIMIZE - with FINAL:';
SELECT key, value FROM test_summing_explicit_col FINAL ORDER BY key;

OPTIMIZE TABLE test_summing_explicit_col FINAL;

SELECT 'After OPTIMIZE:';
SELECT key, value FROM test_summing_explicit_col ORDER BY key;

DROP TABLE test_summing_explicit_col;

-- Test 2: Nested Tuple - specify top-level Tuple column name
SELECT '=== Test 2: Explicit column_names_to_sum with nested Tuple ===';

CREATE TABLE test_summing_explicit_col_nested (
    key UInt32,
    data Tuple(
        x UInt64,
        inner Tuple(
            y UInt64,
            z UInt64
        )
    ),
    other UInt64
) ENGINE = SummingMergeTree(data) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_explicit_col_nested VALUES (1, (10, (20, 30)), 100);
INSERT INTO test_summing_explicit_col_nested VALUES (1, (40, (50, 60)), 200);

SELECT 'Before OPTIMIZE - with FINAL:';
SELECT key, data, other FROM test_summing_explicit_col_nested FINAL ORDER BY key;

OPTIMIZE TABLE test_summing_explicit_col_nested FINAL;

SELECT 'After OPTIMIZE:';
SELECT key, data, other FROM test_summing_explicit_col_nested ORDER BY key;

DROP TABLE test_summing_explicit_col_nested;

-- Test 3: Mixed - specify one Tuple column, leave another unsummed
SELECT '=== Test 3: Explicit column_names_to_sum - only specified Tuple is summed ===';

CREATE TABLE test_summing_explicit_col_mixed (
    key UInt32,
    summed Tuple(a UInt64, b UInt64),
    not_summed Tuple(c UInt64, d UInt64)
) ENGINE = SummingMergeTree(summed) ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO test_summing_explicit_col_mixed VALUES (1, (10, 20), (100, 200));
INSERT INTO test_summing_explicit_col_mixed VALUES (1, (30, 40), (300, 400));

SELECT 'Before OPTIMIZE - with FINAL:';
SELECT key, summed, not_summed FROM test_summing_explicit_col_mixed FINAL ORDER BY key;

OPTIMIZE TABLE test_summing_explicit_col_mixed FINAL;

SELECT 'After OPTIMIZE (not_summed should keep last value):';
SELECT key, summed, not_summed FROM test_summing_explicit_col_mixed ORDER BY key;

DROP TABLE test_summing_explicit_col_mixed;
