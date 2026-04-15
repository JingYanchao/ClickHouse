-- Test: UniqueMergeTree SELECT correctness and edge cases
--
-- Covers query correctness (aggregations, PREWHERE, subqueries, LIMIT/OFFSET,
-- GROUP BY) and edge cases (empty inserts, single-row parts, large batches,
-- string keys, composite keys).

-- ===================================================================
-- Aggregation correctness with delete marks
-- ===================================================================
SELECT '--- select: aggregations ---';

DROP TABLE IF EXISTS umt_select_agg;

CREATE TABLE umt_select_agg
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_select_agg SELECT number, number FROM numbers(100);
INSERT INTO umt_select_agg SELECT number, number + 1000 FROM numbers(50);

SELECT count() FROM umt_select_agg;
SELECT sum(value) FROM umt_select_agg;
SELECT min(value), max(value) FROM umt_select_agg;
SELECT avg(value) FROM umt_select_agg;

DROP TABLE umt_select_agg;

-- ===================================================================
-- PREWHERE with delete marks
-- ===================================================================
SELECT '--- select: prewhere ---';

DROP TABLE IF EXISTS umt_select_prewhere;

CREATE TABLE umt_select_prewhere
(
    id UInt32,
    value UInt32,
    tag String,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_select_prewhere SELECT number, number, 'old' FROM numbers(20);
INSERT INTO umt_select_prewhere SELECT number, number + 100, 'new' FROM numbers(10);

SELECT count() FROM umt_select_prewhere WHERE tag = 'new';
SELECT count() FROM umt_select_prewhere WHERE tag = 'old';
SELECT count() FROM umt_select_prewhere PREWHERE value >= 100;

DROP TABLE umt_select_prewhere;

-- ===================================================================
-- Subquery and IN
-- ===================================================================
SELECT '--- select: subquery ---';

DROP TABLE IF EXISTS umt_select_subquery;

CREATE TABLE umt_select_subquery
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_select_subquery SELECT number, number FROM numbers(10);
INSERT INTO umt_select_subquery SELECT number, number + 100 FROM numbers(5);

SELECT count() FROM umt_select_subquery WHERE id IN (SELECT id FROM umt_select_subquery WHERE value >= 100);
SELECT * FROM umt_select_subquery WHERE id IN (SELECT number FROM numbers(3)) ORDER BY id;

DROP TABLE umt_select_subquery;

-- ===================================================================
-- LIMIT and OFFSET with delete marks
-- ===================================================================
SELECT '--- select: limit offset ---';

DROP TABLE IF EXISTS umt_select_limit;

CREATE TABLE umt_select_limit
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_select_limit SELECT number, number FROM numbers(20);
INSERT INTO umt_select_limit SELECT number, number + 100 FROM numbers(10);

SELECT * FROM umt_select_limit ORDER BY id LIMIT 5;
SELECT * FROM umt_select_limit ORDER BY id LIMIT 5 OFFSET 8;

DROP TABLE umt_select_limit;

-- ===================================================================
-- GROUP BY with delete marks
-- ===================================================================
SELECT '--- select: group by ---';

DROP TABLE IF EXISTS umt_select_groupby;

CREATE TABLE umt_select_groupby
(
    id UInt32,
    category UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_select_groupby SELECT number, number % 3, number FROM numbers(15);
INSERT INTO umt_select_groupby SELECT number, number % 3, number + 100 FROM numbers(6);

SELECT category, count(), sum(value) FROM umt_select_groupby GROUP BY category ORDER BY category;

DROP TABLE umt_select_groupby;

-- ===================================================================
-- Edge case: empty insert
-- ===================================================================
SELECT '--- edge: empty insert ---';

DROP TABLE IF EXISTS umt_edge_empty;

CREATE TABLE umt_edge_empty
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_edge_empty SELECT number, number FROM numbers(5);
INSERT INTO umt_edge_empty SELECT number, number FROM numbers(0);

SELECT count() FROM umt_edge_empty;
SELECT * FROM umt_edge_empty ORDER BY id;

DROP TABLE umt_edge_empty;

-- ===================================================================
-- Edge case: single-row parts
-- ===================================================================
SELECT '--- edge: single row parts ---';

DROP TABLE IF EXISTS umt_edge_single;

CREATE TABLE umt_edge_single
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_edge_single VALUES (1, 10);
INSERT INTO umt_edge_single VALUES (2, 20);
INSERT INTO umt_edge_single VALUES (1, 100);
INSERT INTO umt_edge_single VALUES (3, 30);
INSERT INTO umt_edge_single VALUES (2, 200);

SELECT count() FROM umt_edge_single;
SELECT * FROM umt_edge_single ORDER BY id;

OPTIMIZE TABLE umt_edge_single FINAL;
SELECT count() FROM umt_edge_single;
SELECT * FROM umt_edge_single ORDER BY id;

DROP TABLE umt_edge_single;

-- ===================================================================
-- Edge case: large batch for parallel dedup
-- ===================================================================
SELECT '--- edge: large batch ---';

DROP TABLE IF EXISTS umt_edge_large;

CREATE TABLE umt_edge_large
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY id;

INSERT INTO umt_edge_large SELECT number, number FROM numbers(10000);
INSERT INTO umt_edge_large SELECT number, number + 100000 FROM numbers(5000);

SELECT count() FROM umt_edge_large;
SELECT min(id), max(id) FROM umt_edge_large;
SELECT value FROM umt_edge_large WHERE id = 0;
SELECT value FROM umt_edge_large WHERE id = 4999;
SELECT value FROM umt_edge_large WHERE id = 5000;
SELECT value FROM umt_edge_large WHERE id = 9999;

DROP TABLE umt_edge_large;

-- ===================================================================
-- Edge case: String unique key
-- ===================================================================
SELECT '--- edge: string key ---';

DROP TABLE IF EXISTS umt_edge_string;

CREATE TABLE umt_edge_string
(
    key String,
    value UInt32,
    PROJECTION __unique_index INDEX key TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY key;

INSERT INTO umt_edge_string VALUES ('aaa', 1), ('bbb', 2), ('ccc', 3);
INSERT INTO umt_edge_string VALUES ('aaa', 10), ('ddd', 4);

SELECT count() FROM umt_edge_string;
SELECT * FROM umt_edge_string ORDER BY key;

OPTIMIZE TABLE umt_edge_string FINAL;
SELECT * FROM umt_edge_string ORDER BY key;

DROP TABLE umt_edge_string;

-- ===================================================================
-- Edge case: composite unique key (multiple columns)
-- ===================================================================
SELECT '--- edge: composite key ---';

DROP TABLE IF EXISTS umt_edge_composite;

CREATE TABLE umt_edge_composite
(
    a UInt32,
    b UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX a, b TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY (a, b);

INSERT INTO umt_edge_composite VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30);
INSERT INTO umt_edge_composite VALUES (1, 1, 100), (2, 2, 40);

SELECT count() FROM umt_edge_composite;
SELECT * FROM umt_edge_composite ORDER BY a, b;

DROP TABLE umt_edge_composite;
