-- ==========================================================
-- Create-time validation for UniqueMergeTree + unique projection.
-- ==========================================================

-- Test 1: UniqueMergeTree without any projection must fail.
DROP TABLE IF EXISTS test_no_proj;
CREATE TABLE test_no_proj (x Int32, y UInt64) ENGINE = UniqueMergeTree() ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Test 2: UniqueMergeTree with a non-unique projection must fail.
DROP TABLE IF EXISTS test_wrong_proj;
CREATE TABLE test_wrong_proj
(
    x Int32,
    y UInt64,
    PROJECTION __unique_index (SELECT count() GROUP BY x)
)
ENGINE = UniqueMergeTree() ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Test 3: UniqueMergeTree projection name parameter must be a string literal.
DROP TABLE IF EXISTS test_version_param;
CREATE TABLE test_version_param
(
    x Int32,
    y UInt64,
    PROJECTION __unique_index INDEX x TYPE unique
)
ENGINE = UniqueMergeTree(123) ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Test 4: Unique projection keys may be arbitrary expressions, not just simple identifiers.
-- The key is a deterministic expression over existing physical columns; the projection
-- is built successfully and dedup happens on the expression value.
DROP TABLE IF EXISTS test_expr_key;
CREATE TABLE test_expr_key
(
    x Int32,
    y UInt64,
    PROJECTION __unique_index INDEX x + y TYPE unique
)
ENGINE = UniqueMergeTree() ORDER BY x;
SELECT engine FROM system.tables WHERE name = 'test_expr_key' AND database = currentDatabase();
DROP TABLE test_expr_key;

-- Test 5: Successful creation with a single-column unique projection.
DROP TABLE IF EXISTS test_ok_single;
CREATE TABLE test_ok_single
(
    x Int32,
    y UInt64,
    PROJECTION __unique_index INDEX x TYPE unique
)
ENGINE = UniqueMergeTree() ORDER BY x;
SELECT engine FROM system.tables WHERE name = 'test_ok_single' AND database = currentDatabase();
DROP TABLE test_ok_single;

-- Test 6: Successful creation with a multi-column unique projection.
DROP TABLE IF EXISTS test_ok_multi;
CREATE TABLE test_ok_multi
(
    x Int32,
    y UInt64,
    z String,
    PROJECTION __unique_index INDEX x, y TYPE unique
)
ENGINE = UniqueMergeTree() ORDER BY x;
SELECT engine FROM system.tables WHERE name = 'test_ok_multi' AND database = currentDatabase();
DROP TABLE test_ok_multi;

-- Test 7: Successful creation with a version column in TYPE unique('ver').
DROP TABLE IF EXISTS test_ok_version;
CREATE TABLE test_ok_version
(
    x Int32,
    y UInt64,
    ver UInt64,
    PROJECTION __unique_index INDEX x TYPE unique('ver')
)
ENGINE = UniqueMergeTree() ORDER BY x;
SELECT engine FROM system.tables WHERE name = 'test_ok_version' AND database = currentDatabase();
DROP TABLE test_ok_version;

-- Test 8: version column referenced by TYPE unique(<name>) must be UInt64 at CREATE time.
-- The check now lives in ProjectionIndexUnique::fillProjectionDescription and runs eagerly,
-- so creating a table with a non-UInt64 version column must be rejected up front.
DROP TABLE IF EXISTS test_bad_version_type;
CREATE TABLE test_bad_version_type
(
    x Int32,
    ver String,
    PROJECTION __unique_index INDEX x TYPE unique('ver')
)
ENGINE = UniqueMergeTree() ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Test 9: version column referenced by TYPE unique(<name>) must exist at CREATE time.
DROP TABLE IF EXISTS test_missing_version;
CREATE TABLE test_missing_version
(
    x Int32,
    PROJECTION __unique_index INDEX x TYPE unique('ver')
)
ENGINE = UniqueMergeTree() ORDER BY x; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- ==========================================================
-- ALTER-time validation on a live UniqueMergeTree table.
-- Columns referenced by the unique projection (keys + optional version)
-- must not be dropped, renamed, or have their types changed.
--
-- To exercise the projection-level check specifically, we intentionally
-- put the unique key and the version column OUTSIDE the ORDER BY so they
-- are not protected by the general "sorting key column" rule first.
-- ==========================================================

DROP TABLE IF EXISTS test_alter;
CREATE TABLE test_alter
(
    id Int32,
    k UInt32,
    ver UInt64,
    v String,
    PROJECTION __unique_index INDEX k TYPE unique('ver')
)
ENGINE = UniqueMergeTree() ORDER BY id;

-- Sanity check: the table is indeed a UniqueMergeTree.
SELECT engine FROM system.tables WHERE name = 'test_alter' AND database = currentDatabase();

-- DROP COLUMN on a unique-key column is rejected when the projection is rebuilt
-- (getPhysical throws NO_SUCH_COLUMN_IN_TABLE, wrapped as "Cannot apply ALTER because it breaks projection").
ALTER TABLE test_alter DROP COLUMN k; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- DROP COLUMN on the version column is rejected for the same reason.
ALTER TABLE test_alter DROP COLUMN ver; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- RENAME COLUMN on a unique-key column is rejected because
-- `fillProjectionDescription` calls `getPhysical` on the old name,
-- which no longer exists after the rename is applied.
ALTER TABLE test_alter RENAME COLUMN k TO k2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- RENAME COLUMN on the version column is likewise rejected.
ALTER TABLE test_alter RENAME COLUMN ver TO ver2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- MODIFY COLUMN that changes the type of a unique-key column is rejected:
-- sample_block_for_keys now carries the real user-column types, so
-- blocksHaveEqualStructure returns false and ALTER_OF_COLUMN_IS_FORBIDDEN is thrown.
ALTER TABLE test_alter MODIFY COLUMN k UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- MODIFY COLUMN that changes the version column away from UInt64 is rejected:
-- first by the explicit type check in fillProjectionDescription (BAD_ARGUMENTS).
ALTER TABLE test_alter MODIFY COLUMN ver String; -- { serverError BAD_ARGUMENTS }

-- A benign MODIFY on a column that is NOT part of the unique projection must still succeed.
ALTER TABLE test_alter MODIFY COLUMN v FixedString(8);
SELECT type FROM system.columns WHERE table = 'test_alter' AND database = currentDatabase() AND name = 'v';

-- Adding an unrelated column must still succeed.
ALTER TABLE test_alter ADD COLUMN extra String DEFAULT '';
SELECT name FROM system.columns WHERE table = 'test_alter' AND database = currentDatabase() AND name = 'extra';

-- And dropping that unrelated column must still succeed.
ALTER TABLE test_alter DROP COLUMN extra;

DROP TABLE test_alter;
