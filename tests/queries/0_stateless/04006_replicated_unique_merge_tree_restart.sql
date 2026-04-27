-- Tags: zookeeper

-- Test: ReplicatedUniqueMergeTree restart dedup (buildAllDeleteMarksOnStartup)
-- After DETACH + ATTACH, in-memory delete marks are lost and must be rebuilt.
-- Verify that dedup results are identical before and after restart.

SELECT '--- restart dedup: basic ---';

DROP TABLE IF EXISTS replicated_unique_restart_basic;

CREATE TABLE replicated_unique_restart_basic
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_restart_basic', '1')
ORDER BY id;

-- Insert 10 rows, then upsert 5 of them with new values.
INSERT INTO replicated_unique_restart_basic SELECT number, number FROM numbers(10);
INSERT INTO replicated_unique_restart_basic SELECT number, number + 100 FROM numbers(5);

-- Verify before restart.
SELECT count() FROM replicated_unique_restart_basic;
SELECT sum(value) FROM replicated_unique_restart_basic;

-- DETACH + ATTACH to trigger buildAllDeleteMarksOnStartup.
DETACH TABLE replicated_unique_restart_basic;
ATTACH TABLE replicated_unique_restart_basic;

-- Verify after restart: results must be identical.
SELECT count() FROM replicated_unique_restart_basic;
SELECT sum(value) FROM replicated_unique_restart_basic;

DROP TABLE replicated_unique_restart_basic;

SELECT '--- restart dedup: multi-partition ---';

DROP TABLE IF EXISTS replicated_unique_restart_partition;

CREATE TABLE replicated_unique_restart_partition
(
    dt Date,
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_restart_partition', '1')
PARTITION BY dt
ORDER BY id;

-- Insert into two partitions, then upsert overlapping keys.
INSERT INTO replicated_unique_restart_partition VALUES ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
INSERT INTO replicated_unique_restart_partition VALUES ('2024-01-02', 1, 30), ('2024-01-02', 2, 40);
INSERT INTO replicated_unique_restart_partition VALUES ('2024-01-01', 1, 100);
INSERT INTO replicated_unique_restart_partition VALUES ('2024-01-02', 2, 200);

SELECT * FROM replicated_unique_restart_partition ORDER BY dt, id;

DETACH TABLE replicated_unique_restart_partition;
ATTACH TABLE replicated_unique_restart_partition;

SELECT * FROM replicated_unique_restart_partition ORDER BY dt, id;

DROP TABLE replicated_unique_restart_partition;

SELECT '--- restart dedup: two replicas ---';

DROP TABLE IF EXISTS replicated_unique_restart_replica_r1;
DROP TABLE IF EXISTS replicated_unique_restart_replica_r2;

CREATE TABLE replicated_unique_restart_replica_r1
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_restart_replica', '1')
ORDER BY id;

CREATE TABLE replicated_unique_restart_replica_r2
(
    id UInt32,
    value UInt32,
    PROJECTION __unique_index INDEX id TYPE unique
)
ENGINE = ReplicatedUniqueMergeTree('/clickhouse/tables/{database}/test_04006/replicated_unique_restart_replica', '2')
ORDER BY id;

-- Insert and upsert on replica 1
INSERT INTO replicated_unique_restart_replica_r1 SELECT number, number FROM numbers(10);
INSERT INTO replicated_unique_restart_replica_r1 SELECT number, number + 100 FROM numbers(5);
SYSTEM SYNC REPLICA replicated_unique_restart_replica_r2;

-- Verify both replicas before restart
SELECT count() FROM replicated_unique_restart_replica_r1;
SELECT count() FROM replicated_unique_restart_replica_r2;

-- Restart replica 2 (DETACH + ATTACH)
DETACH TABLE replicated_unique_restart_replica_r2;
ATTACH TABLE replicated_unique_restart_replica_r2;

-- Replica 2 should rebuild delete marks and show correct results
SELECT '--- replica 2 after restart ---';
SELECT * FROM replicated_unique_restart_replica_r2 ORDER BY id;

DROP TABLE replicated_unique_restart_replica_r1;
DROP TABLE replicated_unique_restart_replica_r2;
