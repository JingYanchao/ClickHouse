DROP TABLE IF EXISTS minmax_idx_upsert;
CREATE TABLE minmax_idx_upsert
(
    u64 UInt64,
    i64 Int64,
    i32 Int32,
    PROJECTION __unique_index INDEX u64 TYPE unique
)
ENGINE = UniqueMergeTree()
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';
INSERT INTO minmax_idx_upsert VALUES (0, 2, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 2, 2), (5, 2, 2), (6, 2, 2), (7, 2, 2), (8, 1, 2), (9, 1, 2);
ALTER TABLE minmax_idx_upsert ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1 SETTINGS mutations_sync = 2;
ALTER TABLE minmax_idx_upsert MATERIALIZE INDEX idx IN PARTITION 1 SETTINGS mutations_sync = 2;
ALTER TABLE minmax_idx_upsert MATERIALIZE INDEX idx IN PARTITION 2 SETTINGS mutations_sync = 2;
ALTER TABLE minmax_idx_upsert CLEAR INDEX idx IN PARTITION 1 SETTINGS mutations_sync = 2;
ALTER TABLE minmax_idx_upsert CLEAR INDEX idx IN PARTITION 2 SETTINGS mutations_sync = 2;
ALTER TABLE minmax_idx_upsert MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
select u64 from minmax_idx_upsert order by u64;
DROP TABLE minmax_idx_upsert;