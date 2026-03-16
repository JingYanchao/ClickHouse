drop table if exists test_ipv4;
drop table if exists test_ipv4_upsert;
CREATE TABLE test_ipv4
(
    `x` IPv4,
    `y` Int32
)ENGINE = MergeTree
ORDER BY (x,y);

CREATE TABLE test_ipv4_upsert
(
    `x` IPv4,
    `y` Int32,
    PROJECTION __unique_index INDEX x, y TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY (x,y);
INSERT INTO test_ipv4 SELECT * FROM generateRandom('x IPv4, y Int32', 1, 10, 2) LIMIT 99999;
INSERT INTO test_ipv4_upsert SELECT * FROM test_ipv4;
select count(*) from test_ipv4_upsert;
INSERT INTO test_ipv4_upsert SELECT * FROM test_ipv4;
select count(*) from test_ipv4_upsert;


drop table if exists test_uuid;
drop table if exists test_uuid_upsert;
CREATE TABLE test_uuid
(
    `x` UUID,
    `y` Int32
)ENGINE = MergeTree
ORDER BY (x,y);

CREATE TABLE test_uuid_upsert
(
    `x` UUID,
    `y` Int32,
    PROJECTION __unique_index INDEX x, y TYPE unique
)
ENGINE = UniqueMergeTree()
ORDER BY (x,y);

INSERT INTO test_uuid SELECT * FROM generateRandom('x UUID, y Int32', 1, 10, 2) LIMIT 100002;
INSERT INTO test_uuid_upsert SELECT * FROM test_uuid;
select count(*) from test_uuid_upsert;
INSERT INTO test_uuid_upsert SELECT * FROM test_uuid;
select count(*) from test_uuid_upsert;