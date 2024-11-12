-- clickhouse-client --user root --password --queries-file xxx.sql
-- DROP TABLE IF EXISTS database.table;
SET
    allow_experimental_object_type = 1;

CREATE TABLE
    IF NOT EXISTS database.table (
        `_id` String,
        `doc` JSON,
        `__date` Date MATERIALIZED toDate (
            reinterpretAsInt64 (reverse (unhex (substring(_id, 1, 8))))
        ),
        `__ver` UInt64 DEFAULT 0, -- version, derived from oplog timestamp or _id timestamp
        `__is_deleted` UInt8 DEFAULT 0, -- 0:未删除 1:已删除 默认值为 0
        `__ns` String, -- namespace
        `__op_time` UInt64 DEFAULT 0, -- for tracing oplog
        `__sync_time` UInt64 DEFAULT 0 -- for tracing
    ) ENGINE = ReplacingMergeTree (`__ver`, `__is_deleted`)
PARTITION BY
    `__date`
ORDER BY
    `_id`;