
我们需要的是：数据是一个 JSON 结构，ClickHouse 可以自动映射到对应的 table column。

### clickhouse-go 不适用
我们假设事先并不知道 MongoDB 中 collection 的数据结构。因此 clickhouse-go 这个库不能用。

batch.Append 值与表结构字段需要对齐。不合适。
https://clickhouse.com/docs/en/integrations/go#batch-insert

batch.AppendStruct 要求代码中有对应的数据结构。不合适。
https://clickhouse.com/docs/en/integrations/go#append-struct

https://github.com/ClickHouse/clickhouse-go/issues/182
FORMAT is not supported by the driver. The driver sends binary blocks (columns) to the ClickHouse server and doesn't parse FORMAT section.

### 动态数据结构使用 JsonEachRow

vector/sink/clickhouse 中使用 JsonEachRow, JSONAsObject Format 插入动态数据结构。

JSONAsObject，整个 row 作为 JSON 字段，这样连顶级的字段都可以随意变动了。
JsonEachRow，一行一个 JSON 数据，解析到字段，现在还是得用这个，假设顶级的字段是不变的（不然就得 ALTER TABLE）。

To add a primary key, and still exploit the JSON object capabilities, we recommended using a dedicated subkey for the JSON. This requires inserting the data using the JSONEachRow format instead of JSONAsObject.
https://clickhouse.com/docs/en/integrations/data-formats/json#adding-primary-keys