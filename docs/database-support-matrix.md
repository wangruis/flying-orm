# 数据库支持矩阵

本文区分三种状态，避免把“代码里有方言”误写成“生产环境已经认证”。

- **内置**：已有方言、DDL/DML、分页、参数标记和元数据读取实现。
- **自动测试**：项目内或可选外部测试入口覆盖关键行为。
- **实库认证**：在明确的数据库和驱动版本上执行完整清单并归档结果。没有归档就不能写已认证。

| 数据库 | 内置能力 | V2 当前验证 | V2 发布要求 |
| --- | --- | --- | --- |
| H2 | 动态表单、DDL、CRUD、分页、批量、JSON、LOB、元数据 | 原生 JDBC 与 R2DBC 契约持续通过，当前 `core` 124 项、`rdb` 737 项 | 作为开发契约基线持续通过 |
| MySQL | 动态表单、DDL、CRUD、分页、upsert、JSON、LOB、元数据、异常分类 | MySQL 8.4.10 已完成 JDBC/R2DBC 功能、事务、故障、连接池恢复和三轮性能认证 | 已达到 V2 发布门禁，发版前只重跑最终回归 |
| PostgreSQL | MySQL 同级能力，另含原生 Array、数组条件、pgvector 与 RLS | PostgreSQL 17.8 + pgvector 0.8.1 已完成 JDBC/R2DBC 同级批次、Schema/RLS 池清理和三轮性能认证 | 已达到 V2 发布门禁，发版前只重跑最终回归 |
| Oracle | DDL、CRUD、分页、序列、标识列、批量 merge/upsert、类型映射、元数据、异常分类 | Oracle Free 23.26.0 已完成 JDBC 契约及 15 个 R2DBC 功能、事务、故障、取消和并发目标场景认证，并连续复跑三轮 | 当前能力已完成实库认证；专门吞吐基线不作为 V2 阻断项 |
| SQL Server | DDL、CRUD、分页、序列、标识列、带并发保护的批量 merge/upsert、参数标记、类型映射、元数据、异常分类 | SQL Server 2022 CU22 GDR1 已完成 JDBC 契约及 R2DBC 功能、事务、死锁、锁超时和恢复认证 | 当前能力已实库认证；专门吞吐基线不作为 V2 阻断项 |
| OpenGauss | 不提供 | 不测试 | 当前不规划 |

上表中的“已认证”包含 V2.0.0 当前基线的轻量 JOIN、字段加密、保护搜索、CONTAINS 辅助表和脱敏链路。
这些能力已经通过 H2 契约以及 MySQL 8.4、PostgreSQL、Oracle Free 23、SQL Server 2022 的 JDBC/R2DBC
真实数据库复核，详细批次见真实数据库认证文档。

## Oracle 和 SQL Server 版本边界

版本配置控制的是 flying-orm 会生成哪一类 SQL，不代表对应数据库已经通过真实环境认证。没有显式选择版本时，
Oracle 使用 19c，SQL Server 使用 2022；这两个默认值优先保证 SQL 稳定，不会主动启用较新版本才有的列类型。

| 方言配置 | 当前代码边界 |
| --- | --- |
| `OracleVersion.V12C` | 最低代码边界；支持 `OFFSET/FETCH`、序列、标识列和 `MERGE`。Boolean 用 `NUMBER(1)`，JSON 用 `CLOB`。 |
| `OracleVersion.V19C` | 默认配置，沿用 12c 的保守类型映射。 |
| `OracleVersion.V21C` | JSON 列可映射为原生 `JSON`，Boolean 仍用 `NUMBER(1)`。 |
| `OracleVersion.V23AI` | JSON 使用原生 `JSON`，Boolean 可映射为 SQL `BOOLEAN`。 |
| `SqlServerVersion.V2012` | 最低代码边界；支持 `OFFSET/FETCH`、序列、`IDENTITY` 和 `MERGE`，不声明 JSON 函数能力。 |
| `SqlServerVersion.V2016` | 在 2012 能力上声明 JSON 函数可用。 |
| `SqlServerVersion.V2019` / `V2022` | 当前与 2016 使用同一稳定 SQL 子集；默认版本为 2022。 |

SQL Server 的 JSON 列当前统一使用 `NVARCHAR(max)`，不依赖特定新版本的原生 JSON 列。分页必须显式排序，
避免数据库为了满足语法而使用一个不稳定的隐藏顺序。SQL Server `MERGE` 使用 `WITH (HOLDLOCK)` 缩小并发
upsert 同时判断记录不存在后撞唯一键的窗口，但它不是业务幂等的替代品。

Oracle 旧版本 Boolean 会在参数层明确写成 `1/0`；Oracle 23ai 和 SQL Server `BIT` 绑定 `Boolean`。
驱动返回的 `BigDecimal`、`Timestamp` 等常见值会收口为稳定 Java 类型，但不会强行解析自定义执行器返回的字符串。
Oracle R2DBC 继续使用匿名 `?` 参数，SQL Server 在真正执行前把它们转换成 `@P0`、`@P1`。

## 实库认证清单

每个目标数据库至少记录：数据库版本、JDBC/R2DBC 驱动版本、连接池版本、字符集和执行日期，并分别验证以下项目：

1. 建表、增列、注释、索引、外键和安全迁移计划。
2. 动态 Map 与实体 Repository 的 CRUD、分页、逻辑删除和乐观锁。
3. ATOMIC 与 INDEPENDENT 批量、冲突、回滚、取消和 UNKNOWN 恢复。
4. JSON、时间、Boolean、Enum、LOB；PostgreSQL 额外验证 Array。
5. 元数据反读、DDL 后精确缓存失效、异常分类和连接资源释放。
6. 有界并发下的吞吐、P95/P99、连接占用和慢消费者行为。
7. V2.0.0 验证 INNER/LEFT/RIGHT JOIN 的外连接 Scope 语义，以及加密字段 EXACT/SUFFIX/CONTAINS、脱敏、密钥
   轮换兼容、批量原子回滚和数据库生成主键侧索引 owner。

本机 Docker 环境、固定版本、执行顺序和证据格式见[真实数据库认证方法](real-database-certification.md)，本轮结果见
[V2.0.0 真实数据库与性能认证](database-certification-2026-08-08-v2.0.0.md)。连接信息由环境变量或测试属性传入，
测试报告不得提交密码或完整生产连接串。
