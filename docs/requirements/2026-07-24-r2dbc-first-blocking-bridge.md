# R2DBC-first 与传统 JDBC 风格桥接决策

> 历史决策，已被 V2.0.0 的原生 JDBC/R2DBC 双执行内核替代。本文只保留设计演进记录，
> `SyncSqlExecutor.bridge(...)` 和 `R2dbcSyncSqlExecutor` 已删除，不能作为当前接入说明。

## 状态

历史阶段曾采用，V2.0.0 起不再执行。

## 背景

flying-orm 是一个为动态表单而生的简单 ORM：轻 SQL、重 Java、真响应式、前端可透传结构化条件且无 SQL 注入。

“支持同步调用”指的是提供普通 Java 同步方法，不是再做一套 `DataSource + java.sql.Connection` 执行内核。

## 决策

flying-orm 是 R2DBC-first / Reactor-first。

- 数据库执行、连接生命周期、事务、批量分片、UNKNOWN 恢复和结果模型都以 R2DBC 内核为准。
- 同步支持是阻塞桥接层：对外提供同步方法，内部调用 R2DBC/Reactor 链路，并在最外层阻塞等待结果。
- 不再规划 JDBC/R2DBC 双内核，也不维护另一套基于 `DataSource` 的正式执行入口。
- 正式同步入口只有 `SyncSqlExecutor.bridge(...)` 和 `SyncFormClient`，桥接实现类留在包内。

## API 命名方向

后续文档和代码应优先使用这些名字：

- 响应式主线：`ReactiveSqlExecutor`、`R2dbcSqlExecutor`、`ReactiveFormClient`、`ReactiveSchemaClient`。
- 同步桥接：`SyncSqlExecutor.bridge(...)`、`SyncFormClient`。

## 事务策略

- 普通响应式 CRUD：默认轻量执行，可由外层 R2DBC 事务或上层应用框架事务包住。
- 批量动态表单写入：ORM 内建事务语义，默认 `ATOMIC`，显式 `INDEPENDENT`。
- 同步桥接模式：复用同一套 R2DBC 事务语义和批量结果，不创建 `DataSourceTransactionManager` 风格的第二套事务模型。
- DDL 动态表结构维护：不同数据库对 DDL 事务支持不一致，不能跨库承诺所有建表/改表都原子；需要在后续 schema 设计里单独说明。

## 实施影响

1. 文档统一使用“同步 R2DBC 桥接”，避免把同步编程模型误解为 JDBC 内核。
2. 批量计划只实现 R2DBC/Reactor 执行链，同步调用在最外层等待同一份结果。
3. flying-orm 本体不提供应用框架自动配置；上层服务或独立适配项目可以基于 `ConnectionFactory` 组装响应式和同步桥接客户端。
4. 后续代码调整时，把纯 JDBC 验证路径从正式架构里拿掉，避免形成两套行为。
