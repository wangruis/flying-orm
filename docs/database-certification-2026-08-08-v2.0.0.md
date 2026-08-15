# V2.0.0 真实数据库与性能认证

## 认证结论

2026-08-08 使用 V2 当前开发树重新验证 H2、MySQL、PostgreSQL、Oracle 和 SQL Server。2026-08-09 对 Oracle
慢消费者取消场景完成分阶段定位和修复后，又在同一固定 Docker 环境连续执行三轮四库认证：每轮 65 项，失败 0、
错误 0、跳过 0。MySQL、PostgreSQL、Oracle 和 SQL Server 均完成当前 V2 能力的 JDBC/R2DBC 实库认证。

- 最终源码门禁：`core` 124 项、`rdb` 737 项、testkit 78 项（65 项真实库用例按环境跳过）、
  benchmark 21 项测试通过；JaCoCo、SpotBugs、Checkstyle、Javadoc、源码包和发布产物检查通过。
- 最终源码审查新增的参数数组快照、JDBC 回调回滚，以及迁移、Schema 和模板错误脱敏均已通过 H2/契约门禁。
- MySQL、PostgreSQL、Oracle、SQL Server 的功能、事务、故障和并发批次连续执行三轮，每轮 65 项，0 失败、
  0 错误、0 跳过，三轮合计 195 项。
- MySQL、PostgreSQL、Oracle、SQL Server 各有 1 个原生 JDBC 契约实际执行通过。
- Oracle 的 15 个 R2DBC 目标场景全部通过，包括慢消费者分段 request、取消和连接池归还。
- MySQL、PostgreSQL 的 JDBC 与 R2DBC 各执行三轮固定参数性能认证，共 12 份报告、54 个场景轮次，全部通过。
- 每个性能轮次都没有操作失败或预热失败，结束时连接池借出和等待均为 0。

### 2026-08-11 当前候选复核

在完成生产代码全面审查、批量执行与字段保护边界修复后，使用下列固定 Docker 镜像重新执行四库阻断认证：

- MySQL 8.4.10 与 PostgreSQL 17.10 同批实际执行 37 个目标场景，失败 0、错误 0。
- Oracle Free 23.26.0 实际执行 16 个目标场景，失败 0、错误 0。
- SQL Server 2022 CU22 GDR1 实际执行 16 个目标场景，失败 0、错误 0。
- 四库合计实际执行 69 个目标场景，覆盖 JDBC/R2DBC、DDL、CRUD、JOIN、字段加密与搜索、批量事务、
  UNKNOWN 恢复、连接中断、锁冲突、慢消费者取消和有界并发。
- 全量质量门禁执行 1,298 项测试，失败 0、错误 0；其中 69 项未配置外部数据库的普通构建分支按设计跳过。
  JaCoCo、Checkstyle 和 SpotBugs 均通过。

MySQL 本轮使用容器 TLS 完成 `caching_sha2_password` 认证，不把本地公钥检索开关作为生产安全基线。SQL Server
强制断连场景仍可能由驱动向 stderr 输出 `onErrorDropped(Connection closed)`；测试结果、连接恢复和资源归还均通过，
没有使用全局 Reactor Hook 隐藏该驱动日志。

## 固定环境

| 目标 | 数据库/镜像 | 驱动 |
| --- | --- | --- |
| MySQL | MySQL 8.4.10 | Connector/J 9.7.0、r2dbc-mysql 1.4.1 |
| PostgreSQL | PostgreSQL 17.10、pgvector 0.8.1 | JDBC 42.7.13、r2dbc-postgresql 1.1.1.RELEASE |
| Oracle | Oracle Free 23.26.0 | ojdbc11 23.6.0.24.10、oracle-r2dbc 1.3.0 |
| SQL Server | SQL Server 2022 CU22 GDR1 | mssql-jdbc 10.2.3.jre8、r2dbc-mssql 1.0.4.RELEASE |
| JDBC 连接池 | HikariCP 7.0.2 | 固定 16 个连接 |
| R2DBC 连接池 | r2dbc-pool 1.0.2.RELEASE | 固定 16 个连接 |

连接地址、账号和密码只通过本机环境传入，没有写入报告或源码。

## 功能与故障范围

真实库批次验证了动态表单 DDL、CRUD、命名参数原生 SQL、JSON/LOB、PostgreSQL Array/Vector、元数据读取、
乐观锁、ATOMIC 整批回滚、INDEPENDENT 分片隔离、连接池恢复、取消、死锁和锁超时。R2DBC 批次还验证了
UNKNOWN 回执恢复；JDBC 对未知提交结果返回同一 UNKNOWN 状态，由业务唯一键或上层幂等事实确认。
JDBC 批次同时检查了自动方言识别、同步入口只使用 JDBC，以及所有连接最终关闭。

SQL Server 的强制断连场景结束后，驱动后台偶尔记录 `onErrorDropped(Connection closed)`。受测业务 Publisher
没有收到额外错误，连接池最终归零，Surefire 结果通过。内核没有用全局 Reactor Hook 隐藏这类日志。

旧批次曾把 `acquired=1` 归因于 Oracle 慢消费者取消。后续在清表、建表、批量写入、查询订阅和取消之后分别读取池
指标，确认连接在慢查询开始前已经遗留：认证准备阶段对不存在表执行 `DROP` 得到 ORA-00942，执行会话把所有 SQL
错误都当成不确定连接并走 fail-closed 物理失效；默认失效器无法越过通用 R2DBC SPI 驱逐池包装连接，因此没有执行
正常 close/reset 归还。修复后，只有数据库明确拒绝且会话状态确定的 `BAD_SQL`、约束、重复键、死锁和锁超时走
普通关闭；连接、超时、取消和未知错误继续物理失效。查询取消逻辑、驱动适配和公开 API 均未增加 Oracle 专用分支。

2026-08-08 的失败证据仍作为历史诊断记录保留，但已被 2026-08-09 的分阶段根因定位和三轮 65/65 认证取代，不能
继续用于声明 Oracle 取消限制。

## 性能参数

每个数据库、每种执行内核独立 JVM 运行三轮。每轮预热 5 秒、测量 15 秒，种子数据 10,000 行，连接池 16，
查询/更新并发 16，批量外层并发 4，批量大小 32。R2DBC 的 INDEPENDENT 分片大小为 8、分片并发为 4；
JDBC 同步内核的分片并发为 1。表中使用三轮中位数，不用单轮最好值。

| 内核 | 数据库 | 场景 | 中位吞吐 | 中位行吞吐 | 中位 P99 |
| --- | --- | --- | ---: | ---: | ---: |
| JDBC | MySQL | queryById | 28,607.40 ops/s | 28,607.40 rows/s | 0.920 ms |
| JDBC | MySQL | updateById | 4,559.47 ops/s | 4,559.47 rows/s | 8.692 ms |
| JDBC | MySQL | atomicBatchInsert | 301.73 ops/s | 9,655.47 rows/s | 19.251 ms |
| JDBC | MySQL | independentBatchInsert | 193.27 ops/s | 6,184.53 rows/s | 30.212 ms |
| JDBC | PostgreSQL | queryById | 32,787.40 ops/s | 32,787.40 rows/s | 0.797 ms |
| JDBC | PostgreSQL | updateById | 13,889.60 ops/s | 13,889.60 rows/s | 1.697 ms |
| JDBC | PostgreSQL | atomicBatchInsert | 3,246.27 ops/s | 103,880.53 rows/s | 2.041 ms |
| JDBC | PostgreSQL | independentBatchInsert | 908.13 ops/s | 29,060.27 rows/s | 6.951 ms |
| R2DBC | MySQL | queryById | 14,885.60 ops/s | 14,885.60 rows/s | 1.766 ms |
| R2DBC | MySQL | updateById | 3,970.03 ops/s | 3,970.03 rows/s | 9.306 ms |
| R2DBC | MySQL | transactionalUpdateBatch | 1,767.00 ops/s | 14,136.01 rows/s | 14.557 ms |
| R2DBC | MySQL | atomicBatchInsert | 289.81 ops/s | 9,273.79 rows/s | 20.414 ms |
| R2DBC | MySQL | independentBatchInsert | 456.88 ops/s | 14,620.25 rows/s | 12.526 ms |
| R2DBC | PostgreSQL | queryById | 26,667.14 ops/s | 26,667.14 rows/s | 0.984 ms |
| R2DBC | PostgreSQL | updateById | 12,102.76 ops/s | 12,102.76 rows/s | 1.979 ms |
| R2DBC | PostgreSQL | transactionalUpdateBatch | 2,651.22 ops/s | 21,209.75 rows/s | 8.364 ms |
| R2DBC | PostgreSQL | atomicBatchInsert | 350.29 ops/s | 11,209.28 rows/s | 16.015 ms |
| R2DBC | PostgreSQL | independentBatchInsert | 634.36 ops/s | 20,299.49 rows/s | 8.806 ms |

这些数字是本机固定环境回归基线，不是跨机器性能承诺。后续改动用相同环境比较中位数；性能优化不能关闭 Scope、
参数化、事务确认、错误分类、执行保护或观测来换取更好数字。
