# 真实死锁与恢复认证实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在真实 MySQL/PostgreSQL 上完成确定性死锁、会话中断和 UNKNOWN 回执恢复认证。

**Architecture:** 新增独立外部故障认证类，使用双事务反向锁制造死锁，使用数据库原生命令终止慢查询连接，并在真实提交完成后制造提交确认丢失。继续复用 `R2dbcSqlExecutor`、`RdbExceptionTranslator`、批量恢复令牌和现有认证脚本通配符。

**Tech Stack:** Java 21、JUnit 5、Reactor、R2DBC SPI、MySQL 8.4、PostgreSQL 17、Maven、Docker Compose、PowerShell。

## Global Constraints

- 主项目不依赖 Spring，也不新增 Spring 代码。
- 默认批量策略保持 ATOMIC；UNKNOWN 只在提交结果确实无法判断时返回。
- 不用 testkit 预设异常冒充真实数据库故障。
- 测试未配置外部 URL 时安静跳过。
- 注释和文档使用自然、能直接看懂的中文。
- 本批统一提交，保持 `AGENTS.md` 未暂存。

---

### Task 1: 确定性真实死锁

**Files:**
- Create: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcFailureRecoveryCompatibilityTest.java`

**Interfaces:**
- Consumes: R2DBC `Connection.beginTransaction()`、`SELECT ... FOR UPDATE`、`RdbExceptionTranslator.translate(Throwable)`。
- Produces: MySQL/PostgreSQL 真实死锁及稳定 `RdbErrorKind.DEADLOCK` 结论。

- [x] 写两库死锁测试：两个事务分别先锁记录 1 和记录 2，再同时申请对方记录。
- [x] 运行 `Core` 认证观察真实驱动结果；若分类遗漏，先给翻译器补失败契约。
- [x] 确保受害事务立即回滚释放锁，另一事务能结束，所有连接最终关闭。
- [x] 重跑两库死锁场景，要求一个且只有一个可识别死锁受害者。

### Task 2: 真实连接中断

**Files:**
- Modify: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcFailureRecoveryCompatibilityTest.java`
- Modify only when a real gap is proven: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/exception/RdbExceptionTranslator.java`
- Test only when production mapping changes: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/exception/RdbExceptionTranslatorTest.java`

**Interfaces:**
- Consumes: MySQL `CONNECTION_ID()`/`KILL CONNECTION`、PostgreSQL `pg_backend_pid()`/`pg_terminate_backend()`、`R2dbcSqlExecutor.query(...)`。
- Produces: 正在执行 SQL 的会话被真实终止后稳定 `CONNECTION` 分类，以及后续轻查询可用结论。

- [x] 预先读取受测连接的数字会话编号，并把这条连接交给执行器执行慢查询。
- [x] 管理连接延迟终止目标会话，断言执行器返回 `RdbErrorKind.CONNECTION`。
- [x] 执行 `select 1`，确认故障没有污染正常连接入口。
- [x] MySQL/PostgreSQL 各复跑一次，记录实际错误分类。

### Task 3: UNKNOWN 回执确认和安全重放

**Files:**
- Modify: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcFailureRecoveryCompatibilityTest.java`

**Interfaces:**
- Consumes: `BatchWriteOptions.atomic(int).withReceipt(String)`、`BatchWriteException.result()`、`ReactiveSqlExecutor.resolveUnknown(...)`。
- Produces: 真实提交后的 UNKNOWN、COMMITTED 确认和无重复重放证据。

- [x] 创建真实业务表和回执表，构造固定 operation id 的两行 ATOMIC 批量。
- [x] 包装连接：真实提交完成后关闭连接并返回 SQLState `08006`，先验证得到 UNKNOWN 和恢复令牌。
- [x] 使用稳定执行器解析令牌，要求返回 COMMITTED。
- [x] 使用相同 operation id 和参数重放，要求直接返回提交结果，业务表仍为两行。

### Task 4: 证据、回归和提交

**Files:**
- Modify: `docs/real-database-certification.md`
- Modify: `docs/database-support-matrix.md`
- Modify: `docs/v1-roadmap.md`

**Interfaces:**
- Consumes: `ExternalR2dbc*CompatibilityTest` 认证入口和本批真实运行结果。
- Produces: 可复跑证据、准确支持矩阵和下一批任务边界。

- [x] 执行 `mvn test`，确认普通构建的外部场景按设计跳过。
- [x] 执行 `Invoke-Certification.ps1 -Action Verify -Database Core`，记录通过、失败、跳过和证据目录。
- [x] 只把真实通过的死锁、连接中断和 UNKNOWN 恢复写入文档。
- [x] 复核无用 import、变量、凭据和越界说明，执行 `git diff --check`。
- [x] 批量提交并推送，保持 `AGENTS.md` 未暂存。
