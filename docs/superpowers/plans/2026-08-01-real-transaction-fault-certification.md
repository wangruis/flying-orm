# 真实事务与故障认证实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在固定 Docker MySQL/PostgreSQL 上验证 flying-orm 的批量事务结果、真实错误分类、超时取消和锁冲突行为。

**Architecture:** 新增独立的外部事务认证测试类，继续复用现有系统属性、`R2dbcSqlExecutor` 和批量结果模型。普通功能 smoke 与事务故障分开，认证脚本用测试类通配符一次执行两组；只有数据库真实返回的错误才能计入实库认证。

**Tech Stack:** Java 21、JUnit 5、Reactor、R2DBC SPI、r2dbc-mysql、r2dbc-postgresql、Maven、Docker Compose、PowerShell。

## Global Constraints

- 主项目不依赖 Spring，也不新增 Spring 代码。
- 默认批量策略仍是 ATOMIC；INDEPENDENT 必须显式开启。
- 测试没有配置外部 URL 时安静跳过。
- SQL 和参数不能包含密码，认证证据继续写入忽略的 `target/certification-results`。
- 只做稳定可复跑的场景；断网和提交响应丢失留给后续容器控制批次。
- 注释和文档使用自然、能直接看懂的中文。

---

### Task 1: 真实批量事务与约束冲突

**Files:**
- Create: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcTransactionCompatibilityTest.java`

**Interfaces:**
- Consumes: `R2dbcSqlExecutor.create(ConnectionFactory)`、`BatchWriteRequest`、`BatchWriteOptions.atomic(int)`、`BatchWriteOptions.independent(int)`。
- Produces: MySQL/PostgreSQL 共用的 ATOMIC 回滚、INDEPENDENT 部分成功和重复键分类认证场景。

- [x] 写 MySQL/PostgreSQL 参数化测试，构造三行输入，其中第二行重复主键。
- [x] 运行外部测试，确认新测试在现有认证入口中尚未被执行，形成脚本接入的 RED。
- [x] 验证 ATOMIC 抛出 `BatchWriteException`、结果为 `ROLLED_BACK`，并确认表中没有残留行。
- [x] 验证 INDEPENDENT 返回 `COMMITTED/FAILED/COMMITTED`，并确认成功分片真实落库。
- [x] 验证普通重复键写入被翻译成 `RdbErrorKind.DUPLICATE_KEY`，原始驱动异常仍保留在 cause 链中。

### Task 2: 真实超时取消与锁冲突

**Files:**
- Modify: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcTransactionCompatibilityTest.java`
- Modify when RED proves a missing mapping: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/exception/RdbExceptionTranslator.java`
- Test when production mapping changes: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/exception/RdbExceptionTranslatorTest.java`

**Interfaces:**
- Consumes: `SqlExecutionOptions.timeout(Duration)`、`RdbException.kind()`、R2DBC `Connection.beginTransaction()`。
- Produces: MySQL/PostgreSQL 慢查询超时后可继续使用执行器，以及 `FOR UPDATE NOWAIT` 锁冲突的稳定分类结果。

- [x] 写慢查询超时测试，MySQL 使用 `sleep`，PostgreSQL 使用 `pg_sleep`，随后执行 `select 1` 确认下一次连接可正常工作。
- [x] 写双连接锁测试：第一连接持有行锁，第二次通过 flying-orm 执行 `FOR UPDATE NOWAIT`。
- [x] 运行真实库测试观察 RED，只补数据库实际返回但现有翻译器遗漏的错误码。
- [x] 增加对应翻译器单元测试并先验证失败，再实现最小映射。
- [x] 重跑两库场景，要求错误稳定归为 `TIMEOUT` 或 `LOCK_TIMEOUT`，并确保持锁事务在成功、异常、取消路径都回滚关闭。

### Task 3: 认证入口、证据和文档

**Files:**
- Modify: `certification/Invoke-Certification.ps1`
- Modify: `docs/real-database-certification.md`
- Modify: `docs/database-support-matrix.md`
- Modify: `docs/v1-roadmap.md`

**Interfaces:**
- Consumes: `ExternalR2dbc*CompatibilityTest` 测试类命名约定。
- Produces: 一条可复跑的 `Core` 认证命令和脱敏证据目录。

- [x] 把认证脚本测试选择器改成 `ExternalR2dbc*CompatibilityTest`，让功能与事务测试同批执行。
- [x] 执行 `mvn test`，确认未配置外部 URL 时所有外部场景按设计跳过。
- [x] 执行 `Invoke-Certification.ps1 -Action Verify -Database Core`，记录通过、失败和跳过数量。
- [x] 更新支持矩阵和认证进度，只声明本轮真实通过的范围。
- [x] 批量提交并推送；保持 `AGENTS.md` 未暂存。
