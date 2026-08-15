# 真实连接池与并发稳定性认证实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在真实 MySQL/PostgreSQL 上完成连接池耗尽恢复、慢消费者取消回压和持续有界并发认证。

**Architecture:** testkit 使用仅测试期的 `r2dbc-pool` 包住现有真实驱动；小池提供确定的资源上限，`SqlExecutionOptions` 负责 ORM 侧连接等待保护，`StepVerifier` 控制下游需求和取消，`ReactiveConcurrencyProbe` 统一统计持续并发结果。

**Tech Stack:** Java 21、JUnit 5、Reactor、Reactor Test、R2DBC Pool、MySQL 8.4、PostgreSQL 17、Maven、Docker Compose。

## Global Constraints

- 主项目不依赖 Spring，连接池只作为 testkit 测试依赖。
- 不扩大 V1 公共 API；真实运行未暴露内核缺口时不修改生产代码。
- 测试保持有限操作数和明确超时，不在本批输出正式性能结论。
- 注释和文档使用自然、能直接看懂的中文。
- 本批统一提交，保持 `AGENTS.md` 未暂存。

---

### Task 1: 连接池耗尽、超时与恢复

**Files:**
- Modify: `pom.xml`
- Modify: `flying-orm-testkit/pom.xml`
- Create: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcConcurrencyStabilityCompatibilityTest.java`

- [x] 增加仅测试期 `r2dbc-pool`，固定版本由父 POM 管理。
- [x] 借满两条连接后发起带连接获取超时的 ORM 查询。
- [x] 断言稳定异常类型和 `CONNECTION` 分类，再释放连接并验证等待队列清空。
- [x] 复用同一个池执行轻查询，确认池和执行器恢复。

### Task 2: 慢消费者、需求与取消

**Files:**
- Modify: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcConcurrencyStabilityCompatibilityTest.java`

- [x] 为两库准备固定数量的有序记录。
- [x] 使用零初始需求订阅，分段 request 后只收到对应数量。
- [x] 在结果未读完时 cancel，并等待池归还连接。
- [x] 取消后执行轻查询，确认没有连接泄漏或污染。

### Task 3: 持续有界并发与资源曲线

**Files:**
- Modify: `flying-orm-testkit/src/test/java/com/flying/orm/testkit/dialect/ExternalR2dbcConcurrencyStabilityCompatibilityTest.java`

- [x] 用 4 条连接的小池和同样大小的并发上限连续执行有限次查询。
- [x] 断言全部成功、没有整体超时、探针峰值不超过上限。
- [x] 等待池指标稳定，断言借出数和等待数归零。
- [x] 关闭池并确认测试不会留下后台资源。

### Task 4: 证据、回归和提交

**Files:**
- Modify: `docs/real-database-certification.md`
- Modify: `docs/database-support-matrix.md`
- Modify: `docs/v1-roadmap.md`

- [x] 执行新增外部测试和普通 Maven 回归。
- [x] 执行 `Invoke-Certification.ps1 -Action Verify -Database Core` 并归档脱敏证据。
- [x] 只记录真实通过的连接池、取消和持续并发结论。
- [x] 清理无用 import、变量和重复辅助逻辑，执行 `git diff --check`。
- [x] 批量提交并推送，保持 `AGENTS.md` 未暂存。
