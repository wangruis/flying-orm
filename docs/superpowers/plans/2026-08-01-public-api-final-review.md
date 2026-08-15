# Public API Final Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 收紧 V1 公共模型的状态边界，修复分页极值溢出，并补齐公开包的职责说明，不改变正常 SQL、事务和批量执行语义。

**Architecture:** 保留现有 core/rdb 包布局和稳定入口。校验放在公开不可变模型的构造器里，让错误状态在进入执行器或观测系统前失败；说明性改动放在 package-info 和核心请求模型注释中。

**Tech Stack:** Java 21、Reactor、R2DBC、JUnit 5、Maven。

## Global Constraints

- 主项目保持零 Spring 依赖。
- 本轮不连接真实数据库，不执行 Docker 认证。
- 不修改 `flying-orm-example`。
- 测试保持少量，只覆盖本轮真正改变的契约。
- 中文注释要自然、具体，说明为什么有这个边界。
- 不提交用户自己的 `AGENTS.md` 修改。

---

### Task 1: 锁定公共结果模型边界

**Files:**
- Modify: `flying-orm-core/src/test/java/com/flying/orm/core/page/PageResultTest.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/batch/BatchWriteResultTest.java`
- Create: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/observation/ExecutionObservationContractTest.java`

- [x] 增加 Long.MAX_VALUE 分页总数测试。
- [x] 增加批量分片状态、失败摘要和影响行数互相矛盾时的拒绝测试。
- [x] 增加普通 SQL 和批量观测状态互相矛盾时的拒绝测试。
- [x] 运行三组测试，确认当前实现会暴露问题。

### Task 2: 修正公共模型实现

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/page/PageResult.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchChunkResult.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/observation/SqlExecutionObservation.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/observation/BatchExecutionObservation.java`

- [x] 使用不发生加法溢出的公式计算总页数。
- [x] 让批量分片只携带与状态相符的 failure、recovery token、conflicts 和 affectedRows。
- [x] 让 SQL 观测的 SUCCESS、ERROR、CANCELLED 与错误分类保持一致。
- [x] 让 CHUNK、SUMMARY、RECOVERY 只携带各自的主状态字段。
- [x] 重跑定向测试。

### Task 3: 补齐公共包说明和内部边界

**Files:**
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/{command,error,metadata,param}/package-info.java`
- Create: `flying-orm-core/src/main/java/com/flying/orm/core/sql/{ast,plan}/package-info.java`
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/{array,codec,dialect,json,lock,metadata}/package-info.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/BatchWriteRequest.java`
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/reactive/ReactiveImplementationVisibilityTest.java`

- [x] 说明每个公开包能做什么、不能做什么以及安全边界。
- [x] 讲清批量 Publisher 的订阅和参数数组所有权。
- [x] 把回执存储和摘要器加入包内可见性契约。

### Task 4: 验证与交付

**Files:**
- Modify: `docs/v1-roadmap.md`

- [x] 清理无用 import、变量和重复校验。
- [x] 运行定向测试和完整 Maven 构建。core/rdb 共 402 个测试通过；全模块测试只剩冻结 testkit 中 SQL Server 双引号旧契约失败。
- [x] 运行发布 Profile，确认 Javadoc 和依赖检查通过。
- [x] 检查 Git 边界，只批量提交本轮文件并推送。
