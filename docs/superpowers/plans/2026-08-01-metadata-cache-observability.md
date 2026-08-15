# 元数据缓存观测与容量收口实施计划

> 历史计划：其中 `MetadataCacheOptions` API 已被 v1.0.0 的 `CacheRegionPolicy` 取代；保留下文只用于追溯当时的设计过程。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不引入 Spring 或指标框架的前提下，给 Caffeine 元数据缓存补稳定统计快照，并锁住容量、失败重试和并发失效边界。

**Architecture:** 保留 `CachedReactiveFormMetadataReader` 的两块独立缓存：表单元数据和数据库表元数据各自使用一个 Caffeine 实例，`maxEntries` 表示每块缓存的硬上限。新增一个只含 Java 基础类型的只读快照：命中和驱逐沿用 Caffeine 统计，加载成功、失败和耗时在被共享的源 Mono 上记录；旧的 `formStats()` / `tableStats()` 继续保留兼容。

**Tech Stack:** Java 21、Reactor、Caffeine 3.2.4、JUnit 6、Maven。

**Execution Status:** 本计划已在同一批次完成；下方复现步骤保留为实现和复核记录。

## Global Constraints

- 主项目不依赖 Spring，也不直接依赖 Micrometer。
- 不连接真实数据库，测试只使用内存 delegate。
- 测试保持少量，只覆盖容量、失败重试和并发失效三个高风险边界。
- 新增和修改代码写自然、能直接看懂的中文注释。
- 不修改 `flying-orm-testkit` 和 `flying-orm-benchmark` 源码。
- 不提交用户修改的 `AGENTS.md`。
- 本批任务完成后统一提交，不为每个小步骤单独提交。

---

### Task 1: 稳定统计快照

**Files:**
- Create: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/MetadataCacheSnapshot.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReader.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReaderTest.java`

**Interfaces:**
- Consumes: Caffeine `Cache.estimatedSize()` 和 `CacheStats`。
- Produces: `MetadataCacheSnapshot`、`MetadataCacheSnapshot.Region`、`CachedReactiveFormMetadataReader.snapshot()`。

- [ ] **Step 1: 写失败测试**

  读取同一表单两次后断言 `snapshot().forms()` 的 entries、hitCount、missCount 和 loadSuccessCount；同时断言 `combined()` 汇总两块缓存。

- [ ] **Step 2: 确认测试因 API 不存在而失败**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

- [ ] **Step 3: 实现最小快照 API**

  `MetadataCacheSnapshot` 使用两个 `Region` 表示 forms/tables。`Region` 保存 entries、请求、命中、未命中、加载成功、加载失败、总加载纳秒、驱逐数量和驱逐权重，并提供 `plus` 供 `combined()` 汇总。

- [ ] **Step 4: 验证测试通过**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

---

### Task 2: 容量和失败重试边界

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/MetadataCacheOptions.java`
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReader.java`
- Test: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReaderTest.java`

**Interfaces:**
- Consumes: `MetadataCacheOptions.maxEntries()`、`CachedReactiveFormMetadataReader.snapshot()`。
- Produces: 明确的“每个缓存区域上限”语义、快照前 `cleanUp()`、失败结果立即驱逐并允许下一次重新加载。

- [ ] **Step 1: 写容量失败测试**

  用 `maxEntries(1)` 连续读取两个表单，取快照后断言 forms entries 不超过 1 且 evictionCount 至少为 1。

- [ ] **Step 2: 写失败重试测试**

  delegate 第一次返回错误、第二次成功；断言错误不会留在缓存，第二次确实再次调用 delegate，快照记录一次加载失败和一次加载成功。

- [ ] **Step 3: 确认测试失败原因正确**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

- [ ] **Step 4: 实现容量收口**

  快照读取前调用两块缓存的 `cleanUp()`，让维护任务、TTL 和容量驱逐先落地；补清楚 `maxEntries` 是每个区域的上限，总理论上限为两倍。

- [ ] **Step 5: 验证测试通过**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

---

### Task 3: 失效期间的并发加载边界

**Files:**
- Modify: `flying-orm-rdb/src/test/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReaderTest.java`
- Modify only if the test exposes a defect: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/CachedReactiveFormMetadataReader.java`

**Interfaces:**
- Consumes: `MetadataCacheInvalidator.invalidate(String)`。
- Produces: 失效后新订阅不会复用失效前缓存项的契约。

- [ ] **Step 1: 写并发失效测试**

  第一次加载用可控 `Mono` 挂起；调用 `invalidate("Users")` 后发起第二次读取，断言 delegate 被调用两次，第二次结果不会被第一次加载覆盖。

- [ ] **Step 2: 运行并观察现有实现行为**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

- [ ] **Step 3: 仅在失败时修复生产代码**

  使用 `cache.asMap().remove(key, expectedValue)` 保证旧加载只能删除自己，不能删除失效后放入的新值；不增加全局锁。

- [ ] **Step 4: 验证测试通过**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

---

### Task 4: 文档和交付验证

**Files:**
- Modify: `docs/requirements/index.md`
- Modify: `docs/v1-roadmap.md`
- Modify: `docs/target-api-examples.md`

**Interfaces:**
- Consumes: `CachedReactiveFormMetadataReader.snapshot()`。
- Produces: 上层服务读取快照并接入自身监控系统的示例。

- [ ] **Step 1: 更新文档**

  说明主项目只暴露稳定快照，不管理上层 Spring/Caffeine/Micrometer；明确每区域容量和总理论容量。

- [ ] **Step 2: 运行重点回归**

  Run: `mvn -pl flying-orm-rdb -am "-Dtest=CachedReactiveFormMetadataReaderTest,DatabaseOperatorTest,FlyingOrmClientsTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`

- [ ] **Step 3: 运行发布构建**

  Run: `mvn -P release-artifacts -DskipTests clean verify`

- [ ] **Step 4: 批量提交推送**

  只暂存本计划涉及的代码、测试和文档，提交说明使用自然中文；确认 `AGENTS.md` 留在工作区且未暂存。
