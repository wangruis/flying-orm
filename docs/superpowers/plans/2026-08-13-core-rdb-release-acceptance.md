# Core/RDB 封版验收计划

> **For agentic workers:** 本计划在当前任务内串行执行；受协作规则约束，不派生子代理，不创建分支、不提交或推送 Git。

**Goal:** 对当前工作区中 `flying-orm-core` 与 `flying-orm-rdb` 的全部生产代码改动进行封版级复核，修复所有可复现问题，并以完整质量门禁证明最终源码一致。

**Architecture:** 以当前 `HEAD` 为比较基线，先审查全部已修改及新增生产类型，再沿调用图复核直接上下游。审查分为 Core 模型与 codec、RDB 查询与 Schema、JDBC/R2DBC 批量与资源生命周期三个闭环；任何修改必须先有失败证据，修复后重跑直接测试和全量质量门禁。

**Tech Stack:** Java 21、Maven 3.9.16、JUnit Jupiter、Reactor、R2DBC SPI、JDBC、JaCoCo、Checkstyle、SpotBugs。

## Global Constraints

- 只审查 `flying-orm-core/src/main/java` 与 `flying-orm-rdb/src/main/java` 的生产代码；测试仅作为契约证据。
- 不删除或扩张稳定公共 API，不改变 V2.0.0 基线，不引入 Spring 或新的 ORM 内核。
- 保持 JDBC/R2DBC 的事务、Scope、SQL、批量状态和错误分类对称。
- 新增生产类型必须少于 300 个物理行和 20 个可调用方法；所有生产类型必须少于 400 行。
- 不通过放宽断言、跳过测试或吞掉异常制造绿色结果。
- 不执行 Git 提交、推送或真实数据库认证。

---

### Task 1: 冻结差异与调用链

**Files:**
- Review: `flying-orm-core/src/main/java/**/*.java`
- Review: `flying-orm-rdb/src/main/java/**/*.java`

- [ ] 记录当前 `HEAD`、工作区状态、生产代码差异和新增内部类型。
- [ ] 使用知识图谱复核修改热点及直接调用方；索引不足时以物理源码补足。
- [ ] 确认没有无关删除、稳定公共 API 扩张或大类门禁回归。

### Task 2: Core 模型、条件与 codec

**Files:**
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/codec/*.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/condition/TermCondition.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/form/*.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/page/CursorPageQuery.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/param/ParameterConditionSpec.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/scope/TenantScope.java`
- Review: `flying-orm-core/src/main/java/com/flying/orm/core/sql/render/RelationExistsTermHandler.java`

- [ ] 验证嵌套数组图快照的别名、环、访问器隔离和普通对象身份边界。
- [ ] 验证内置 codec 对普通非法值保持脱敏，对嵌套 `VirtualMachineError` 原对象传播。
- [ ] 验证 Schema change set 不能伪造来源、字段差异或 case-only rename。
- [ ] 验证关系表允许限定名，而 alias/字段只允许单段标识符。

### Task 3: RDB 查询、Scope、Schema 与连接

**Files:**
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormOperationPlanner.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/FormScopeGuard.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/isolation/RoutingConnectionFactory.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/schema/*.java`

- [ ] 验证批量更新必须有业务 where，Scope 只能收窄范围。
- [ ] 验证 QuerySpec 的字段权限、加密/脱敏排序分组、投影和游标约束在 SQL 前执行。
- [ ] 验证 MySQL 类型修改、不可表达约束拒绝和主表/侧表精确缓存失效。
- [ ] 验证 R2DBC session initialize 取消与 reset 同步/异步失败均会隔离连接。

### Task 4: 批量、回执、LOB 与事务终态

**Files:**
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/*.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/execution/*.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/jdbc/*.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/*.java`
- Review: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/result/JdbcDynamicRowFactory.java`

- [ ] 验证批量行在所有权交接时冻结可变参数，并在复制前按预算失败闭合。
- [ ] 验证 JDBC/R2DBC 输入失败、取消、提交/回滚和连接清理不会改写已确认终态。
- [ ] 验证总 deadline 不被连续元素重置，回执主动确认不隐藏原始致命错误。
- [ ] 验证 JDBC SQLXML 与 R2DBC Blob/Clob 在连接资源域内物化或释放，覆盖全部 Row 出口。
- [ ] 验证公开批量失败摘要不泄露驱动消息或非法 SQLState。

### Task 5: 失败驱动修复

**Files:**
- Modify: 仅限能够复现问题的生产类型。
- Test: 对应模块的直接契约测试。

- [ ] 为每个确认问题先添加或确认失败测试。
- [ ] 以最小职责范围修复根因，不新增稳定 API。
- [ ] 重跑直接测试，随后反向复核调用方和对称路径。

### Task 6: 完整质量门禁

- [ ] 运行聚焦测试，覆盖本轮修改的直接契约。
- [ ] 运行：`D:\apache-maven-3.9.16\bin\mvn.cmd -Dmaven.repo.local=D:\MavenRepository -Pquality -pl flying-orm-core,flying-orm-rdb -am clean verify`。
- [ ] 核对测试总数、JaCoCo、Checkstyle、SpotBugs 和 23 项公共 API 门禁。
- [ ] 运行 `git diff --check`，并重新计算生产类行数与新增稳定 API。

### Task 7: 最终反向 code-review

- [ ] 只基于修复后的最终源码重新审查差异，不以旧结论代替现状。
- [ ] 检查修复之间是否互相冲突，尤其是快照与内存预算、回执与 VME、LOB 与取消、Schema 与缓存失效。
- [ ] 只有确认问题为零且完整质量门禁通过，才给出封版通过结论；真实数据库认证单独声明为未执行边界。
