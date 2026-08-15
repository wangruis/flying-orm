# Java 中文注释治理实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `flying-orm-core` 和 `flying-orm-rdb` 的 277 个 Java 文件补齐自然、准确、能解释设计边界的中文注释。

**Architecture:** 注释按领域包分批补充，不改变任何类型、方法签名或执行路径。生产代码先补类型职责和公共契约，再补复杂内部流程；测试代码说明用例要防止的具体回归。每批用编译兜底，最终统一运行纯单元测试和全模块打包。

**Tech Stack:** Java 21、Maven、JUnit 5、Reactor、R2DBC、Javadoc。

## Global Constraints

- 只修改 `flying-orm-core`、`flying-orm-rdb` 和本任务设计/计划文档。
- `flying-orm-testkit`、`flying-orm-benchmark` 保持不变。
- 不修改用户本地的 `AGENTS.md`。
- 不改变 API、SQL、事务、缓存、并发或异常行为。
- 使用自然中文解释职责、约束和原因，不写逐行旁白。
- 简单 getter、record 自动访问器和机械转发不强行添加重复注释。
- 不新增 Spring 依赖，不运行真实数据库、并发压力或性能测试。
- 全部注释治理完成后只做一次批量 Git 提交。

---

### Task 1: Core 条件、表单和数据范围

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/condition/*.java`（20 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/form/*.java`（8 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/param/*.java`（4 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/scope/*.java`（7 个）

**Interfaces:**
- Consumes: 当前条件 AST、动态表单定义、参数条件编译和 Scope API。
- Produces: 能说明结构化条件安全边界、空值策略、租户与数据范围合并规则的 Javadoc。

- [ ] **Step 1: 扫描 39 个文件的主类型和公共 API 注释缺口**

  对每个文件确认主类型是否解释职责，公共构造/工厂/编译方法是否解释输入约束和失败语义。

- [ ] **Step 2: 补类型和公共契约注释**

  重点写清前端结构化条件如何进入 AST、字段/operator/value 如何校验、Scope 为什么只能安全 AND 合并，以及无租户系统和 SaaS 系统的差别。

- [ ] **Step 3: 补复杂内部流程注释**

  在递归条件编译、嵌套深度控制、空值归一化、租户保护和字段范围组合处解释“为什么”，不注释普通集合操作。

- [ ] **Step 4: 编译 core**

  Run: `mvn -pl flying-orm-core -DskipTests compile`

  Expected: `BUILD SUCCESS`，没有 Java 编译错误。

### Task 2: Core 元数据、SQL 和基础值对象

**Files:**
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/codec/*.java`（2 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/command/*.java`（2 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/error/*.java`（2 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/metadata/*.java`（9 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/page/*.java`（4 个）
- Modify: `flying-orm-core/src/main/java/com/flying/orm/core/sql/**/*.java`（17 个）

**Interfaces:**
- Consumes: 当前不可变元数据模型、SQL AST、渲染上下文和 codec 注册表。
- Produces: 能说明参数顺序、标识符安全、值转换方向和元数据快照语义的注释。

- [ ] **Step 1: 补 36 个文件的主类型说明**

  区分值对象、注册表、规划器、AST 和渲染器的职责，明确哪些对象不可变、哪些 builder 只能在构建期使用。

- [ ] **Step 2: 补公共 API 契约**

  说明绑定参数顺序、标识符为何不能当参数绑定、codec 查找优先级、分页边界和元数据查找结果。

- [ ] **Step 3: 补 SQL 渲染关键过程注释**

  在 term 分派、方言无关 AST 到 SQL 片段、关系条件渲染和参数收集处说明顺序与注入防护。

- [ ] **Step 4: 编译 core**

  Run: `mvn -pl flying-orm-core -DskipTests compile`

  Expected: `BUILD SUCCESS`。

### Task 3: RDB 执行、批量、事务和观测

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/batch/*.java`（9 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/bootstrap/*.java`（1 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/exception/*.java`（4 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/execution/*.java`（4 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/jdbc/*.java`（2 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/observation/*.java`（12 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/reactive/*.java`（11 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/sync/*.java`（2 个）

**Interfaces:**
- Consumes: `ReactiveSqlExecutor`、R2DBC SPI、批量结果模型、执行保护选项和 observer SPI。
- Produces: 明确非阻塞边界、JDBC 桥接方式、ATOMIC/INDEPENDENT 语义、取消/回滚/UNKNOWN 和观测事件含义的注释。

- [ ] **Step 1: 补 45 个文件的类型与公共入口说明**

  说明客户端创建边界、executor 冷 Publisher 契约、同步桥接的阻塞位置和 observer 不得读取 SQL 参数值的安全约束。

- [ ] **Step 2: 详细注释批量事务状态机**

  覆盖 `NEW`、`ACTIVE`、`COMMITTING`、`COMMITTED`，解释何时能回滚、何时只能返回 `UNKNOWN`、receipt 和 recovery token 如何帮助上层恢复。

- [ ] **Step 3: 详细注释执行保护与资源释放**

  说明总超时与连接获取超时的区别、取消时 `usingWhen` 的清理路径、最大行数和最大批量输入如何阻止资源耗尽。

- [ ] **Step 4: 详细注释观测和异常分类**

  说明 CHUNK/SUMMARY/RECOVERY 的粒度、稳定错误分类来源、为什么观测对象不携带绑定值。

- [ ] **Step 5: 编译 rdb 及依赖模块**

  Run: `mvn -pl flying-orm-rdb -am -DskipTests compile`

  Expected: `BUILD SUCCESS`。

### Task 4: RDB 动态表单、Repository、Operator 和实体映射

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/form/*.java`（13 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/lock/*.java`（3 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/mapping/*.java`（9 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/operator/*.java`（24 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/repository/*.java`（4 个）

**Interfaces:**
- Consumes: 动态表单定义、条件 AST、Scope、实体元数据、乐观锁和逻辑删除配置。
- Produces: 能指导业务使用动态 CRUD、链式 Operator、Repository、逻辑删除和显式乐观锁的中文 API 文档。

- [ ] **Step 1: 补 53 个文件的类型职责和调用关系**

  说明 FormClient、Repository、DatabaseOperator 的入口差别和共享渲染/执行内核，避免让使用方误以为它们有不同安全规则。

- [ ] **Step 2: 补动态表单写入与 Scope 注释**

  解释租户字段自动补齐、受保护字段拒绝写入、逻辑删除条件注入和前端条件不能覆盖系统 Scope。

- [ ] **Step 3: 补实体映射和反射缓存注释**

  说明注解解析约定、命名策略、ClassValue 生命周期、字段读写转换和不可访问字段的失败方式。

- [ ] **Step 4: 补乐观锁与链式 Operator 注释**

  说明乐观锁必须显式开启、版本条件如何参与 update/delete、冲突为何不能当普通零行更新忽略。

- [ ] **Step 5: 编译 rdb 及依赖模块**

  Run: `mvn -pl flying-orm-rdb -am -DskipTests compile`

  Expected: `BUILD SUCCESS`。

### Task 5: RDB 方言、类型、元数据和 DDL

**Files:**
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/array/*.java`（3 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/codec/*.java`（3 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/dialect/*.java`（5 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/json/*.java`（6 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/metadata/*.java`（12 个）
- Modify: `flying-orm-rdb/src/main/java/com/flying/orm/rdb/schema/*.java`（7 个）

**Interfaces:**
- Consumes: 方言 SPI、codec、数据库元数据 reader、结构化 JSON/Array 条件和 Schema 迁移模型。
- Produces: 能解释数据库差异、安全类型白名单、缓存失效和危险 DDL 审核边界的注释。

- [ ] **Step 1: 补 36 个文件的类型和方言能力说明**

  明确 MySQL、PostgreSQL、H2 主线与 Oracle、SQL Server 后续兼容的职责位置，不把 SQL 标准写成数据库方言。

- [ ] **Step 2: 补 JSON、Array 和 LOB 类型注释**

  说明结构化 path/value 如何防注入、PostgreSQL 数组操作只在哪些方言可用、LOB 生命周期为何不能随意缓存。

- [ ] **Step 3: 补元数据缓存和 reader 注释**

  说明缓存键、Caffeine 容量/过期、动态改表后的精确失效，以及各数据库 reader 的系统表差异。

- [ ] **Step 4: 补 DDL 与迁移计划注释**

  说明安全类型语法、危险变更为什么进入审核计划而不直接执行、上层如何处理跳过项和迁移结果。

- [ ] **Step 5: 编译 rdb 及依赖模块**

  Run: `mvn -pl flying-orm-rdb -am -DskipTests compile`

  Expected: `BUILD SUCCESS`。

### Task 6: Core 和 RDB 测试说明

**Files:**
- Review: `flying-orm-core/src/test/java/**/*.java`（20 个）
- Review: `flying-orm-rdb/src/test/java/**/*.java`（48 个）

**Interfaces:**
- Consumes: 当前 JUnit 5 测试及测试内 R2DBC 代理。
- Produces: 测试代码保持简洁，缺少类型说明的测试类得到补充，高风险用例能直接说明要阻止的回归。

- [ ] **Step 1: 补缺少说明的测试类型**

  只处理没有任何 Javadoc 的测试类或明显失真的旧说明。测试类注释写“覆盖什么边界”，不写“这是测试类”。Jakarta/Javax 注解替身只说明它们用于验证反射兼容。

- [ ] **Step 2: 补复杂测试方法说明**

  只补结构化条件安全、Scope 绕过、批量事务、取消、超时、UNKNOWN、缓存失效、DDL 注入和方言 SQL 契约等高风险场景。普通断言、测试数据准备和名称已经清楚的测试方法不加注释。

- [ ] **Step 3: 运行纯单元测试**

  Run: `mvn -pl flying-orm-core test`

  Expected: core 全部测试通过。

  Run: `mvn -pl flying-orm-rdb "-Dtest=!H2*" test`

  Expected: 排除 H2 真实库测试后，其余 rdb 测试全部通过。

### Task 7: 全项目注释一致性复核与批量提交

**Files:**
- Verify: `flying-orm-core/src/**/*.java`
- Verify: `flying-orm-rdb/src/**/*.java`
- Verify unchanged: `flying-orm-testkit/**`
- Verify unchanged: `flying-orm-benchmark/**`
- Include: `docs/superpowers/specs/2026-08-01-java-comment-coverage-design.md`
- Include: `docs/superpowers/plans/2026-08-01-java-comment-coverage.md`

**Interfaces:**
- Consumes: 前六个任务的注释修改。
- Produces: 注释覆盖完整、行为未变、可编译并可一次提交的变更集。

- [ ] **Step 1: 复扫每个目标 Java 文件的主类型注释**

  目标数量必须是 core 95 个、rdb 182 个，共 277 个。逐一确认主类型前有自然中文说明，且说明与实现一致。

- [ ] **Step 2: 清理重复、失真和乱码注释**

  删除只重复名字的空话；修正因编码显示异常或已经不符合实现的旧说明；不借机重构代码。

- [ ] **Step 3: 检查改动边界和格式**

  Run: `git diff --check`

  Expected: 无空白错误。

  Run: `git status --short`

  Expected: `testkit`、`benchmark` 没有修改，`AGENTS.md` 仍未暂存。

- [ ] **Step 4: 全模块打包**

  Run: `mvn -DskipTests package`

  Expected: 五个模块全部 `BUILD SUCCESS`。

- [ ] **Step 5: 批量提交**

  Stage: `docs/superpowers/specs/2026-08-01-java-comment-coverage-design.md`、`docs/superpowers/plans/2026-08-01-java-comment-coverage.md`、`flying-orm-core`、`flying-orm-rdb`。

  Exclude: `AGENTS.md`、`flying-orm-testkit`、`flying-orm-benchmark`。

  Commit message: `补全核心代码注释，讲清执行和安全边界`
