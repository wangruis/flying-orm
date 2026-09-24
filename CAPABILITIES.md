# flying-orm 4.1.1 能力

Java 模型与条件 → 参数化 SQL → JDBC / R2DBC 执行 → 结果映射。常用操作使用短入口，复杂模型继续使用同一内核的规格对象，不需要另装一套客户端。

## 能力与入口

| 能力 | 直接入口 | 保留的语义 |
| --- | --- | --- |
| 动态表单 CRUD | `operator.dml().query(form) / insert / update / delete` | 运行时模型、类型转换、参数快照、非空写条件保护 |
| 实体读写 | `clients.repository(Type.class)` | 注解映射、Lambda 字段、生成键、填充和生命周期 |
| 投影与结果映射 | `select(...).fetchMap() / fetch(Type.class) / fetch(mapper) / one(Type.class)` | DynamicRow、bean、record、自定义 RowMapper；实体 `fetch()` 支持部分投影 |
| 条件 | `where(field, operator, value)`、`filter(input)` | AND/OR、NULL、集合、区间、可选条件、已注册业务语义 |
| Scope 与字段治理 | `withDefaultDataScope`、`scope`、`from(form, policy, limits)` | 行范围取交集；投影、过滤、排序、分组等用途分别审核 |
| 分页 | `page / cursorPage / keysetPage` | 页码与总数、稳定游标、复合可空 keyset、明确 NULL 顺序；动态查询可直接传 `Type.class` 映射 DTO |
| JOIN / 自关联 | `dml().joinQuery`、`JoinQuerySpec` | 多来源、复合 ON、来源限定排序与分页；同表不同角色独立治理 |
| 报表 | `query(form).aggregate(...)` | 分组、COUNT / COUNT DISTINCT / SUM / AVG / MIN / MAX、HAVING、类型化结果 |
| 写入治理 | `update / delete`、`WriteSpec` | 租户、逻辑删除、乐观锁、字段权限、范围内写入 |
| 批量 | `insertBatch / upsertBatch / updateBatch` | 有界缓冲、逐行乐观锁、范围内冲突更新、生成键、保护字段、执行证据；响应式 Repository 可直接接收实体流并沿用客户端预算 |
| 多行同值更新 / 删除 | `update / delete + where(..., "in", ids)` | 一条范围操作，继续应用 Scope 与逻辑删除 |
| 动态结构 | `operator.ddl()`、`clients.schema()` | 建表、加列、索引、差异计划、风险审核、执行前核验与回读 |
| 读取已有结构 | `operator.metadata().readTable / readForm` | 表、列、索引与外键元数据，动态表单转换、显式缓存失效 |
| 实体注解结构同步 | `clients.entitySchemas()` | 校验、安全更新、批准后的危险变更；完整关系模型使用 relational 入口 |
| 加密、检索、脱敏 | 模型声明 + `ProtectedConditions` | EXACT / SUFFIX / CONTAINS、辅助关系、候选验证、声明/强制脱敏 |
| 锁定读取 | `forms.lockingRead` | 方言支持的行锁语义，不管理事务 |
| SQL 模板与原生 SQL | `operator.sqlTemplate / unsafeNativeSql` | 注册模板、标识符槽位、命名参数、映射与执行保护 |

## 查询安全

前端只提供结构化条件，不提供 SQL；服务端决定模型、可用条件、字段用途和 Scope。条件值参数绑定，字段及标识符受控解析。自定义条件处理器和原生 SQL 属于可信后端代码。

绑定 DynamicForm 的入口保留字段治理、加密及脱敏。物理表字符串查询不具备未提供的表单元数据；模板/原生 SQL 不自动注入租户或逻辑删除，不能当作受治理表单查询替代品。

## 批量语义

- INSERT、UPSERT 和每行独立乐观锁 UPDATE 使用批量内核；没有独立的 `BatchSpec.delete`。
- 输入、行重量和缓冲有预算，响应式输入在订阅后消耗，不为方便而收集整批。
- Scope 限制 UPSERT 的已有冲突目标，不允许越范围更新。
- `BatchExecutionEvidence` 只报告已证明的执行位置、影响行数、冲突及安全失败摘要。未知计数不是零，也不代表已提交。
- 失败、重试、补偿及原子性由上层裁决；不能把证据当事务回执。

## 模型与结构

实体注解支持表、列、主键、生成方式、唯一约束、索引、外键、CHECK、注释及受控分区声明。类型能力包括 UUID、日期时间、数组、自定义 codec、实体类型映射及主键回填。

PostgreSQL、MySQL、Oracle、SQL Server、H2 使用各自方言能力；不支持的语义明确拒绝。PostgreSQL 分区父表声明不等于 ORM 分片。数据库版本支持和真实往返测试必须单独核验，静态方言合同不等于五库实测认证。

## 配置与边界

缓存、字段填充、ID 生成、扩展条件、日志、观测、批量预算和保护字段策略在客户端装配阶段配置。普通查询不需要操作内部 Runtime、Planner 或协调器。

容量由开发者决定：分页无框架固定上限；条件、读取和批量使用可配置预算，内部不再叠加更小的固定门槛。加密检索不另设固定候选数、令牌数或密钥版本数量上限。参数绑定、字段治理、密码格式和目标数据库的实际约束仍然有效。

JDBC 返回同步结果；R2DBC 返回原生 Flux/Mono，保留取消、背压与资源清理。不提供事务、分片、驱动或连接池管理，也不配置连接、事务及执行超时。外部 Connection 由上层获取/释放，ORM 清理自己的语句、结果与 LOB。

## 阅读入口

[快速接入](README.md) · [完整示例](EXAMPLES.md) · [高级能力](ADVANCED-CAPABILITIES.md) · [实体注解](ANNOTATIONS.md)
