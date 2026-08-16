# 已知限制

## 轻量 JOIN 与受保护字段

- JOIN 首版只提供 INNER、LEFT OUTER、RIGHT OUTER 的扁平只读查询；不提供 FULL/CROSS、自连接、写 JOIN、
  GROUP BY/HAVING、任意子查询、cursor page 或懒加载实体图。offset/page 与 count 已支持。
- JOIN 使用内部稳定 `tN` 别名。`selectAs(...)` 是结果列名/DTO 映射能力，不是要求调用方维护 SQL 表别名。
- 受保护字段不能用于 JOIN ON、排序或范围比较；EXACT/SUFFIX 只允许 WHERE，CONTAINS 首版不允许 JOIN。
- 可搜索加密会泄露受控的相等、后缀或 trigram 重复模式；只应为确有查询需求的字段声明对应模式。
- CONTAINS 先读取最多 1000 个候选再解密复核。超过上限稳定失败；首版不以静默截断换取表面成功。
- 为同时兼容 Oracle 的单个 `IN` 列表和 SQL Server 的请求参数上限，单版本 CONTAINS 查询最多使用 1000 个
  token，跨可读密钥版本的一次查询最多绑定 2100 个参数；超过时在驱动执行前稳定失败。
- JOIN offset 分页必须显式声明至少一个带表来源的排序；JOIN 游标分页仍未开放。
- 多版本密钥轮换期间，加密唯一字段必须配置独立稳定的 `uniqueSearchKey`；更换该密钥前必须完成全量唯一 token 重建与冲突检查。
- Schema 同步只创建目标密文列、盲索引列和辅助表，不会猜测并自动改写既有历史明文。历史数据必须使用显式迁移任务。
- `ProtectedFieldReprotection` 负责识别和解密旧版本，不负责扫描、并发调度、进度持久化或重试；这些属于上层迁移作业。

本文记录当前 V2 开发分支已经确认的边界。没有用当前代码得到真实结果的能力不会写成“已认证”。

## 数据库支持

- H2 用于内嵌开发和测试，不代表生产数据库行为。
- V2 已使用当前代码重新认证 MySQL 8.4.10、PostgreSQL 17.8 + pgvector 0.8.1、Oracle Free 23.26.0 和
  SQL Server 2022 CU22 GDR1；四库功能、事务、故障、取消和并发批次连续复跑三轮，结论只基于当前版本证据。
- MySQL、PostgreSQL 已完成 JDBC/R2DBC 三轮固定参数性能门禁。Oracle、SQL Server 已完成功能、事务、故障和并发
  边界认证；专门吞吐基线不作为 V2 阻断项。
- SQL Server 的强制断连恢复用例结束后，`r2dbc-mssql` 偶尔会在驱动后台记录 `onErrorDropped(Connection closed)`。业务 Publisher、连接池归零和测试结果均正常；内核不会安装全局 Reactor Hook 去吞掉应用错误。
- OpenGauss 不在当前支持范围。

## 执行与事务

- 响应式 API 只使用 R2DBC，不阻塞数据库线程；`Sync*` API 只使用原生 JDBC。两条链共享 SQL 和安全语义，但不互相桥接。
- flying-orm 只管理自身显式开启的批量事务。上层事务管理和动态数据源由上层服务负责，必须把事务结果纳入业务处理。
- `UNKNOWN` 表示提交结果暂时无法确认，不等于失败或成功。上层必须使用 recovery token 查询回执或按业务幂等键核对。
- 事务回执表、recovery token 查询和同 operationId 安全重放目前由 R2DBC 批量内核提供。原生 JDBC
  仍会准确返回 `UNKNOWN`，但不会伪造无法查询的回执令牌；JDBC 调用方需要用业务唯一键或上层幂等记录确认。
- 内核不会自动重试写操作，避免无幂等保障时重复写入。
- JDBC 的 `INDEPENDENT` 批量当前按分片顺序执行，`concurrency` 必须为 `1`。每个分片仍独立提交并返回自己的结果；
  需要并行分片时优先使用 R2DBC，避免同步内核私自创建线程池或突破连接池上限。
- JDBC/R2DBC 的连接排队、获取超时和健康检查由 HikariCP、r2dbc-pool 或其他上层连接池配置。
  ORM 只借用、归还连接，并在取消、污染或结果不确定时请求失效；SQL 执行兜底和驱动 socket 超时分别由
  flying-orm 执行选项与上层数据源配置协作。

## 动态表结构

- 默认安全迁移不会自动执行删列、危险类型变更、主键和外键重构。
- 在线 DDL 审核、结构化回滚计划、锁等待上限和回滚缺口已经提供；参数化数据迁移可以为成功步骤执行逆序补偿。删列数据恢复、复杂约束重建和真正零停机迁移仍需要备份、外部在线 DDL 工具或维护窗口，补偿 SQL 也不能代替备份。
- DDL 后会精确失效本进程的 Caffeine 元数据缓存；多实例缓存通知由上层基础设施负责。

## 条件与数据范围

- 前端只能传结构化条件，不能透传 SQL、列名片段或 JSON path 片段。
- 业务 term 的语义和数据来源由上层实现，ORM 负责注册、校验、参数化和组合。
- Tenant/Data/Field/Time Scope 是数据访问保护，不替代完整的身份认证和权限决策系统。

## 类型系统

- PostgreSQL Array、JSON 和 pgvector 已完成真实库认证；Vector 当前覆盖参数绑定、读取、距离条件和最近邻排序，不等同于完整向量索引管理平台。
- JSON 方言能力仍以 MySQL/PostgreSQL 为主。Oracle/SQL Server 已认证 V2 当前契约，更高级的数据库原生 JSON 特性不在本版本承诺内。
- V2 提供受控的滚动结果集、保存点和完整 JDBC 元数据入口。这些能力必须显式调用，并继续遵守当前事务连接所有权、执行保护、观测和异常翻译；不同驱动的具体兑现程度留到真实库认证确认。

## 框架边界

- 主项目不包含 Spring、自动配置和 Web 错误映射。
- Spring Boot 开箱即用能力放在上层服务或平级示例，不能反向污染 ORM 内核。
