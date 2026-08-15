# flying-orm v1.0.0 版本说明

> 当前源码版本为 `1.0.0`，Git 标签使用 `v1.0.0`。本次只在 GitHub 发布源码，不发布 Maven 仓库制品。

当前 Maven 坐标为 `com.flying.orm:flying-orm-core:1.0.0`、`com.flying.orm:flying-orm-rdb:1.0.0`。`testkit` 和 `benchmark` 用于研发验证，不是业务运行时依赖。项目不配置 Maven 发布仓库，也不上传或签名二进制制品。

## 版本定位

flying-orm 是一个简单、R2DBC 优先、为动态表单而生的 ORM。核心目标是用结构化 Java API 完成动态表结构、CRUD、参数驱动条件和业务 term 扩展，同时守住参数绑定、数据范围、资源保护和响应式执行边界。

## 候选版能力

- 动态表单 DDL、CRUD、分页、逻辑删除和显式乐观锁。
- 前端结构化条件、安全 operator 白名单、稳定错误码和字段路径。
- 可扩展业务 term，例如 `user-in-org`，ORM 只接收结构化参数，不接收前端 SQL。
- Reactor + R2DBC 真响应式执行；同步 API 通过 R2DBC 桥接，不维护第二套 JDBC 内核。
- MySQL、PostgreSQL、H2 方言主线；Oracle、SQL Server 为预览支持；不支持 OpenGauss。
- `ATOMIC` 默认批量、显式 `INDEPENDENT`、分片结果、冲突结果和 UNKNOWN 恢复令牌。
- Tenant/Data/Field/Time Scope 与逻辑删除安全合并，支持无租户和 SaaS 两类系统。
- Caffeine 元数据缓存、执行保护、SQL/批量观测和稳定数据库错误分类。
- 注册 SQL 模板可承载 CTE、聚合、窗口函数和数据库专有查询；服务端安全参数在订阅时提供，普通调用不能覆盖。
- `unsafeNativeSql(...)` 保留为明确的后端 SQL 逃生口，继续使用命名参数、统一执行保护和错误分类。
- 主项目零 Spring 依赖，上层服务或平级示例基于纯 Java 入口完成框架装配。

## API 冻结方向

业务优先从 `FlyingOrmClients`、FormClient、Repository、DatabaseOperator、`ReactiveSqlExecutor` 和共享条件/批量/Scope 模型进入。默认 options、observer 包装器和数据库专用元数据 reader 已收回包内，避免实现类进入 1.x 兼容承诺。

同步 API 统一使用 `Sync*` 命名并桥接 R2DBC，不再保留容易让人误以为存在 JDBC 双内核的早期命名别名。

## 已知发布边界

- MySQL、PostgreSQL/pgvector、H2、Oracle 和 SQL Server 的目标环境认证已经完成；Oracle、SQL Server 按已认证的保守版本边界正式支持。
- 三轮真实性能和 JVM 基线已经归档，所有场景零业务错误且连接全部归还；MySQL `updateById` P99 相对基线仍超过 10% 硬线，因此性能门禁保持阻断。
- 原生 Linux 或独立磁盘交叉验证已按当前决定跳过，不得把缺少证据改写成性能通过。
- 本项目当前不添加项目许可证。公开源码不等于自动授予复制、修改、分发或商业使用权，使用方需要自行确认授权边界。
- 本次只把源码提交到 GitHub，不发布 Maven 仓库制品，不配置产物签名或发布凭据。
- 平级 `flying-orm-example` 已完成 `1.0.0` 上层集成验收，5 个轻量测试全部通过；最终构建证据在 `v1.0.0` 标签前重新生成。

项目已经提供显式 `audit` Profile 和无数据库 JMH JSON 比较器。许可证聚合报告没有未知第三方许可证，但漏洞数据库尚未完成正式同步，因此不能把 Profile 存在或跳过漏洞库的构建写成审计通过。

候选发布构建使用 `mvn -Prelease-artifacts -DskipTests verify`。默认 Enforcer 会检查 Java 21、Maven 3.9+ 和依赖收敛，发布 Profile 还会检查直接依赖声明，并为各 Java 模块生成源码包和 Javadoc 包。

根 POM 已补齐项目 URL、SCM、开发者和问题跟踪信息。按照当前发布决定，根 POM 不声明项目许可证、发布仓库和产物签名配置。

已知限制见 [known-limitations.md](known-limitations.md)，完整放行条件见 [v1-release-checklist.md](v1-release-checklist.md)。
