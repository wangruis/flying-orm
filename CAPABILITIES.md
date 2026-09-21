# flying-orm 4.1.0 能力

flying-orm 是面向动态表单、实体 Repository 的轻量 Java ORM。核心流程是：Java 语义 → 参数化 SQL → JDBC/R2DBC 执行 → 结果映射。

## 正式能力

| 能力 | 说明 |
| --- | --- |
| 动态表单与实体读写 | 运行时表结构、查询、插入、更新、删除 |
| 条件与安全 | 参数驱动、可扩展条件、标识符校验、无 SQL 注入 |
| 数据治理 | Scope、租户、逻辑删除、乐观锁、字段用途治理 |
| 关系查询 | JOIN、同表自关联、分页、游标、聚合 |
| 批量写入 | 批量增删改、UPSERT、范围内更新、保护批量、执行证据 |
| 实体结构同步 | 注解建模、Schema 审核、迁移计划、DDL、回读验证 |
| 类型与主键 | 生成键、回填、UUID、日期、数组和自定义类型映射 |
| 字段保护 | 加密、EXACT/SUFFIX/CONTAINS 检索、脱敏 |
| 执行模式 | JDBC 同步、R2DBC 响应式、取消、背压、资源清理 |

## 查询

查询支持字段投影、条件组合、排序、分页、旧游标、JOIN、同表自关联、聚合和锁定读取。
条件通过结构化 API 构造，业务值始终作为参数绑定；动态表名、列名和排序字段必须来自受控映射。
Scope、租户、逻辑删除和字段用途策略在 SQL 计划阶段统一合并。

```java
var condition = ConditionGroup.and().where("status", "=", "ACTIVE").build();
forms.select(QuerySpec.of("users", condition));
```

## 写入与批量

单条和批量写入共用字段映射、codec、Scope 和保护规则。批量支持 INSERT、UPDATE、DELETE、UPSERT、生成键回填及保护字段辅助关系维护。
带 Scope 的 UPSERT 在冲突更新 SQL 内核验原目标行；范围外目标不会被更新。批量结果只报告已接收输入、已证明位置、影响行数和冲突等实际执行事实，不报告事务提交状态。

## Schema

实体注解和关系模型可生成表、列、主键、唯一约束、索引、外键、CHECK、注释、默认值、生成方式及受控分区声明。
Schema 流程为：读取快照 → 生成差异 → 风险审核 → 执行前核验 → 执行 → 回读验证。普通 CRUD 不自动建表。
五方言保留各自 SQL 差异；无法安全表达的变更在发送 SQL 前拒绝，不降级为错误结构。

## 字段保护

显式声明 `@EncryptedField`、`@MaskedField` 或 DynamicForm 保护字段后，可使用密文列、EXACT/SUFFIX/CONTAINS 检索列、辅助关系和脱敏展示。
保护声明同时进入 CRUD、批量、Schema、回读和差异链路。密钥由上层提供，未声明字段不会自动加密或脱敏。

## 安全与资源

- 标识符经过白名单/方言规则校验，业务值不拼接进 SQL。
- R2DBC 保持冷 Publisher、取消和背压；不隐藏 `subscribe` 或 `block`。
- ORM 只清理自己创建的 Statement、ResultSet、LOB；外部 Connection 的关闭和归还由上层完成。
- SQL 日志默认不输出敏感值；异常保留结构化分类和实际执行事实。

## 职责边界

flying-orm 不提供或感知：

- 事务及事务结果恢复
- 分片、路由、归并和分片事务
- 连接池、DataSource/ConnectionFactory 生命周期
- 具体数据库驱动的选择、凭据、健康检查、重连
- 连接、事务、执行和 Schema 会话锁等待的超时策略

连接由上层通过 JDBC/R2DBC 获取与释放。ORM 只清理自己创建的 Statement、ResultSet、LOB 等语句级资源。

## 使用入口

- [README](README.md)：快速接入和示例
- [专业能力](ADVANCED-CAPABILITIES.md)：Operator、Schema、缓存和 API 细节
- [示例](EXAMPLES.md)：DynamicForm、Repository、批量、Schema 和两种执行入口
- [实体注解](ANNOTATIONS.md)：映射、关系、分区和字段治理
