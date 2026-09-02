# flying-orm 常用正式能力

本页承接 [README](README.md) 的 DynamicForm 主路径。这里列出的能力全部是 flying-orm 3.1.0 的正式能力，不是试验功能或可被发版省略的附加项。

## 分页与游标分页

`ReactiveFormClient` 和 `SyncFormClient` 都支持普通分页与游标分页。查询条件、Scope、投影、排序和敏感字段显示策略仍来自同一个 `QuerySpec`，不会形成第二套 SQL 语义。

```java
QuerySpec query = QuerySpec.of(userForm, where)
        .withSorts(List.of(PageSort.asc("id")));

Mono<PageResult<DynamicRow>> page = forms.page(query, PageQuery.of(1, 50));
```

游标必须与稳定排序字段匹配。大数据量连续读取优先使用游标分页，普通跳页使用 offset 分页。

## 轻量 JOIN

JOIN 使用不可变 `JoinQuerySpec`，支持 `JoinType` 当前声明的 INNER、LEFT 和 RIGHT。源级租户、Scope 和逻辑删除条件在各自数据源内生效，避免外连接被最终 WHERE 意外收紧。

```java
JoinQuerySpec.Builder join = JoinQuerySpec.builder(userForm);
JoinSource user = join.root();
JoinSource department = join.join(
        JoinType.LEFT, departmentForm, user, "department_id", "id");

JoinQuerySpec query = join
        .select(user, "id")
        .select(user, "name")
        .selectAs(department, "name", "department_name")
        .build();

Flux<DynamicRow> rows = forms.selectJoin(query);
```

JOIN 面向受控等值关联和常规多表读取；复杂数据库专有查询可使用正式的模板或受控原生 SQL 能力。

## 参数条件与结构化条件

- `ConditionGroup`：Java 代码直接构建 AND/OR 条件树。
- `ParameterConditionCompiler`：把预先声明的请求参数规则编译为条件树。
- `StructuredConditionInput`：接收前端结构化条件，并通过字段、operator、深度和容量策略校验。
- `TermRegistry`：注册受控扩展 term；扩展处理器仍必须返回参数化 SQL 片段。

```java
ConditionGroup where = ConditionGroup.and()
        .whereIfPresent("status", "=", status)
        .or(group -> group
                .where("name", "like-ignore-case", keyword)
                .where("code", "like-ignore-case", keyword))
        .build();
```

字段名和 operator 必须来自应用允许的规则；结构化输入不能作为任意 SQL 文本入口。

## Scope、逻辑删除与乐观锁

flying-orm 正式支持 `TenantScope`、`DataScope`、`FieldScope`、`TimeScope`、逻辑删除和乐观锁。这些规则在 SQL 计划阶段组合，而不是查询后在内存中过滤。

- 默认 Scope 可以在客户端装配时声明，请求级 Scope 可以通过 `QuerySpec.withScope` 或 `WriteSpec.withScope` 收紧。
- 逻辑删除由 `DynamicForm.logicDelete(...)` 或实体注解声明。
- 乐观锁写入使用 `OptimisticLockOptions`，影响行数为零时由调用方按业务冲突处理。
- FieldScope 的交集为空表示无字段权限，不会被解释成“全部放行”。

## 批量写入

`BatchSpec` 支持 INSERT、UPSERT 和乐观锁 UPDATE，执行模式包括：

- `ATOMIC`：整批在 flying-orm 自有事务中提交或回滚；外部事务存在时参与外部事务边界。
- `INDEPENDENT`：按分片独立完成，适合允许部分成功并需要分片结果的场景；在不兼容的外部事务边界中会拒绝执行。

批量具有最大行数、分片大小、并发度、内存预算、结果分片数和超时边界。恢复回执用于明确配置的幂等恢复，不会默认改变普通批量语义。该能力当前由 R2DBC 批量执行器提供；JDBC 保留 ATOMIC/INDEPENDENT 批量，并在订阅输入 Publisher 和获取连接前拒绝回执恢复配置。

## 实体映射与 Repository

实体 Repository 与 DynamicForm 共享映射、条件、Scope、逻辑删除、乐观锁、字段保护和执行器，不是另一套 ORM 内核。

```java
ReactiveFormRepository<UserEntity> users = clients.repository(UserEntity.class);
SyncFormRepository<UserEntity> syncUsers = clients.syncRepository(UserEntity.class);
```

响应式 Repository 返回 Reactor 类型；同步 Repository 使用 JDBC。主键类型由实体元数据和 Repository 方法契约解析，不要求再维护独立的运行时表定义。

## Schema 与元数据

Schema 能力包括数据库元数据读取、差异计划、风险审核、显式同步、迁移执行、回滚计划和观测。Schema 计划与执行分离：调用方可以先审查计划，再显式批准高风险操作。

flying-orm 不替代企业迁移平台，也不会在普通 CRUD 热路径自动执行 DDL。生产环境应把 Schema 权限与业务 DML 权限分离。

## 字段加密、保护搜索与脱敏

字段保护只对以下显式声明生效：

- 实体字段上的 `@EncryptedField` 或 `@MaskedField`。
- `DynamicForm.Builder.encrypted(...)` 或 `masked(...)`。

未声明字段不会自动加密、生成搜索 token 或脱敏。上层服务只需提供版本化密钥材料；密钥来源、部署配置和权限体系不进入 flying-orm。

- EXACT：使用字段和租户隔离的搜索 token 进行精确匹配。
- SUFFIX：按声明的后缀长度生成 token，适合手机号后几位等明确需求。
- CONTAINS：按受控长度生成更多 token，索引和写入成本更高，只应在明确需要时启用。
- 脱敏：控制结果展示，不等同于解密授权；调用方通过声明的显示模式选择默认、脱敏或完整显示。

字段保护适用于任意显式声明的业务字段，不绑定手机号或身份证等固定字段类型。

## 错误处理

公开异常和执行结果提供稳定的分类信息；SQL 日志默认不应输出原始敏感值。应用可以接入自己的日志、指标和告警体系，而不需要引入 flying-orm 专属监控运行时。

## 继续阅读

- [五分钟上手](README.md#五分钟上手)
- [专业正式能力](ADVANCED-CAPABILITIES.md)
