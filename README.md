# flying-orm

一个简单、易用、稳定、安全的 ORM 工具，为动态表单而生。

flying-orm 使用 Java 对象描述运行时表结构、查询条件和写入计划，由框架生成参数化 SQL。响应式调用使用原生
R2DBC/Reactor，同步调用使用原生 JDBC；两条执行链共享同一套 DynamicForm、条件 AST、Scope、方言、codec、
事务和错误规则。

## 适用场景

- 运行时创建、维护和查询动态表单。
- 需要一套模型同时覆盖 DDL、CRUD、分页、批量、字段权限和结果解码。
- 既有响应式服务需要真正非阻塞的 R2DBC，或传统/虚拟线程服务需要原生 JDBC。
- 前端只能提交结构化条件，不能提交 SQL。
- 需要租户、数据范围、逻辑删除、乐观锁和资源上限作为统一默认边界。

flying-orm 不依赖 Spring 或其他应用框架，不提供 starter，也不实现连接池。上层应用负责配置、依赖注入、数据源、
连接池、外部事务和调用上下文；flying-orm 负责 ORM 语义和一次数据库操作的安全执行。

## 主路径

普通业务只需要理解四个概念：

```text
FlyingOrmClients
        ↓
DynamicForm
        ↓
ReactiveFormClient / SyncFormClient
        ↓
QuerySpec / WriteSpec / BatchSpec
```

- `FlyingOrmClients`：一次装配数据库连接能力和 ORM 默认配置。
- `DynamicForm`：运行时表模型，是 DDL、CRUD、批量和结果解码的统一底座。
- `ReactiveFormClient` / `SyncFormClient`：同一 FormClient 语义的响应式与同步执行方式。
- `QuerySpec` / `WriteSpec` / `BatchSpec`：不可变的查询、单写和批量计划。

Repository、JOIN、Schema、字段保护、DatabaseOperator、SQL 模板和原生 SQL 都是按需能力，不是完成基本 CRUD 的
前置知识。它们在 [其他能力](CAPABILITIES.md) 和 [高级能力](ADVANCED-CAPABILITIES.md) 中单独说明。

## 环境要求

- Java 21
- Maven 3.9+
- JDBC `DataSource`、R2DBC `ConnectionFactory` 或两者之一

当前源码基线为 `2.0.0`。应用通常只依赖 `flying-orm-rdb`，再按数据库和执行方式选择对应的 JDBC/R2DBC 驱动：

```xml
<dependency>
    <groupId>com.flying.orm</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>2.0.0</version>
</dependency>
```

## 五分钟上手

### 1. 创建 FlyingOrmClients

响应式应用提供 `ConnectionFactory`：

```java
ConnectionFactory connectionFactory = ...;

FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory).build();
ReactiveFormClient forms = clients.forms();
```

同步应用提供 `DataSource`：

```java
DataSource dataSource = ...;

FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).build();
SyncFormClient forms = clients.syncForms();
```

方言默认根据驱动和数据库 metadata 识别。无法识别、显式方言与物理数据库冲突，或同时装配的 JDBC/R2DBC 方言
不一致时，客户端创建失败，不会静默选择错误方言。

### 2. 定义 DynamicForm

```java
DynamicForm userForm = DynamicForm.builder("userForm", "users")
        .addField(DynamicField.primaryKey("id", "BIGINT"))
        .addField(DynamicField.of("name", "VARCHAR"))
        .addField(DynamicField.of("age", "INTEGER"))
        .addField(DynamicField.of("enabled", "BOOLEAN"))
        .addField(DynamicField.of("deleted", "INTEGER"))
        .logicDelete("deleted", 0, 1)
        .build();
```

这份定义会被查询、写入、批量、字段校验、逻辑删除、Schema 和结果解码共同使用，不需要为每条执行链重复建模。

### 3. 使用 FormClient 完成 CRUD

```java
Mono<Long> inserted = forms.insert(WriteSpec.insert(userForm, Map.of(
        "id", 1L,
        "name", "Alice",
        "age", 20,
        "enabled", true)));

Flux<DynamicRow> rows = forms.select(QuerySpec.of(
        userForm,
        ConditionGroup.and()
                .where("enabled", "=", true)
                .where("age", ">", 18)
                .build()));

Mono<Long> updated = forms.update(WriteSpec.update(
        userForm,
        Map.of("name", "Alice Chen"),
        ConditionGroup.and().where("id", "=", 1L).build()));

Mono<Long> deleted = forms.delete(WriteSpec.delete(
        userForm,
        ConditionGroup.and().where("id", "=", 1L).build()));
```

同步代码使用同样的 `DynamicForm`、条件和规格，只把 `ReactiveFormClient` 换成 `SyncFormClient`，返回值对应变为
普通 Java 值或集合。

表单声明逻辑删除后，普通查询会自动排除已删除数据，`delete(...)` 会更新删除标记。只有显式调用
`physicalDelete(...)` 才会真正删除记录。

### 4. 添加分页

```java
QuerySpec query = QuerySpec.of(userForm, ConditionGroup.and()
        .where("enabled", "=", true)
        .build());

Mono<PageResult<DynamicRow>> page = forms.page(
        query,
        PageQuery.of(1, 20, PageSort.asc("id")));
```

需要持续向后读取大结果集时使用游标分页。游标、复合排序和字段可见性边界见
[其他能力：分页与游标分页](CAPABILITIES.md#分页与游标分页)。

### 5. 添加批量写入

```java
Flux<Map<String, Object>> source = Flux.just(
        Map.of("id", 2L, "name", "Bob", "age", 21, "enabled", true),
        Map.of("id", 3L, "name", "Carol", "age", 22, "enabled", true));

BatchSpec batch = BatchSpec.insert(userForm, source)
        .withOptions(BatchWriteOptions.atomic(500));

Mono<BatchWriteResult> result = forms.writeBatch(batch);
```

批量默认使用 ATOMIC。只有业务明确接受分片分别提交时才使用 INDEPENDENT；外部事务、UNKNOWN、回执恢复和容量配置
见 [其他能力：批量写入](CAPABILITIES.md#批量写入)。

## 默认安全行为

- 所有业务值都使用参数绑定，不拼进 SQL。
- 表名、字段名和其他标识符来自结构化模型并由方言校验。
- 更新和删除必须包含明确业务条件；Scope、逻辑删除和乐观锁只能继续收窄范围。
- 普通 `where(...)` 使用严格空值语义；可选搜索项必须显式使用 `whereIfPresent(...)`。
- 查询行数、结果字节、LOB、执行时间、批量规模和缓存都有默认边界；连接等待由上层连接池治理。
- 公共错误不输出凭据、完整连接串、敏感业务值、LOB 或无界驱动消息。
- 检测到外部事务时复用外部事务连接，不重复控制事务。

## 能力导航

### 其他能力

[CAPABILITIES.md](CAPABILITIES.md) 面向常规业务按需使用，包含：

- 分页与游标分页
- 轻量 JOIN
- 参数条件与前端结构化条件
- TenantScope、DataScope、FieldScope、TimeScope
- 逻辑删除与乐观锁
- ATOMIC/INDEPENDENT 批量
- 实体映射与 Repository
- Schema 与元数据
- 字段加密、保护搜索与脱敏

### 高级能力

[ADVANCED-CAPABILITIES.md](ADVANCED-CAPABILITIES.md) 面向复杂查询、基础设施接入和性能治理，包含：

- DatabaseOperator 链式 DML
- 注册 SQL 模板与受控原生 SQL
- JDBC/R2DBC、连接池和外部事务
- 执行兜底、清理、LOB 和批量边界
- SQL 观测、错误分类和缓存治理
- 方言、codec、数组、JSON 和向量扩展
- fetchSize、批大小、连接池和真实数据库性能验证

## 架构边界

```text
应用代码
   ↓
FlyingOrmClients
   ↓
DynamicForm + Query/Write/BatchSpec
   ↓
统一条件、Scope、SQL 渲染、codec 和错误模型
   ↓
原生 R2DBC/Reactor 或原生 JDBC
   ↓
上层提供的 ConnectionFactory / DataSource / 连接池
```

- `flying-orm-core`：DynamicForm、条件 AST、Scope、分页、通用 SQL 模型、codec 和渲染契约。
- `flying-orm-rdb`：JDBC/R2DBC 执行、FormClient、Repository、Schema、Operator、缓存和数据库方言。
- `flying-orm-testkit`：真实数据库兼容、事务故障、取消和资源释放验证工具。
- `flying-orm-benchmark`：JMH 与真实数据库性能入口，不进入运行时模块。

## 数据库支持

- H2：开发和自动测试基线。
- MySQL 8.4：正式支持。
- PostgreSQL：正式支持，包含原生 Array、数组条件和 pgvector。
- Oracle：代码边界覆盖 12c 至 23ai，实库认证使用 Oracle Free 23。
- SQL Server：代码边界覆盖 2012 至 2022，实库认证使用 SQL Server 2022。

代码中存在方言不等于已经完成实库认证。当前证据和限制以
[数据库支持矩阵](docs/database-support-matrix.md) 与 [真实数据库认证方法](docs/real-database-certification.md) 为准。

## 构建与验证

项目的 Maven 和依赖缓存位置由开发环境决定。标准验证命令为：

```shell
# 编译全部模块
mvn -DskipTests package

# 单元测试、覆盖率、Checkstyle 和 SpotBugs
mvn -Pquality clean verify

# 源码包、Javadoc 和发布制品检查
mvn -Prelease-artifacts verify
```

真实数据库认证和性能测试不默认进入普通 Maven 构建，执行方法见
[真实数据库认证方法](docs/real-database-certification.md)。

## 进一步阅读

- [其他能力](CAPABILITIES.md)
- [高级能力](ADVANCED-CAPABILITIES.md)
- [V2.0.0 纯 Java 接入示例](docs/v2.0.0-java-integration.md)
- [V2.0.0 JDBC/R2DBC 双执行契约](docs/v2.0.0-execution-contract.md)
- [错误码手册](docs/error-code-reference.md)
- [公共 API 稳定边界](docs/public-api-stability.md)
- [数据库支持矩阵](docs/database-support-matrix.md)
- [真实数据库认证方法](docs/real-database-certification.md)
- [已知限制](docs/known-limitations.md)
