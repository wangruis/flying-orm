# flying-orm

flying-orm 是一个为运行时动态表单而生、同时提供实体 Repository 的轻量级 Java ORM。它把表单、条件、Scope、分页、JOIN、聚合和写入规格编译为安全的参数化 SQL，并通过原生 JDBC 或 R2DBC 执行。

项目坚持简单、易用、稳定、安全、开箱即用；在正确性和可维护性成立后，追求高性能、高并发、高吞吐和低延迟。

## 要求与边界

- Java 21、Maven 3.9 或更高版本。
- 上层应用提供 JDBC `DataSource`、R2DBC `ConnectionFactory` 或两者。
- 上层应用选择并配置数据库驱动、连接池、凭据、路由和事务管理器；flying-orm 不实现连接池、数据源路由或事务管理器。
- 支持 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 的已声明方言能力。静态 SQL/能力合同与真实数据库往返认证是两类证据，未运行实库门禁时不声称已认证。

## 添加依赖

`flying-orm-rdb` 会传递引入 `flying-orm-core`。数据库驱动和连接池由上层应用单独声明。

```xml
<dependency>
    <groupId>io.github.wangruis</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>3.3.0</version>
</dependency>
```

## 五分钟上手

### 1. 创建客户端

响应式应用传入 R2DBC `ConnectionFactory`：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory).build();
ReactiveFormClient forms = clients.forms();
```

同步应用传入 JDBC `DataSource`：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).build();
SyncFormClient forms = clients.syncForms();
```

同时使用 JDBC 与 R2DBC 时，可以把两者一起交给 `FlyingOrmClients.builder(dataSource, connectionFactory)`。应用停止时调用 `clients.close()`；关闭客户端不会代替上层关闭连接池。

### 2. 定义 DynamicForm

```java
DynamicForm userForm = DynamicForm.builder("user", "app_user")
        .addField(DynamicField.primaryKey("id", "varchar(64)"))
        .addField(DynamicField.of("name", "varchar(100)").withNullable(false))
        .addField(DynamicField.of("created_at", "timestamptz"))
        .build();
```

`DynamicForm` 是不可变的运行时表模型。字段名、数据库类型、主键、租户、逻辑删除以及显式字段保护都从这里进入统一 SQL 管线。

实体中不对应数据库列的计算属性继续使用 `@TableField(exist = false)` 或 Java `transient`；flying-orm 不再发明一套重复注解。这些属性不进入读取、插入、更新、批量或 Schema 列计划；同时声明列或约束注解会被当作配置错误。

实体也可以作为完整期望关系模型的唯一来源：`@TableName` / `@TableCatalog` 声明表身份，`@TableComment` 声明表注释，
`@TableColumn` 声明列结构与列注释，`@TablePrimaryKey`、`@TableUnique`、`@TableIndex`、
`@TableForeignKey`、`@TableCheck` 和 `@TablePartition` 声明受控关系结构；当前分区原语只支持 PostgreSQL 单列时间 `RANGE`。显式调用
`EntitySchemaSynchronizer.synchronizeRelational(...)` 或响应式入口后，flying-orm 才会执行
“注解编译 → Schema diff → 精确 SQL 审阅 → 前置条件复核 → DDL → 执行后回读验证”；普通 Repository/CRUD 不会自动进入这条冷路径。
自动执行要求元数据读取器明确声明能够完整回读所有被比较的结构事实。内置 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 读取器均提供完整关系快照，覆盖表与列、PK、UK、索引、FK、CHECK、默认值、生成方式及注释；第三方读取器若只声明部分 coverage，审阅阶段会返回人工步骤和零 SQL，不会先执行 DDL 再把未知事实误报为成功。

`@EncryptedField` 的密文列、EXACT/SUFFIX 搜索列和按需的 CONTAINS 辅助表由 ORM 从同一实体描述投影到最终物理关系，并与 CRUD、指纹、DDL、回读和差异共用一条链路。只有能够保持语义的唯一约束和等值索引才会投影；主键、外键、分区键、范围约束或不安全的复合保护索引会在 SQL 发送前明确拒绝。未启用保护的普通实体不增加 CRUD 热路径成本。

### 3. 查询与写入

```java
ConditionGroup byId = ConditionGroup.and()
        .where("id", "=", "u-1001")
        .build();

Mono<Long> inserted = forms.insert(WriteSpec.insert(userForm, Map.of(
        "id", "u-1001",
        "name", "Alice",
        "created_at", Instant.now()
)));

Flux<DynamicRow> selected = forms.select(QuerySpec.of(userForm, byId));
```

同步调用使用完全相同的 `DynamicForm`、`ConditionGroup`、`QuerySpec` 和 `WriteSpec`：

```java
long inserted = forms.insert(WriteSpec.insert(userForm, values));
List<DynamicRow> selected = forms.select(QuerySpec.of(userForm, byId));
```

更新和删除必须提供受控条件；SQL 值始终通过参数绑定，不应把业务值拼入 SQL 文本。

### 4. 批量写入

```java
BatchSpec batch = BatchSpec.insert(userForm, Flux.fromIterable(rows))
        .withOptions(BatchWriteOptions.atomic(500));

Mono<BatchWriteResult> result = forms.writeBatch(batch);
```

批量输入按有界分片处理，不要求先把全部数据收集到内存。同步 FormClient 使用同一个 `BatchSpec`。

`maxBufferedBytes` 限制所有在途分片的输入估算重量；每片额度为总预算除以并发数，
`maxRowBytes` 声明单行上限。请求下一行前会预留这份额度，剩余空间不足时先执行当前分片。
默认单行上限为每片额度的一半（至少 1 字节）；`chunkSize = 1` 时使用全部额度。
这约束的是输入估算重量，不是整个 JVM 的堆占用。

```java
BatchWriteOptions options = BatchWriteOptions.atomic(500)
        .withMemoryLimits(100_000, 32L * 1024 * 1024, 4096)
        .withMaxRowBytes(1024 * 1024);
```

`withMemoryLimits` 会重新计算默认单行上限，因此显式的 `withMaxRowBytes` 应放在它之后。
单行上限越接近每片额度，分片可能越早结束；两者相等时每行单独成片，
INDEPENDENT 的事务和回执边界也随之改变，应同时考虑结果分片数限制。
本次新增 record 分量 `maxRowBytes`，直接调用规范构造器的代码需要补齐参数并重新编译。
回执计划升级后，旧计划回执不能作为新计划自动重放；升级前应完成旧任务的恢复确认。

需要区分“SQL 已执行”和“外部事务已提交”时，使用独立证据入口：

```java
Mono<BatchExecutionEvidence> evidence = forms.writeBatchEvidence(batch);
```

`BatchExecutionEvidence` 保留分片位置、执行状态和驱动能够证明的影响行数。外部事务中返回 `PENDING_EXTERNAL`，不等待同一事务完成，也不由 flying-orm 提交、回滚或关闭外部连接。

## 正式能力导航

下列能力属于 `3.3.0` 的公开能力；分组只用于阅读导航。

- [常用正式能力](CAPABILITIES.md)：可空复合 keyset、来源隔离的受治理 JOIN、结构化条件、字段用途、查询预算、类型化聚合、批量执行证据、保护关系投影、Repository 和实体注解 Schema 闭环。
- [专业正式能力](ADVANCED-CAPABILITIES.md)：DatabaseOperator、SQL 模板、受控原生 SQL、外部事务、锁定读取、超时、观测、缓存、方言、保护字段关系模型、PostgreSQL 分区父表和受治理扩展。

## 默认安全行为

- 标识符经过受控解析，业务值使用 JDBC/R2DBC 参数绑定。
- update、delete、Scope、租户、逻辑删除和乐观锁在统一 SQL 计划中组合。
- 外部条件树、批量、LOB、日志和缓存都有明确边界；不会通过无限收集换取表面易用。
- 只有注解或 `DynamicForm` 显式声明的字段才启用加密、保护搜索或脱敏。
- 密钥材料由上层服务提供和管理，flying-orm 只持有必要的内存副本并执行字段保护。

## 从源码构建

```bash
mvn -pl flying-orm-core,flying-orm-rdb -am verify
```

## License

flying-orm 使用 [Apache License 2.0](LICENSE)。
