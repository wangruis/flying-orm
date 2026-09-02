# flying-orm

flying-orm 是一个为运行时动态表单而生、同时提供实体 Repository 的轻量级 Java ORM。它把表单、条件、Scope、分页、JOIN 和写入规格编译为安全的参数化 SQL，并通过原生 JDBC 或 R2DBC 执行。

项目坚持简单、易用、稳定、安全、开箱即用；在正确性和可维护性成立后，追求高性能、高并发、高吞吐和低延迟。

## 要求与边界

- Java 21、Maven 3.9 或更高版本。
- 上层应用提供 JDBC `DataSource`、R2DBC `ConnectionFactory` 或两者。
- 上层应用选择并配置数据库驱动、连接池、凭据、路由和事务管理器；flying-orm 不实现连接池。
- 支持 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 的既有方言能力。发版认证结果以当前 3.1.0 的实际门禁报告为准。

## 添加依赖

`flying-orm-rdb` 会传递引入 `flying-orm-core`。数据库驱动和连接池由上层应用单独声明。

```xml
<dependency>
    <groupId>io.github.wangruis</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>3.1.0</version>
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

## 正式能力导航

下列能力全部属于 flying-orm 3.1.0 的正式能力；分组只用于阅读导航。

- [常用正式能力](CAPABILITIES.md)：分页、游标分页、轻量 JOIN、结构化条件、Scope、批量、Repository、Schema、字段加密、保护搜索和脱敏。
- [专业正式能力](ADVANCED-CAPABILITIES.md)：DatabaseOperator、SQL 模板、受控原生 SQL、外部事务、超时、观测、缓存、方言和类型扩展。
- [PERF31 后本机 PostgreSQL 性能、历史最优与 10 万逻辑并发报告](docs/superpowers/plans/2026-09-01-flying-orm-local-postgresql-performance-after-perf31.md)：标准五轮通过；8 个 ORM 场景相对历史同协议最优为 5 升 3 降，查询路径退化；单机池 16 的 10 万逻辑并发未通过。
- [最新全仓性能复审](docs/superpowers/plans/2026-09-01-flying-orm-performance-audit-after-perf31.md)：覆盖当前 683 个生产文件；当前账本 35 FIXED、0 OPEN、0 DEFERRED，独立复核 ACCEPT；不代表实库吞吐或并发容量认证。
- [PERF30 / PERF31 修复与验证报告](docs/superpowers/plans/2026-09-01-flying-orm-perf30-31-repairs.md)：关闭最新审查确认的查询形状成员扫描和保护迁移 Map 重扫；记录 TDD、独立复核、1,075 项完整门禁及 API/ABI、制品证据。
- [PERF30 / PERF31 修复前的全仓性能审查](docs/superpowers/plans/2026-09-01-flying-orm-performance-audit-after-perf29.md)：保留两项问题发现时的生产入口、扫描计数和验证限制；当前状态以上方最新复审及修复报告为准。
- [PERF25/28/29 修复与验证报告](docs/superpowers/plans/2026-09-01-flying-orm-perf25-28-29-repairs.md)：响应式独立分片生命周期与两处宽表编译扫描的修复、独立复核、质量门禁和 API/制品证据；这是上一轮修复记录，后续新增问题见上方审查。
- [上轮修复前的全仓性能审查](docs/superpowers/plans/2026-09-01-flying-orm-whole-performance-review.md)：覆盖 683 个生产文件，记录当时确认的 3 个开放根因（1 项 P1、2 项 P2）；这三项的后续处理以上方修复报告为准。
- [前轮修复与验证报告](docs/superpowers/plans/2026-09-01-flying-orm-perf24-27-repairs.md)：保留 PERF24–27 当轮修复、主键清理边界修复及 1031 项测试/一次完整质量门禁的历史证据；已修 JDBC 路径保持 FIXED，后续响应式遗漏以最新审查为准。
- [前轮修复前的并发、吞吐与低延迟性能审查](docs/superpowers/plans/2026-09-01-flying-orm-throughput-latency-review.md)：保留 682 个生产文件的覆盖登记及当时发现的 4 项问题（2 项 P2、2 项 P3）；当前状态以上方最新报告为准。
- [前轮全仓性能与过度防御复审](docs/superpowers/plans/2026-09-01-flying-orm-comprehensive-review.md)：保留当轮无新增问题的历史结论与覆盖证据；后续新增量化证据见上述性能审查，既有 27 项 FIXED 未重开。
- [前轮修复与验证报告](docs/superpowers/plans/2026-09-01-flying-orm-perf21-23-repairs.md)：关闭当轮 PERF21–23 三项问题；990 项测试及完整质量门禁通过。保留容量修复的分配权衡、内部 API 增量及当时的验证范围。
- [修复前的完整性能与过度防御审查](docs/superpowers/plans/2026-09-01-flying-orm-overdefense-sweep.md)：保留 682 个生产文件的覆盖证据，以及当时发现的三项 P3 问题；当前修复状态以上述最新报告为准。
- [此前修复与验证报告](docs/superpowers/plans/2026-08-31-flying-orm-overdefense-audit-fixes.md)：关闭当轮四项问题，956 项测试及完整质量门禁通过；保留本轮修复前的验证基线。
- [修复前的性能与过度防御审查](docs/superpowers/plans/2026-08-31-flying-orm-overdefense-audit.md)：保留问题发现时的调用链、公开 SPI 复现、覆盖和当时结论。
- [上轮修复与验证报告](docs/superpowers/plans/2026-08-31-flying-orm-overdefense-repair.md)：保留当轮四项修复与 934 项测试及完整质量门禁通过的证据；不代表后续审查没有新问题。
- [此前审查与证据边界](docs/superpowers/plans/2026-08-31-flying-orm-overdefense-review.md)：保留此前问题发现时的快照、证据及历史裁决。

## 默认安全行为

- 标识符经过受控解析，业务值使用 JDBC/R2DBC 参数绑定。
- update、delete、Scope、租户、逻辑删除和乐观锁在统一 SQL 计划中组合。
- 外部条件树、批量、LOB、日志和缓存都有明确边界；不会通过无限收集换取表面易用。
- 只有注解或 `DynamicForm` 显式声明的字段才启用加密、保护搜索或脱敏。
- 密钥材料由上层服务提供和管理，flying-orm 只持有必要的内存副本并执行字段保护。

## 从源码构建

```powershell
& 'D:\apache-maven-3.9.16\bin\mvn.cmd' `
  '-Dmaven.repo.local=D:\MavenRepository' `
  '-pl' 'flying-orm-core,flying-orm-rdb' `
  '-am' verify
```

## License

flying-orm 使用 [Apache License 2.0](LICENSE)。
