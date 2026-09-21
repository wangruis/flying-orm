# flying-orm 4.1.0 

> flying-orm `4.1.0` 的项目介绍、接入方式、职责边界和常用示例。

flying-orm 是一个为运行时动态表单而生、同时提供实体 Repository 的轻量级 Java ORM。它把表单、条件、Scope、分页、JOIN、聚合和写入规格编译为安全的参数化 SQL，并通过原生 JDBC 或 R2DBC 执行。

项目坚持简单、易用、稳定、安全、开箱即用；在正确性和可维护性成立后，追求高性能、高并发、高吞吐和低延迟。

## 4.1.0 概览

JDBC/R2DBC 只调用上层连接获取/释放端口，方言显式选择；批量统一返回实际 SQL 执行证据。事务、分片、驱动与连接治理、所有执行时限及回执恢复均归上层。

- **单一编译内核**：DynamicForm、Repository、Schema 和 DatabaseOperator 共用字段、类型、Scope 与参数语义，最终生成 `SqlRequest`；JDBC/R2DBC 保留各自驱动执行方式。
- **结构与类型闭环**：UUID 写入及空值绑定、DATE 回读、受控 Schema CHANGE/DROP、显式 DISTINCT 可空唯一，以及保护字段的物理列与回读验证。
- **查询与写入边界**：同表自关联按来源隔离治理；关系条件可与加密精确过滤组合；批量 UPSERT 在 SQL 内约束已有冲突目标行的 Scope，不再仅靠拒绝整个带 Scope 请求兜底。
- **代码表面积治理**：在不削弱正式能力的前提下合并重复规划、渲染和编排；具体清单与验证见[4.1.0交付记录](docs/reports/2026-09-17-flying-orm-4.1.0-delivery.md)。“轻量”指独立、按需启用、无框架容器依赖。

常用场景与规格对象保留；连接工厂、批量结果及上层职责接口由调用方提供。公共 API 与配置边界见 [专业正式能力](ADVANCED-CAPABILITIES.md#公共-api-与数据库认证)。

## 要求与边界

- Java 21、Maven 3.9 或更高版本。
- 上层应用实现 `JdbcConnectionAccess`、`R2dbcConnectionAccess` 或两者，决定连接来源与释放策略；ORM 不接收或保存 `DataSource` / `ConnectionFactory`。
- 上层应用选择并配置数据库驱动、连接池、凭据、路由和事务管理器；flying-orm 不实现连接池、数据源路由或事务管理器。
- ORM 不探测、参与或管理事务，也不因未配置事务而拒绝普通/保护写入。同连接多语句只借用一次；原子性、提交、回滚和重试完全由上层决定。
- 执行、监听、清理及 Schema 会话锁等待等时间政策归上层；ORM 不保留超时参数、不设置 Statement 超时。成功获取连接后，先清理自有 Statement/结果/LOB，再在完成、失败或取消时调用上层 release 一次；获取失败不调用 release，release 失败仍报告错误。
- 支持 PostgreSQL、MySQL 8.0.16+、Oracle、SQL Server 和 H2 的已声明方言能力。静态 SQL/能力合同与真实数据库往返认证是两类证据，未运行实库门禁时不声称已认证。

## 添加依赖

`flying-orm-rdb` 会传递引入 `flying-orm-core`。数据库驱动和连接池由上层应用单独声明。
以下坐标对应 `4.1.0`。依赖是否可解析由使用方配置的 Maven 仓库决定；POM 中的版本号本身不表示已在任何远程仓库可下载。

```xml
<dependency>
    <groupId>io.github.wangruis</groupId>
    <artifactId>flying-orm-rdb</artifactId>
    <version>4.1.0</version>
</dependency>
```

## 上层接入示例

下面两个例子使用 **Java 21、PostgreSQL、flying-orm 4.1.0**，共用一份 DynamicForm。数据库及账号由应用准备，ORM 不创建数据库或管理凭据。使用方从自己配置的 Maven 仓库解析依赖；示例不假定固定的本机路径或远程发布状态。

示例选择 Spring Boot 4.1.1；普通 Java 不依赖 Spring。这里的 POM 是**使用方项目配置**，不要加进 flying-orm 自身的 POM。已有项目只合并需要的依赖，不要重复添加 parent 或整个 dependencies 块。

### 两个例子共用的表模型

保存为 `src/main/java/example/QuickstartModel.java`。这是唯一的示例表结构定义，建表与 CRUD 都复用它：

```java
package example;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;

public final class QuickstartModel {
    public static final DynamicForm USERS = DynamicForm.relationalBuilder(
            "quickstartUsers", RelationIdentity.of(null, "public", "flying_orm_quickstart"))
            .addField(DynamicField.primaryKey("id", "VARCHAR(64)"))
            .addField(DynamicField.of("name", "VARCHAR(128)").withNullable(false))
            .build();

    private QuickstartModel() {
    }
}
```

示例访问前必须已创建该表。空的演示数据库可用下面普通 Java 程序的 `--init` 模式创建一次，不手写另一份建表 SQL。它不是启动时自动迁移方案：实体应用使用 `clients.entitySchemas()` 的审阅/同步链，普通请求不执行 DDL。

### 例一：Spring Boot 最小接入

使用 WebFlux + R2DBC，数据库访问不阻塞请求线程。Spring Boot 负责按 `spring.r2dbc.*` 创建连接工厂，flying-orm 只需要一个客户端 Bean；不需要 ORM 专属 starter、`@EnableFlyingOrm`、Mapper 扫描或 Spring Data Repository。Boot 4 的基础 R2DBC starter 与 Spring Data starter 分开，参见 [Spring Boot starter 对照](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide#starters) 和 [R2DBC 配置](https://docs.spring.io/spring-boot/4.1/reference/data/sql.html#data.sql.r2dbc)。

**1. 使用方 `pom.xml`：**

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.1</version>
        <relativePath/>
    </parent>
    <groupId>example</groupId>
    <artifactId>flying-orm-boot-demo</artifactId>
    <version>1.0.0</version>
    <properties>
        <java.version>21</java.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>io.github.wangruis</groupId>
            <artifactId>flying-orm-rdb</artifactId>
            <version>4.1.0</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-r2dbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>r2dbc-postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

Boot 管理 starter、驱动及共享依赖的版本；应用升级 Boot/BOM 后仍需验证解析出的依赖组合，不能把本例当作所有 Spring Boot 版本的兼容认证。

**2. `src/main/resources/application.yml`：**

```yaml
spring:
  r2dbc:
    url: ${DB_R2DBC_URL:r2dbc:postgresql://localhost:5432/orm_demo}
    username: ${DB_USER}
    password: ${DB_PASSWORD}
server:
  address: 127.0.0.1
```

启动环境中提供 `DB_USER`、`DB_PASSWORD`，可用 `DB_R2DBC_URL` 覆盖示例地址。不要把生产密码写入源码或 URL；本地演示接口只监听回环地址，真实服务的认证与权限由应用补齐。

**3. `src/main/java/example/BootDemoApplication.java`：**

```java
package example;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BootDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(BootDemoApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    FlyingOrmClients flyingOrmClients(ConnectionFactory connectionFactory) {
        R2dbcConnectionAccess access = new R2dbcConnectionAccess() {
            @Override
            public Publisher<? extends Connection> getConnection(SqlRequest request) {
                return Mono.defer(() -> Mono.from(connectionFactory.create()));
            }

            @Override
            public Publisher<Void> releaseConnection(
                    SignalType signal, Connection connection, SqlRequest request) {
                // 此处是应用的释放策略；ORM 不直接关闭连接。
                return Mono.defer(() -> Mono.from(connection.close()));
            }
        };
        return FlyingOrmClients.builder(access).dialect(RdbDialect.postgresql()).build();
    }
}
```

**4. `src/main/java/example/UserController.java`：**

```java
package example;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/demo/users")
public class UserController {
    private final ReactiveFormClient forms;

    public UserController(FlyingOrmClients clients) {
        this.forms = clients.forms();
    }

    @PostMapping("/{id}")
    public Mono<Long> insert(@PathVariable("id") String id,
                             @RequestParam("name") String name) {
        return forms.insert(WriteSpec.insert(QuickstartModel.USERS,
                Map.of("id", id, "name", name)));
    }

    @GetMapping("/{id}")
    public Flux<DynamicRow> find(@PathVariable("id") String id) {
        var where = ConditionGroup.and().where("id", "=", id).build();
        return forms.select(QuerySpec.of(QuickstartModel.USERS, where));
    }
}
```

执行 `mvn spring-boot:run`，表已创建后可调用：

```bash
curl -X POST 'http://localhost:8080/demo/users/u-1001?name=Alice'
curl 'http://localhost:8080/demo/users/u-1001'
```

首次 POST 返回影响行数 `1`，GET 返回匹配行；重复插入同一主键会报冲突，不是 UPSERT。WebFlux 负责订阅返回的 Publisher，业务方法不要手工 `subscribe()` 或 `block()`。客户端作为单例共享，由 Spring 关闭；连接池仍由 Spring/应用关闭。

这份最小适配器**不接入 Spring 事务**，也不提供跨请求原子性。仅添加 `@Transactional` 不会把事务连接自动交给 flying-orm。需要 Spring 事务时，由应用在 access 的 get/release 中使用相应框架的连接借还规则，替换本例的直接 create/close；ORM 不提供 participant、事务上下文或完成通知 SPI，也不判断连接是否属于事务。

已有 Spring MVC/JDBC 应用可按下一例实现 `JdbcConnectionAccess`，通过 `FlyingOrmClients.builder(access).dialect(RdbDialect.postgresql()).build()` 注册 Bean 并调用 `syncForms()`；不要把 JDBC 同步入口放入 WebFlux 事件循环，也无需额外装配 R2DBC。

### 例二：普通 Java 最小接入

不用 Spring，直接提供 PostgreSQL JDBC 数据源；复用上面的 `QuickstartModel.java`。`PGSimpleDataSource` 不带连接池，适合说明最短接入路径；长期运行、高并发应用应传入自己维护的连接池并负责关闭，参见 [pgJDBC 数据源说明](https://jdbc.postgresql.org/documentation/datasource/)。

**1. 使用方 `pom.xml`：**

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>example</groupId>
    <artifactId>flying-orm-java-demo</artifactId>
    <version>1.0.0</version>
    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <dependency>
            <groupId>io.github.wangruis</groupId>
            <artifactId>flying-orm-rdb</artifactId>
            <version>4.1.0</version>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.13</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.14.1</version>
            </plugin>
        </plugins>
    </build>
</project>
```

这里代码直接使用 `PGSimpleDataSource`，因此 JDBC 驱动使用默认 compile scope，不能写成仅 runtime。驱动版本是示例固定值，升级由使用方核验。

**2. `src/main/java/example/PlainJavaDemo.java`：**

```java
package example;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class PlainJavaDemo {
    public static void main(String[] args) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(System.getenv().getOrDefault("DB_JDBC_URL",
                "jdbc:postgresql://localhost:5432/orm_demo"));
        dataSource.setUser(System.getenv("DB_USER"));
        dataSource.setPassword(System.getenv("DB_PASSWORD"));

        JdbcConnectionAccess access = new JdbcConnectionAccess() {
            @Override
            public Connection getConnection(SqlRequest request) throws SQLException {
                return dataSource.getConnection();
            }

            @Override
            public void releaseConnection(Connection connection, SqlRequest request) throws SQLException {
                // 这是应用拥有的释放策略；池连接可在此归还，固定借用连接可不关闭。
                connection.close();
            }
        };
        try (FlyingOrmClients clients = FlyingOrmClients.builder(access)
                .dialect(RdbDialect.postgresql()).build()) {
            if (args.length == 1 && "--init".equals(args[0])) {
                // 仅用于尚无示例表的演示库；重复建表会明确失败，不删表或覆盖数据。
                clients.syncSchema().createTable(QuickstartModel.USERS);
                return;
            }

            var forms = clients.syncForms();
            String id = UUID.randomUUID().toString();
            long inserted = forms.insert(WriteSpec.insert(QuickstartModel.USERS,
                    Map.of("id", id, "name", "Alice")));
            var where = ConditionGroup.and().where("id", "=", id).build();
            var rows = forms.select(QuerySpec.of(QuickstartModel.USERS, where));
            System.out.println("inserted=" + inserted + ", rows=" + rows);
        }
    }
}
```

设置 `DB_USER`、`DB_PASSWORD`，需要时设置 `DB_JDBC_URL`。用 IDE 导入 Maven 项目：首次向该空演示库运行 `PlainJavaDemo.main` 时传入 `--init`，之后不带参数运行即可插入并查询；也可先执行 `mvn compile` 检查项目编译。两个示例连接同一数据库时，只需初始化一次。

`try-with-resources` 释放 ORM 自身资源；`PGSimpleDataSource` 本身没有池可关。每次操作结束后 ORM 调用上层 release，本例由应用在该方法中关闭连接。若替换为连接池，借还和池关闭策略仍由应用负责；ORM 不直接关闭外部 Connection。示例插入与查询是两个独立操作，不构成跨语句事务。

这两个例子不更改 ORM 的依赖边界：Spring Boot、数据库驱动、连接池与事务适配只存在于使用方。加密密钥、Scope、Schema 自动同步等按需配置，不是基础 CRUD 的必填项。

示例是接入形状说明，不构成外部数据库或完整应用认证。

## 五分钟上手

### 1. 创建客户端

响应式应用传入上面实现的 `R2dbcConnectionAccess reactiveAccess`：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(reactiveAccess)
        .dialect(RdbDialect.postgresql()).build();
ReactiveFormClient forms = clients.forms();
```

同步应用传入上面实现的 `JdbcConnectionAccess jdbcAccess`：

```java
FlyingOrmClients clients = FlyingOrmClients.builder(jdbcAccess)
        .dialect(RdbDialect.postgresql()).build();
SyncFormClient forms = clients.syncForms();
```

方言必须显式选择；版本化方言可传入 `RdbDialect.oracle(OracleVersion.V12C)`，不会打开连接或读取工厂 metadata 来探测。同时使用两端口时，`FlyingOrmClients.builder(jdbcAccess, reactiveAccess)` 同样必须配置方言。应用停止时调用 `clients.close()`，关闭客户端不会代替上层关闭连接池。

### 2. 定义 DynamicForm

```java
DynamicForm userForm = DynamicForm.builder("user", "app_user")
        .addField(DynamicField.primaryKey("id", "varchar(64)"))
        .addField(DynamicField.of("name", "varchar(100)").withNullable(false))
        .addField(DynamicField.of("created_at", "timestamptz"))
        .build();
```

`DynamicForm` 是不可变的运行时表模型。字段名、数据库类型、主键、租户、逻辑删除以及显式字段保护都从这里进入统一 SQL 管线。

`RelationIdentity` 不从点号字符串猜测 catalog、schema 和 table。`RelationIdentity.table("accounts.v2")`
表示表名本身含点号；需要限定关系时应使用 `RelationIdentity.of(catalog, schema, table)`。关系元数据读取、缓存键和失效路径会按三个身份段精确传递。

实体中不对应数据库列的计算属性继续使用 `@TableField(exist = false)` 或 Java `transient`；flying-orm 不再发明一套重复注解。这些属性不进入读取、插入、更新、批量或 Schema 列计划；同时声明列或约束注解会被当作配置错误。

实体也可以作为完整期望关系模型的唯一来源：`@TableName` / `@TableCatalog` 声明表身份，`@TableComment` 声明表注释，
`@TableColumn` 声明列结构与列注释，`@TablePrimaryKey`、`@TableUnique`、`@TableIndex`、
`@TableForeignKey`、`@TableCheck` 和 `@TablePartition` 声明受控关系结构；当前分区原语只支持 PostgreSQL 单列时间 `RANGE`。显式调用
`EntitySchemaSynchronizer.synchronizeRelational(...)` 或响应式入口后，flying-orm 才会执行
“注解编译 → Schema diff → 精确 SQL 审阅 → 前置条件复核 → DDL → 执行后回读验证”；普通 Repository/CRUD 不会自动进入这条冷路径。
自动执行要求元数据读取器明确声明能够完整回读所有被比较的结构事实。内置 PostgreSQL、MySQL、Oracle、SQL Server 和 H2 读取器在各自已声明且可表示的版本与结构范围内提供 complete coverage，覆盖表与列、PK、UK、索引、FK、CHECK、默认值、生成方式及注释；Oracle 12c 等部分回读配置保留明确缺口。内置或第三方读取器若只有部分 coverage，审阅阶段会返回人工步骤和零 SQL，不会先执行 DDL 再把未知事实误报为成功。

Java `UUID` 实体属性统一编译为逻辑 `UUID`，默认采用 PostgreSQL/H2 原生 `UUID`、MySQL `CHAR(36)`、Oracle `VARCHAR2(36)`、SQL Server `UNIQUEIDENTIFIER`；写入、条件绑定和回读使用对应载体，显式文本 codec 的优先级不变。关系同步支持各内建方言已声明的受控 CHANGE/DROP 与显式 ABSENT 目标；不安全变更仍进入人工步骤，具体边界见 [Schema 能力](CAPABILITIES.md#schema-与元数据)。

Oracle 外键替换可通过审核计划执行：`requiresWritesQuiesced()` 标明相关写入必须静止，
`SchemaMigrationApproval.approveWithWritesQuiesced(plan, reason)` 显式确认后，执行冻结 DDL 并回读验证。
新建且尚无写入者的数据库也满足该前提；ORM 不负责停写、补偿或集群协调，普通批准不会自动放行。

需要“任一键为 NULL 可以共存、全部非空时唯一”时，可显式声明 `@TableUnique(nullPolicy = UniqueNullPolicy.DISTINCT, ...)` 并使用 relational Schema 入口。PG/MySQL 使用等价原生 UNIQUE，H2 显式指定 NULLS DISTINCT，SQL Server 使用受控唯一过滤索引，Oracle 使用受控 CASE 唯一索引；旧注解默认行为不变，不开放任意索引表达式。

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

普通批量顺序执行，只在订阅后消耗响应式输入，不要求先把全部数据收集到内存：

```java
BatchSpec batch = BatchSpec.insert(userForm, Flux.fromIterable(rows))
        .withOptions(BatchWriteOptions.of(500));

Mono<BatchExecutionEvidence> result = forms.writeBatch(batch);
```

同步 FormClient 使用同一个 `BatchSpec`，直接返回 `BatchExecutionEvidence`。一次非空调用只借用一条上层连接；空输入不获取连接。内部按缓冲容量、参数和内存限制执行，不公开 chunks、并发分片或事务模式。ORM 不探测或要求事务；需要整批或保护多语句原子性时，由上层在 access 外围决定事务边界。

`BatchSpec.upsert(...).withScope(...)` 支持范围内冲突更新：默认与显式 Scope 一并检查已有目标行，范围外更新明确失败。没有冲突的新行仍遵循 INSERT 规则；JDBC、R2DBC 和 Repository 共用同一计划。乐观锁批量 UPDATE 保留版本谓词及逐行 `EXACTLY_ONE` 冲突事实。

`BatchWriteOptions` 的四个 record 成分为 `bufferSize`、`maxRows`、`maxBufferedBytes`、`maxRowBytes`。默认缓冲行数 500、总输入上限 100,000、输入估算预算 32 MiB。`maxRows=0` 仅表示不限总输入，缓冲仍有界。`maxRowBytes` 必须为正且不超过缓冲预算；请求下一行前预留这份额度，空间不足先执行当前缓冲。默认单行额度为缓冲预算的一半，`bufferSize=1` 时使用全部预算。这是输入估算重量，不是整个 JVM 堆上限。

```java
BatchWriteOptions options = BatchWriteOptions.of(500)
        .withMemoryLimits(100_000, 32L * 1024 * 1024)
        .withMaxRowBytes(1024 * 1024);
```

`withMemoryLimits` 会重新计算默认单行上限，因此显式 `withMaxRowBytes` 放在它之后。单行额度越大，缓冲可能越早执行；不改变任何上层事务范围。旧模式工厂、结果分片配置、回执和恢复接口已删除，迁移表见[公共 API 与数据库认证](ADVANCED-CAPABILITIES.md#公共-api-与数据库认证)。

已有证据别名进入同一执行核心，不会执行第二次或返回另一套结果：

```java
Mono<BatchExecutionEvidence> evidence = forms.writeBatchEvidence(batch);
```

两段示例是二选一的调用方式；不要分别订阅来重复写入。`BatchExecutionEvidence` 提供已接收输入数、已证明成功/失败的绝对 long 位置、影响行数、冲突和安全失败摘要。`BatchAffectedRows` 区分 KNOWN 与 UNKNOWN；未证明的位置不伪造失败，未知计数不当作零。成功前缀以 long 保存，终止缓冲的位置有界，`successfulOffsets()` / `failedOffsets()` 惰性遍历而不收集全批列表。

失败时保留已形成的 SQL 事实；通常由 `BatchExecutionEvidenceException` 携带，Repository 的 POST 失败保持 `EntityPostWriteException` 并附带证据。PRE 在读取实体参数前执行；生成键应用、必要辅助 SQL 与语句资源清理完成后才触发该行 POST。同一已完成工作范围内的普通 POST 错误会汇聚，其后停止新工作。主表 SQL 成功不等于生成键/辅助关系/POST 全部成功，不能用成功位置重放 POST；已应用键也不会因后续失败或取消自动恢复。证据不表达已提交、已回滚或等待上层确认，恢复和重试由上层裁决。

## 正式能力导航

下列能力属于当前 `4.1.0` 源码的公开能力；分组只用于阅读导航。

- [常用正式能力](CAPABILITIES.md)：可空复合 keyset、来源隔离的受治理 JOIN（含同表自关联）、结构化条件、字段用途、查询预算、类型化聚合、批量执行证据、保护关系投影、Repository 和实体注解 Schema 闭环。
- [专业正式能力](ADVANCED-CAPABILITIES.md)：DatabaseOperator、SQL 模板、受控原生 SQL、上层连接与事务边界、锁定读取、上层时限接入边界、观测、缓存、方言、保护字段关系模型、PostgreSQL 分区父表和受治理扩展。

## 默认安全行为

- 标识符经过受控解析，业务值使用 JDBC/R2DBC 参数绑定。
- update、delete、Scope、租户、逻辑删除和乐观锁在统一 SQL 计划中组合。
- 外部条件树、批量、LOB、日志和缓存都有明确边界；不会通过无限收集换取表面易用。
- 只有注解或 `DynamicForm` 显式声明的字段才启用加密、保护搜索或脱敏。
- 密钥材料由上层服务提供和管理，flying-orm 只持有必要的内存副本并执行字段保护。

## 从源码构建

以下命令在仓库根目录执行，使用应用环境配置的 Maven 和本地仓库，无需固定机器路径：

```bash
mvn -pl flying-orm-core,flying-orm-rdb -am verify
```

完整质量门禁使用 `mvn -Pquality -pl flying-orm-core,flying-orm-rdb -am verify`。构建验证不等于安装或发布；需要更新本地 Maven 制品时再执行相应的 `install`。

本地构建只验证当前源码；依赖的远程可用性由使用方配置的 Maven 仓库决定。

## License

flying-orm 使用 [Apache License 2.0](LICENSE)。

更多代码片段见 [EXAMPLES.md](EXAMPLES.md)，实体注解见 [ANNOTATIONS.md](ANNOTATIONS.md)。
