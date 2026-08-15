# 上层 Spring Boot 自动配置边界

## 状态

已对齐。

## 结论

`flying-orm` 本体不依赖 Spring，不放 Spring Boot 自动配置，也不提供 starter。

如果上层服务使用 Spring Boot，可以直接在上层服务里做自动配置；平级 `flying-orm-example` 负责验证这套接法。
适配代码依赖 `flying-orm-rdb`，再把上层已有的 `ConnectionFactory`、配置项和业务扩展装配成 flying-orm 客户端。

## 推荐装配对象

上层适配器可以自动装配这些对象：

- `RdbDialect`
- `SqlRenderer`
- `ReactiveSqlExecutor`
- `ReactiveFormClient`
- `ReactiveSchemaClient`
- `ReactiveFormMetadataReader`
- `SyncFormClient`
- `SyncSqlExecutor`
- `DatabaseOperator`
- `SyncDatabaseOperator`

## 方言自动装配

业务代码不应该显式创建方言，也不应该自己写：

```java
ReactiveFormClient forms = ReactiveFormClient.create(
        executor,
        FormDataSqlRenderer.create(renderer, dialect));
DatabaseOperator operator = DatabaseOperator.create(executor, renderer, dialect);
```

这些都是自动配置层的职责。推荐装配顺序：

1. 如果配置了 `flying-orm.dialect`，优先使用它。
2. 否则根据 `spring.r2dbc.url` 解析 R2DBC driver/protocol。
3. 如果没有 URL，再根据 `ConnectionFactory.getMetadata().getName()` 推断。
4. 如果仍然识别不了，启动时报清楚错误，不偷偷默认成某个数据库。

`flying-orm-rdb` 只提供纯 Java 的 `RdbDialectResolver` 和 `FlyingOrmClients.builder(ConnectionFactory)`。
上层服务通过 Builder 映射方言覆盖、执行保护、批量策略、事务参与者和日志 observer，再把构建结果拆成 Bean。

业务服务的推荐路径是继承式 service：具体业务类不写 client/operator/context 构造器，只继承父类后直接 `select(...)`、`createUpdate()`。直接注入 `ReactiveFormClient`、`DatabaseOperator` 仍然保留给管理后台、动态表单设计器和高级扩展场景。

## 边界

- 自动配置只属于上层项目或独立适配项目，不进入 `flying-orm-core`、`flying-orm-rdb`、`flying-orm-testkit`、`flying-orm-benchmark`。
- flying-orm 本体继续只暴露纯 Java 工厂方法，保证任何框架、无框架、命令行程序都能使用。
- 上层服务管理事务、动态数据源和框架生命周期；flying-orm 通过统一事务参与契约复用上层绑定连接，保证两边
  对同一次提交、回滚和连接所有权的认识一致。
- 上层需要感知 flying-orm 内部批量事务时，应根据 `BatchWriteResult`、`BatchChunkResult` 或 `BatchWriteException.result()` 处理业务补偿和提示。

## 后续实现位置

上层业务服务可以维护自己的 `FlyingOrmAutoConfiguration`。这层代码可以做到开箱即用，但不进入 flying-orm
仓库，也不改变主项目的无框架依赖原则。

## 外部验证项目

已新增与 `flying-orm` 平级的 `D:\new_code\flying-orm-example`，用来模拟真实上层服务通过 Maven 依赖使用 flying-orm。

当前 example 已跑通：

- 纯 Java 手动组装
- H2 R2DBC 动态建表
- 动态表单写入
- 前端结构化条件查询
- 同步桥接查询
- `DatabaseOperator` 查询

后续上层 Spring Boot 自动配置可以继续在这个平级项目里扩展验证。

已补充 Boot 配置文档和最小注入调用示例：

- `D:\new_code\flying-orm-example\docs\spring-boot-configuration.md`
- `D:\new_code\flying-orm-example\src\test\java\com\flying\orm\example\boot\FlyingOrmBootConfigurationUsageTest.java`

该示例覆盖动态表单、前端结构化 JSON 条件、`user-in-org` 业务 term、Boot 容器配置、`ReactiveFormClient` 注入调用，以及继承式 service 直接 `select(...)` / `createUpdate()` 的业务写法。
