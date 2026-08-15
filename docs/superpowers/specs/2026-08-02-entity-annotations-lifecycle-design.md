# 实体注解语义与映射生命周期设计（历史方案）

> 本文保留早期设计讨论。V2.0.0 已采用 flying-orm 自有实体注解和显式生命周期契约，下面与 Jakarta Persistence
> 相关的设想不属于当前实现，也不是当前使用方式。

## 目标

在不引入 JPA、Spring 或阻塞式持久化框架依赖的前提下，让实体 Repository 支持更完整的字段映射语义和生命周期扩展。动态表单仍是底层统一模型，实体能力只是类型安全的上层入口。

## 当前注解范围

- 当前实现使用 `com.flying.orm.core.annotation` 下的 `@TableName`、`@TableId`、`@TableField`、`@Version`、
  `@TableLogic`、`@EnumValue`、`@KeySequence` 和 `@OrderBy`，不反射读取 Jakarta Persistence。
- `@TableField` 的 `exist`、`select`、`fill`、`insertStrategy` 和 `updateStrategy` 统一参与实体元数据和写入计划。
- 主键生成由 `@TableId(type = ...)` 明确声明，数据库自增、框架生成和调用方赋值使用同一套结果语义。
- 实体字段冲突、列名冲突和不支持的映射在创建 Repository 前直接报错。

## 生命周期

- 生命周期扩展使用 `ReactiveEntityListener` 和 `EntityLifecyclePhase`，不通过注解全名反射识别回调。
- 原生响应式监听器返回 `Publisher<Void>`，可做非阻塞审计、字段填充和校验；不允许在执行线程里偷偷阻塞。
- 所有 before 回调都在订阅后、SQL 之前执行；失败则不发送 SQL。after 回调只在对应操作成功后执行。
- 批量写入逐实体执行 before；after 必须读取批量结果，不能把 `PARTIAL` 或 `UNKNOWN` 当成整批成功。

## 明确不做

- 不实现关系映射、级联、实体状态跟踪和懒加载。
- 不把同步 JDBC 生命周期模型带进响应式内核。
- 不让生命周期回调绕过 TenantScope、DataScope、FieldScope、TimeScope 或执行保护。
