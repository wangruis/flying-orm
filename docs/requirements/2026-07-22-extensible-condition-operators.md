# 2026-07-22 可扩展业务条件算子进展

## 背景

用户明确要求通用条件不能局限于 `=`、`>`、`like` 等 SQL 原生比较，并强调
`where("userId","user-in-org",orgId)` 是重点能力。该类条件本质上是 Java 侧的业务语义，
SQL 只是最终执行形态。

## 本次实现

- 在 `flying-orm-core` 的 SQL term SPI 上新增关系存在型算子工厂 `SqlTermHandler.relationExists(...)`。
- 新增 `RelationExistsTermHandler`，用于将业务 term 渲染为参数化 `exists` 子查询。
- `relationExists` 已支持单值、集合和数组参数：单值渲染为 `= ?`，多值渲染为 `in (?, ?)`。
- 新增 `SqlTermHandler.relationNotExists(...)`，用于表达反向关系过滤，例如排除属于指定组织、角色或租户的数据。
- 新增 `SqlTermPackage` 命名包 SPI，可把多个业务 term handler 作为一个领域包一次性注册。
- 新增 `RelationTermPackage.of(...)`，可一次定义一对关系存在与不存在 term，不在框架里写死业务表结构。
- `SqlRenderer.Builder` 新增 `addTermPackage(...)`，执行层仍然只消费统一 SQL 渲染结果。
- 可与 `ParameterConditionPackage.of(...)` 形成端到端组合：请求参数先编译为业务 term，再由关系 SQL term 包渲染。
- `user-in-org` 不作为框架硬编码业务，而是通过如下配置注册：

```java
SqlTermHandler.relationExists("user-in-org", "org_user", "ou", "user_id", "org_id")
```

也可以直接使用命名条件包：

```java
SqlRenderer renderer = SqlRenderer.builder()
                                  .addTermPackage(RelationTermPackage.of(
                                          "user-organization",
                                          "org_user",
                                          "ou",
                                          "user_id",
                                          "org_id",
                                          "user-in-org",
                                          "user-not-in-org"))
                                  .build();
```

- 业务侧查询仍然保持结构化 Java 表达：

```java
where.where("userId", "user-in-org", orgId)
```

- 渲染结果保持参数化，业务参数只进入 `SqlFragment.parameters()`，不拼接进 SQL 文本。

## 边界

- 当前 `relationExists` 和 `relationNotExists` 适合机构、角色、租户、数据权限等关系表存在性判断。
- 直接渲染空集合或空数组会抛出异常；参数编译器路径会提前跳过空集合，避免生成无意义 SQL。
- `RelationTermPackage.of(...)` 要求调用方明确关系表、别名、关联列和 term id，ORM 不猜业务结构。
- 递归组织树展开、批量条件去重、反连接优化和方言级 SQL 优化留作后续扩展。
- 该能力仍处于 core 层的 SQL 渲染 SPI，不绑定 R2DBC 执行器，响应式路径可直接复用渲染结果。
