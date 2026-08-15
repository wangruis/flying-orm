# 轻量多表 JOIN 查询设计

## 状态

- 设计状态：已确认
- 日期：2026-08-09
- 适用模块：`flying-orm-core`、`flying-orm-rdb`、`flying-orm-testkit`

## 目标

为实体 Lambda 和 DynamicForm 增加轻量的多表查询能力，支持 `INNER JOIN`、`LEFT OUTER JOIN` 和
`RIGHT OUTER JOIN`。JOIN 只负责查询，继续复用统一的条件、Scope、逻辑删除、参数绑定、方言、执行保护和结果模型，
不建立第二套 SQL DSL。

## API 形态

实体入口：

```java
dml.joinQuery(User.class)
   .leftJoin(Order.class, User::getId, Order::getUserId)
   .join(Role.class, User::getRoleId, Role::getId)
   .select(User.class, User::getName)
   .selectAs(Order.class, Order::getOrderNo, "orderNo")
   .where(User.class, User::getEnabled, "=", true)
   .executeRows();
```

DynamicForm 入口：

```java
dml.joinQuery(userForm)
   .rightJoin(orderForm, "id", "userId")
   .select(userForm, "name")
   .selectAs(orderForm, "orderNo", "orderNo")
   .executeRows();
```

同步入口提供相同构建语义，但最终使用原生 JDBC；响应式入口使用原生 R2DBC。两者共享不可变 Join AST 和同一 SQL
渲染器，不让同步热路径经过 Reactor，也不把 JDBC 包装为响应式访问。

## JOIN 语义

- `join()` 固定表示 `INNER JOIN`。
- `leftJoin()` 固定表示 `LEFT OUTER JOIN`，不再增加含义重复的 `leftOuterJoin()`。
- `rightJoin()` 固定表示 `RIGHT OUTER JOIN`，不再增加含义重复的 `rightOuterJoin()`。
- 第一版 ON 只允许已注册字段之间的等值比较，并允许通过 `andOn(...)` 增加复合键条件。
- ON 不接受业务值、原始 SQL、OR、函数、子查询或任意表达式。
- 第一版不支持同一实体或同一 DynamicForm 的自连接，因此业务 API 不暴露字符串表别名。
- SQL 内部别名由渲染器按数据源顺序稳定生成，例如 `t0`、`t1`，调用方不需要管理。

## Join AST

core 新增独立 `com.flying.orm.core.join` 模型，不把 `source.field` 字符串塞入现有 `ConditionGroup`：

- `JoinType`：`INNER`、`LEFT`、`RIGHT`。
- `JoinSource`：稳定的数据源 ID、DynamicForm 和自动 SQL 别名信息。
- `JoinFieldRef`：数据源 ID 与已校验字段名。
- `JoinOn`：左右字段引用组成的等值条件列表。
- `JoinProjection`：字段引用和可选结果别名。
- `JoinOrder`：字段引用和排序方向。
- `JoinQuerySpec`：主数据源、JOIN 列表、投影、条件、排序和分页的不可变快照。

实体 Lambda 只负责把 getter 解析为 `JoinFieldRef`；DynamicForm 字符串入口必须在建立 AST 时完成字段存在性验证。

## 投影与结果

- 多表查询必须显式选择返回字段，避免默认 `select *` 造成列名碰撞和无界结果。
- `selectAs(...)` 的结果别名必须是普通标识符，并且在规范化后唯一。
- 未显式提供结果别名时，使用稳定的 `<source>_<field>` 名称；名称仍由统一标识符规则校验。
- 默认返回 `DynamicRow`，保留共享 `RowLayout + Object[]` 的低分配结构。
- 可选使用现有 `RowMapper<T>` 映射平面 DTO。
- 不自动组装嵌套实体、集合关系、级联关系或懒加载代理。

## Scope 与外连接

每个数据源独立计算 TenantScope、DataScope、逻辑删除和字段权限。不能把可选侧的行级保护条件统一追加到最终
`WHERE`，否则 LEFT/RIGHT JOIN 会被错误收紧为 INNER JOIN。

渲染规则：

1. INNER JOIN 的行级保护条件可统一下推到 WHERE。
2. LEFT JOIN 右侧的行级保护条件放入该 JOIN 的 ON；左侧既有条件保持原语义。
3. RIGHT JOIN 左侧的行级保护条件放入该 JOIN 的 ON；右侧既有条件保持原语义。
4. 链式外连接无法安全只靠 ON/WHERE 表达时，把单个数据源渲染为受控派生关系，再参与 JOIN。
5. 字段权限在建立投影和条件时校验，不能只依赖数据库隐藏列。

派生关系只包含受控表名、字段、Scope 参数和逻辑删除参数，不接受调用方 SQL。四种真实数据库必须验证 SQL 与执行计划，
避免正确但不可接受的物化或全表扫描。

## 条件、排序和分页

- WHERE 继续使用现有 term 注册表、值形状、参数化绑定和 DataScope 合并规则。
- 条件必须显式指定数据源，不能通过带点字段名绕过标识符验证。
- offset/page 和 cursor page 使用 JOIN 投影对应的稳定排序字段。
- cursor 分页必须有唯一稳定排序；如果无法证明唯一性则拒绝，不自动猜测主键组合。
- count 复用同一 Join AST 和同一 Scope，只把投影替换为安全 count 计划。

## 字段保护互操作

- 受保护字段可以被投影，结果进入统一的解密与展示转换。
- 声明 EXACT 或 SUFFIX 的受保护字段可在 JOIN 查询的 WHERE 中搜索。
- 加密字段不能作为 ON、ORDER BY 或 GROUP BY 字段，因为随机密文没有业务排序和等值关联语义。
- 第一版拒绝在 JOIN 查询中使用 CONTAINS 保护搜索；两阶段候选复核会破坏外连接和数据库分页的精确语义。

## 错误与安全

- 数据源重复、字段不存在、投影别名冲突、ON 引用未加入的数据源、Scope 不可满足等问题在生成 SQL 前失败。
- 错误消息只使用稳定分类，不回显无界原始字段或别名。
- 所有业务值继续参数化绑定。
- 前端结构化条件不能控制 JOIN 源、ON 关系或结果展示模式；这些结构只允许可信服务端代码创建。

## 非目标

- JOIN UPDATE、JOIN DELETE、MERGE。
- FULL JOIN、CROSS JOIN、LATERAL、APPLY。
- 聚合 DSL、窗口函数、UNION、CTE 和子查询 DSL。
- 同表自连接和用户管理表别名。
- 自动关系发现、实体关联注解、级联和懒加载。

复杂查询继续使用启动期注册并审核的 SQL 模板，业务值仍必须命名参数绑定。

## 验证范围

1. AST 不可变、字段归属、复合 ON 和重复数据源验证。
2. INNER/LEFT/RIGHT 的 SQL、参数顺序和结果列别名。
3. 外连接下 TenantScope、DataScope 和逻辑删除不会改变 null-extension 语义。
4. 同步 JDBC 与响应式 R2DBC 使用同一 SqlRequest。
5. DynamicRow 与 DTO 映射、重复列标签和空值扩展。
6. offset/page、cursor page 和 count 语义一致。
7. 加密字段投影、EXACT/SUFFIX 搜索和非法 ON/排序拒绝。
8. H2 自动测试以及 MySQL 8.4、PostgreSQL、Oracle Free 23、SQL Server 2022 实库认证。
