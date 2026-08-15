# LIKE 忽略大小写设计

## 目标

为 flying-orm 增加跨 H2、MySQL 8.4、PostgreSQL、Oracle 23 和 SQL Server 2022 行为一致的 LIKE 忽略大小写能力，同时保持 ORM 轻量、参数化和单一条件内核。

## 对外语义

- 新增标准 operator `like-ignore-case` 和 `not-like-ignore-case`。
- Java 条件 DSL 使用现有入口：`where("name", "like-ignore-case", "%alice%")` 或 `WhereDsl.term(...)`。
- 前端结构化条件可使用同名 operator，并继续受字段白名单、operator 白名单、深度、节点数、字符串长度和 Scope 限制。
- `%` 和 `_` 保持数据库 LIKE 的现有通配符语义；本功能不新增自动转义规则。
- 普通 `like` 和 `not-like` 的现有语义不变。

## SQL 渲染

统一生成：

```sql
lower(<安全标识符>) like lower(?)
lower(<安全标识符>) not like lower(?)
```

字段名仍由方言标识符渲染器处理，业务值仍只进入绑定参数。Java 端不提前转换大小写，避免 Locale 影响；具体字符折叠规则由数据库及列字符集/排序规则决定。

不采用 PostgreSQL 专用 `ILIKE`，也不依赖 MySQL/SQL Server 默认排序规则，因为这些实现无法给四库提供统一 SQL 语义。

## 架构边界

- 在现有 `TermRegistry` 中声明两个 SCALAR 标准 term。
- 在现有 `SqlTermHandler` 默认包中注册两个内部 handler，不增加新的 public/protected 方法或生产类型。
- 在 `StructuredConditionPolicy` 默认 operator 映射中放行同名 operator。
- 在 `StructuralPlanCaches` 中将两个 operator 视为标准单参数结构，使重复查询继续复用有界结构缓存。
- 不改变 Scope、逻辑删除、加密字段查询规则、codec、参数顺序或 JDBC/R2DBC 执行链。

## 性能边界

`lower(column)` 可能使普通索引无法直接命中。flying-orm 不自动创建隐式函数索引；高频查询由上层按实际数据库选择函数索引、计算列索引或不区分大小写的列排序规则。该选择属于 Schema/部署策略，不污染通用查询 API。

## 验证

- Core：标准 Java AST 渲染、结构化条件编译、正向和否定 operator、参数不进入 SQL。
- RDB：五种内置方言的标识符渲染一致、结构计划缓存复用且不保留请求值。
- 回归：普通 LIKE 保持原 SQL；公共 API 基线无变化；Core/RDB 全量质量门禁通过。

## 明确不做

- 不增加 `.likeIgnoreCase()`、`.notLikeIgnoreCase()` 等链式公共快捷方法。
- 不增加 `ilike` 别名。
- 不做 Unicode 规范化、拼音搜索、全文检索或自动索引管理。
- 不改变加密字段的受保护搜索模式；随机密文字段不能借该 operator 绕过保护规则。
