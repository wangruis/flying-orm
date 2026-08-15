# 2026-07-22 参数驱动动态条件进展

## 背景

用户强调参数驱动动态条件是重点，条件不能局限于 `=`、`>`、`like` 等固定符号，并给出 `where("userId","user-in-org",orgId)` 作为业务 term 示例。

## 本次实现

- 在 `flying-orm-core` 新增 `ParameterConditionCompiler` 和 `ParameterConditionSpec`。
- 支持将请求参数 `Map<String, ?>` 按 Java 规则编译为 `ConditionGroup`。
- 支持缺失参数、`null`、空白字符串、空集合和空数组自动跳过。
- 支持规范化重复参数名检测，避免大小写和首尾空白导致条件值不确定。
- 在 `flying-orm-rdb` 的 `ReactiveFormClient` 中新增 `select(form, compiler, parameters)`，让动态表单查询可以直接接收请求参数并走 R2DBC/Reactor 链路。
- `ParameterConditionSpec` 已支持默认值和参数转换器，参数语义在 core 层完成，不下沉到 SQL 或执行器。
- `ParameterConditionCompiler` 已支持 `addOrGroup(...)`，可将同一个参数映射为嵌套 OR 条件组，例如 `keyword -> name like OR mobile like`。
- 新增 `ParameterConditionPackage` 参数条件命名包 SPI，可将一组请求参数映射规则一次性装配进 `ParameterConditionCompiler`。
- `ParameterConditionPackage.of(...)` 可把参数映射与对应的 `TermRegistry` 放在同一个命名包里，例如配置 `orgIds -> userId/user-in-org`。
- `ParameterConditionCompiler.Builder` 新增 `addPackage(...)`，支持请求参数包与 SQL term 包按 operator id 联动；框架不预设任何组织表名或字段名。
- `ReactiveFormClient` 已新增 `select(form, conditionPackage, parameters)`，动态表单响应式查询可直接接收参数条件包并走 R2DBC/Reactor 查询链路。

## 边界

- 当前版本支持 AND 根条件和一层 OR 子组，深层任意嵌套规则留作下一阶段扩展。
- 更复杂的参数来源适配、转换器注册表和错误码模型作为后续扩展点。
- 业务 term 的 SQL 解释仍由 `SqlTermHandler` 或 `SqlTermPackage` 注册，例如 `user-in-org`。
- 参数条件包不依赖 SQL 渲染包，两者只通过稳定 operator id 协作，避免参数层绑定某一种执行形态。
