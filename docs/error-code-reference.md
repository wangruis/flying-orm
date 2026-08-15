# 错误码手册

上层服务优先读取 `OrmErrorReport` 的 `category`、`code`、`resource`、`path` 和 `field`，不要按异常文本做业务判断。异常文本用于人看，可以改进措辞；稳定错误码属于公开契约。

`category` 与 `code` 在 1.x 中保持稳定。允许新增错误码，但不会静默改变已有错误码的含义；上层遇到未知新码时应按所属 category 的通用错误处理。

| category | code 来源 | resource | path/field |
| --- | --- | --- | --- |
| `CONDITION` | `StructuredConditionErrorCode` | 通常为空 | 指向前端条件位置和字段 |
| `SCOPE` | `ScopeErrorCode` | 动态表单标识 | 受保护字段，没有时为空 |
| `DATABASE` | `RdbErrorKind` | SQLState，没有时为空 | 为空 |
| `PROTECTED_FIELD` | 受保护字段稳定分类 | 为空 | 为空 |

## 结构化条件

类别为 `CONDITION`，异常类型为 `StructuredConditionException`。常用错误码：

| 错误码 | 含义 | 上层建议 |
| --- | --- | --- |
| `FIELD_NOT_ALLOWED` | 字段不在允许范围，或命中租户/逻辑删除等受保护字段 | 返回参数错误，并使用 `path` 标出条件位置 |
| `OPERATOR_NOT_ALLOWED` | operator 没有注册或未放行 | 返回参数错误，不要降级成原生 SQL |
| `FIELD_OPERATOR_NOT_ALLOWED` | operator 存在，但该字段不允许使用 | 提示该字段支持的查询方式 |
| `VALUE_NULL` / `VALUE_BLANK` | 该条件要求有效值 | 提示前端删除空条件或改用 null 专用条件 |
| `VALUE_TYPE_MISMATCH` / `VALUE_CONVERSION_FAILED` | 值无法按动态字段类型转换 | 返回字段类型提示 |
| `VALUE_COLLECTION_EMPTY` / `VALUE_COLLECTION_TOO_LARGE` | 集合为空或超过策略上限 | 调整输入，不要绕开安全策略 |
| `VALUE_RANGE_SIZE_INVALID` / `VALUE_RANGE_TYPE_MISMATCH` / `VALUE_RANGE_ORDER_INVALID` | 范围值数量、类型或顺序错误 | 标记 `.value` 并要求两个合法边界 |
| `DEPTH_EXCEEDED` / `NODE_COUNT_EXCEEDED` | 条件树超过安全限制 | 拒绝请求，避免复杂条件拖垮服务 |

其余稳定码：`INVALID_NODE_SHAPE`、`EMPTY_GROUP`、`LOGIC_NOT_ALLOWED`、`VALUE_TOO_LONG`、`VALUE_SHAPE_NOT_ALLOWED`。

## Scope 安全

类别为 `SCOPE`，异常类型为 `ScopeAccessException`。

| 错误码 | 含义 |
| --- | --- |
| `TENANT_SCOPE_REQUIRED` | 表单要求租户范围，但调用没有提供可信租户 scope |
| `TENANT_FIELD_REQUIRED` | 租户策略开启，但动态表单没有对应租户字段 |
| `TENANT_VALUE_MISMATCH` | 写入值与服务端租户值冲突 |
| `DUPLICATE_TENANT_FIELD` | 同一批数据重复或冲突地声明租户字段 |
| `FORM_FIELDS_REQUIRED` | 字段范围保护需要动态表单字段元数据 |
| `NO_READABLE_FIELDS` | 字段裁剪后没有任何字段可读 |
| `FIELD_NOT_READABLE` | 请求读取受保护字段 |
| `FIELD_NOT_WRITABLE` | insert/update/batch 尝试写入受保护字段 |

## 数据库执行

类别为 `DATABASE`，异常类型为 `RdbException`，`resource` 保存 SQLState。稳定分类如下：

`DUPLICATE_KEY`、`CONSTRAINT`、`BAD_SQL`、`CONNECTION`、`TIMEOUT`、`DEADLOCK`、`LOCK_TIMEOUT`、`CANCELLED`、`UNKNOWN`。

`DEADLOCK`、`LOCK_TIMEOUT` 和 `CONNECTION` 不代表写入一定可以重试。只有业务操作具备幂等性并且事务结果明确时，上层才能按自己的策略重试。`UNKNOWN` 必须先使用 recovery token、业务唯一键或查询接口确认结果，禁止直接重放。

数据库原始数字错误码保留在 `RdbException.errorCode()` 中用于排障，不属于跨数据库稳定契约。业务分支只使用 `RdbErrorKind`。

连接分类优先识别 SQLState `08`。PostgreSQL 会话被管理员终止、实例关闭或数据库被删除时使用 `57P0x`，
同样归为 `CONNECTION`。少数 R2DBC 驱动确认底层资源已经失效却不给 SQLState，此时
`R2dbcNonTransientResourceException` 作为最后兜底归为 `CONNECTION`；已有更明确 SQLState 的错误仍按原分类处理。

## 执行保护异常

执行保护发生在数据库错误翻译之外，因此不会伪装成 `RdbException`。上层可以按异常类型读取结构化限制值：

| 异常 | 含义 | 可读取信息 |
| --- | --- | --- |
| `SqlExecutionTimeoutException` | 调用超过 `SqlExecutionOptions.timeout()` | `timeout()` |
| `SqlRowLimitExceededException` | 查询返回行数超过 `maxRows` | `statementType()`、`maxRows()`、`overflowIndex()` |
| `SqlResultMemoryLimitExceededException` | 查询累计结果超过 `maxResultBytes` | `statementType()`、`maxResultBytes()`、`attemptedBytes()`、`overflowIndex()` |
| `SqlLargeObjectLimitExceededException` | BLOB/CLOB 物化大小超过上限 | `kind()`、`maxSize()`、`actualSize()` |

这些异常不包含 SQL 参数值或大对象内容，日志可以记录限制值，但不要在上层补打原始业务数据。

## 受保护字段

`ProtectedSearchCandidateLimitExceededException` 提供稳定报告
`PROTECTED_FIELD / PROTECTED_SEARCH_CANDIDATE_LIMIT`，表示 CONTAINS 候选数超过 1000，不能通过截断继续返回不完整结果。
上层可以读取 `limit()` 和 `actual()`，但不应回显查询明文。

密文信封损坏、认证失败或缺少可读密钥版本时抛出 `ProtectedFieldException`，消息固定为
`protected field value cannot be decrypted`，不会包含密文、密钥版本或密码实现细节。该异常通常应按服务端密钥配置或
数据完整性故障处理，而不是向终端用户解释底层原因。

## 数据迁移补偿结果

`DataMigrationResult` 使用 `ROLLBACK_FAILED` 表示至少一个补偿步骤未完成。`DataMigrationStepResult.rollbackFailure()` 只返回稳定说明 `data migration rollback failed`，不会复制驱动异常原文、SQL、连接串或业务值。调用方应按状态停止发布并人工处理，不能解析该说明判断数据库类型或自动重试。

## 观测结果

`SqlFailureCategory` 和 `SqlExecutionResultKind` 用于 observer、指标和批量回执，不替代 `OrmErrorReport`。普通 SQL、批量分片、整批汇总和 UNKNOWN 恢复都可以归到同一组结果语义：

`SUCCESS`、`CANCELLED`、`TIMEOUT`、`ROW_LIMIT`、`RESULT_MEMORY_LIMIT`、`DUPLICATE_KEY`、`CONSTRAINT`、`BAD_SQL`、`CONNECTION`、`DEADLOCK`、`LOCK_TIMEOUT`、`OPTIMISTIC_LOCK`、`ROLLED_BACK`、`PARTIAL`、`UNKNOWN`。

`PARTIAL` 只表示显式 `INDEPENDENT` 模式下部分分片已经提交；`ROLLED_BACK` 表示原子批次已回滚；`UNKNOWN` 表示当前证据不足，不能推断成功或失败。上层必须保留 recovery token，并在恢复确认前停止自动重放。

## HTTP 映射建议

flying-orm 本体不依赖 Web 框架，也不固定 HTTP 状态码。上层通常可将条件错误映射为 400，将 scope 缺失或冲突映射为 403/409，将唯一键冲突映射为 409，将超时和连接故障映射为 503/504。最终映射由业务协议决定。
