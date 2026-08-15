# Dynamic Form Optimistic Lock Design

## Goal

给动态表单 DML 增加首版乐观锁能力，避免多人或多服务同时修改同一行时静默覆盖。首版只做单行 update/delete，不做批量乐观锁。

## Scope

- 支持动态表单 update/delete 显式传入乐观锁选项。
- 支持数字版本字段，比如 `version` / `revision`，update 成功后自动 `+1`。
- 支持时间版本字段，比如 `updated_at`，update 成功后刷新为调用方传入的新时间值。
- 影响行数为 0 时抛出稳定的 `OptimisticLockConflictException`，上层可以 catch 后决定重试、提示用户或放弃。
- Repository 响应式和同步入口复用同一套语义。

不默认开启乐观锁，不改变现有 update/delete 行为；不引入 Spring，不处理上层事务。

## API Shape

新增 `OptimisticLockOptions`：

- `field`：版本字段名。
- `expectedValue`：调用方读数据时拿到的旧版本值。
- `nextValue`：可选，新版本值；不传时数字版本默认 `+1`。
- `mode`：`INCREMENT` 或 `ASSIGN`。

新增 `OptimisticLockConflictException`：

- 携带 table、field、expectedValue。
- 只表示“版本条件没匹配上”，不混成普通 SQL 错误。

## SQL Behavior

Update:

```sql
update users
set name = ?, version = version + 1
where id = ? and version = ?
```

Delete:

```sql
delete from users
where id = ? and version = ?
```

时间版本或调用方显式传 nextValue 时：

```sql
update users
set name = ?, updated_at = ?
where id = ? and updated_at = ?
```

## Error Handling

- `rowsUpdated == 1`：成功。
- `rowsUpdated == 0`：抛 `OptimisticLockConflictException`。
- `rowsUpdated > 1`：保留原影响行数，不在首版额外判断；多行乐观锁后续单独设计。

## Testing

- SQL 渲染测试：update 带版本条件和版本递增。
- SQL 渲染测试：delete 带版本条件。
- 表单客户端测试：影响行数为 0 时抛稳定冲突异常。
- Repository 测试：响应式和同步入口能传递乐观锁选项。

## Follow-up

- 批量乐观锁。
- 自动从元数据或 Repository 映射里识别版本字段。
- 乐观锁冲突观测事件细化。
