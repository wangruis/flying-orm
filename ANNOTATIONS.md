# flying-orm 4.1.2 实体注解

注解只声明映射、结构和字段治理语义；不创建连接、不配置驱动、不创建事务或连接池。

## 注解总览

| 注解 | 位置 | 用途 |
| --- | --- | --- |
| `@TableName` | 类 | 表名、schema 与表注释 |
| `@TableCatalog` | 类 | catalog |
| `@TablePrimaryKey` | 类 | 命名或复合主键 |
| `@TableId` | 字段 | 主键列与生成策略 |
| `@KeySequence` | 类 | 数据库序列名 |
| `@TableField` | 字段 | 列名、是否入库、是否查询、填充与写入策略 |
| `@TableColumn` | 字段/record 组件 | 类型、长度、精度、默认值、注释、可空性和生成方式 |
| `@TableLogic` | 类/字段 | 逻辑删除字段及值；类级用 `field` 指定属性名或列名 |
| `@Version` | 字段 | 乐观锁预期版本；更新时数据库递增，输入实体不回写 |
| `@FlyingTenant` | 类/字段 | 默认租户隔离字段与策略 |
| `@OrderBy` | 字段 | Repository 默认排序 |
| `@EnumValue` | 枚举字段 | 枚举持久化值 |
| `@EncryptedField` | 字段 | 加密入库与 EXACT/SUFFIX/CONTAINS 检索 |
| `@MaskedField` | 字段 | 脱敏展示策略 |
| `@TableUnique` | 类，可重复 | 唯一约束及 NULL 策略 |
| `@TableIndex` / `@TableIndexColumn` | 类 / 注解成员 | 普通或唯一索引与列排序方向 |
| `@TableForeignKey` | 类，可重复 | 外键、引用字段和引用动作 |
| `@TableCheck` | 类，可重复 | 受控 CHECK 约束 |
| `@TablePartition` | 类 | PostgreSQL 单列时间 RANGE 分区父表 |

`@TableIndex.List`、`@TableUnique.List`、`@TableForeignKey.List`、`@TableCheck.List` 是 Java 为重复注解保留的容器，不需要手写。

## 非表字段

实体中只用于接口展示、计算或临时承载的属性，使用 `@TableField(exist = false)`。它不参与查询投影、读写、批量操作或 Schema 同步。

```java
class UserView {
    @TableId
    private Long id;

    private String name;

    @TableField(exist = false)
    private String displayName;
}
```

Java `transient` 属性同样不会映射为表字段；不要同时为非表字段叠加主键、列、索引或约束注解。

## 插入默认值

`INSERT_IF_NULL` 只在新增或 upsert 取值时填充 null，保留已有值（包括空字符串）；普通更新不填充。值由装配时的 `fieldFiller` 提供，仍经过实体枚举和字段编码，默认不回写实体。

`INSERT_DEFAULT` 同样只填充 null，但本次生成的默认值不参与 upsert 冲突更新；调用方显式提供的值仍按 `updateStrategy` 更新。适合“新增时补默认值，更新时只写显式值”。原生 JDBC/R2DBC 批量 upsert 支持各行不同的 INSERT/UPDATE 字段集合，按输入顺序执行。

```java
@TableName("audit_entry")
record AuditEntry(
    @TableId Long id,
    @TableField(fill = FieldFill.INSERT_IF_NULL, updateStrategy = FieldStrategy.NEVER)
    Instant createdAt,
    @TableField(fill = FieldFill.INSERT_DEFAULT) String status,
    String name) {}

FlyingOrmClients clients = FlyingOrmClients.builder(access)
    .dialect(RdbDialect.postgresql())
    .fieldFiller(EntityFieldFiller.byProperty(Map.of(
        "createdAt", Instant::now,
        "status", () -> "enabled")))
    .build();
```

`byProperty` 按 Java 属性名注册取值函数，每次实际填充时才调用；同名属性共用函数，需要区分实体、操作或当前值时仍可使用四参数回调。函数须支持并发调用，返回 Java 值即可，枚举等继续由实体映射编码。

本例 `createdAt` 的 `updateStrategy = NEVER` 禁止普通更新和 upsert 冲突更新修改它；`status` 仅补新增默认值，显式传入时仍可更新。`@TableColumn.defaultId` 则是数据库结构默认表达式，不是 Java 运行时填充。

需要把填充值同步到普通实体时，显式开启 `fillBack`，无需在填充器中手写赋值：

```java
@TableName("audit_entry")
class AuditEntryBean {
    @TableId Long id;
    @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true)
    Instant createdAt;
    String name;
}
```

回填优先调用 setter，没有 setter 时写入非 final 字段；值保持 Java 类型（如枚举），不会把编码后的数据库值写回实体。`record` 不支持开启回填。回填发生在 SQL 参数准备阶段，**不代表数据库执行成功，失败也不会撤销实体变化**；再次使用该实体写入时，已回填的值视为显式值。同步、响应式和批量共用此规则，响应式写入在订阅后填充。`fillBack` 也适用于更新填充。

## 常用实体

```java
@TableName(value = "orders", schema = "app", comment = "订单")
@TableCatalog("business")
@KeySequence("seq_orders")
@TableUnique(id = "uk_orders_no", name = "uk_orders_no", properties = "orderNo")
@TableIndex(id = "ix_orders_user_created", columns = {
    @TableIndexColumn(property = "userId"),
    @TableIndexColumn(property = "createdAt", direction = TableIndexColumn.Direction.DESC)
})
@TableCheck(id = "ck_orders_amount", property = "amount",
    operator = TableCheck.Operator.GREATER_THAN, literalValues = "0")
public class Order {
    @TableId(type = IdType.INPUT)
    private Long id;

    @TableField("order_no")
    @TableColumn(length = 64, nullable = TableColumn.Nullability.NOT_NULL)
    private String orderNo;

    private Long userId;

    @TableColumn(precision = 18, scale = 2)
    private BigDecimal amount;

    @FlyingTenant
    private Long orgId;

    @TableLogic
    private Integer deleted;

    @Version
    private Long version;

    @OrderBy(asc = false, sort = 1)
    private Instant createdAt;

    @TableField(exist = false)
    private String displayName;
}
```

## 关系与分区

```java
@TablePrimaryKey(name = "pk_order_item", properties = {"orderId", "lineNo"})
@TableForeignKey(id = "fk_item_order", localProperties = "orderId",
    targetEntity = Order.class, targetProperties = "id",
    onDelete = ReferentialAction.CASCADE)
class OrderItem {
    private Long orderId;
    private Integer lineNo;
}

@TableName("event_log")
@TablePartition(strategy = TablePartition.Strategy.RANGE, property = "createdAt")
class EventLog {
    @TableId private Long id;
    private Instant createdAt;
}
```

## 枚举、加密与脱敏

```java
enum UserStatus {
    ACTIVE("A"), DISABLED("D");

    @EnumValue
    private final String code;

    UserStatus(String code) { this.code = code; }
}

class Customer {
    @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
        suffixLengths = 4)
    @MaskedField(policy = "partial", prefix = 3, suffix = 4)
    private String phone;
}
```

更多查询与操作示例见 [EXAMPLES.md](EXAMPLES.md)。

同一 Java 类型需要按字段采用不同存储表示时，用 `@TableColumn(databaseTypeId = "映射ID")` 选择 `EntityTypeMappingRegistry.Builder.registerNamed(...)` 注册的映射，并通过 `entitySchema` 装配。它不改变其他同类型字段；有 `@EnumValue` 时映射针对值成员类型。完整代码见 [字段级类型映射](EXAMPLES.md#字段级类型映射)。
