# flying-orm 4.1.1 实体注解

注解只声明映射、结构和字段治理语义；不创建连接、不配置驱动、不创建事务或连接池。

## 注解总览

| 注解 | 位置 | 用途 |
| --- | --- | --- |
| `@TableName` | 类 | 表名与 schema |
| `@TableCatalog` | 类 | catalog |
| `@TableComment` | 类 | 表注释 |
| `@TablePrimaryKey` | 类 | 命名或复合主键 |
| `@TableId` | 字段 | 主键列与生成策略 |
| `@KeySequence` | 类 | 数据库序列名 |
| `@TableField` | 字段 | 列名、是否入库、是否查询、填充与写入策略 |
| `@TableColumn` | 字段/record 组件 | 类型、长度、精度、默认值、注释、可空性和生成方式 |
| `@TableLogic` | 字段 | 逻辑删除字段及值 |
| `@FlyingLogicDelete` | 类/字段 | 逻辑删除的显式字段与值声明 |
| `@Version` | 字段 | 乐观锁版本字段 |
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

## 常用实体

```java
@TableName(value = "orders", schema = "app")
@TableCatalog("business")
@TableComment("订单")
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
