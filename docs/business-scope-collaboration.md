# 业务授权与 Scope 协作

flying-orm 不计算“谁能看什么”，只接收上层已经算好的可信范围并安全合并到 SQL。设备、组织、分享和告警都只是业务例子，不是 ORM 内置模型。

## 两类系统

无租户系统只传业务范围：

```java
DataScope scope = DataScope.orgOnly("org_id", currentOrgId);
```

SaaS 系统先固定租户，再继续收窄业务范围：

```java
DataScope scope = DataScope.tenant("tenant_id", currentTenantId)
                           .and(DataScope.orgAndChildren("org_id", currentOrgId));
```

`DataScope.all()` 只表示当前可信边界内的全部数据，不会移除已有租户范围。

## 设备归属

设备只归当前用户时，上层把用户身份转换成普通条件：

```java
DataScope deviceOwner = DataScope.where(
        ConditionGroup.and()
                      .where("owner_id", "=", currentUserId)
                      .build()
);
```

如果设备可能属于组织，就由上层根据授权结果选择 `orgOnly(...)`、`orgAndChildren(...)` 或直接传允许的组织编号集合。

## 组织树

```java
DataScope organizationTree = DataScope.orgAndChildren("org_id", currentOrgId);
```

这个预设只生成 `org-and-children` 业务 term。闭包表、路径字段、缓存结果等组织树实现由上层注册的 term handler 决定，flying-orm 不写死业务表结构。

## 用户分享

“本人拥有或别人分享给我”可以组合成一个服务端 OR 条件：

```java
DataScope sharedData = DataScope.where(
        ConditionGroup.or()
                      .where("owner_id", "=", currentUserId)
                      .where("id", "shared-with-user", currentUserId)
                      .build()
);
```

`shared-with-user` 是应用注册的业务 term。前端只能传普通业务筛选，不能自己决定这个可信范围。

## 告警时间范围

```java
DataScope alarmScope = DataScope.tenant("tenant_id", currentTenantId)
                                .and(DataScope.time(
                                        TimeScope.between("alarm_time", visibleFrom, visibleUntil)))
                                .withFields(FieldScope.readable("id", "device_id", "level", "alarm_time"));
```

时间语义和时区换算由上层完成。flying-orm 使用参数绑定生成左闭右开的时间条件，并与租户、设备范围和前端条件继续 AND。

## 安全边界

- Scope 必须来自服务端可信上下文，不能直接采用前端传来的租户、组织或用户编号。
- 前端结构化条件只能继续收窄结果，不能覆盖 TenantScope、DataScope、FieldScope 或 TimeScope。
- `AUTO` 租户表单会自动补租户字段；`MANUAL` 表单必须提供与可信租户范围一致的值。
- FieldScope 同时约束读字段和写字段，受保护字段会在生成 SQL 前被拒绝。
- 告警等业务事件可以放在普通关系库；连续时序采集数据不属于 flying-orm 职责。
