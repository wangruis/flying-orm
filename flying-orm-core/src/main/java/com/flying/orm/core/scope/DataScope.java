package com.flying.orm.core.scope;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 可信的数据访问范围。
 *
 * <p>外部条件表达“这次想查什么”，DataScope 表达“这次最多允许碰到什么”。两者只能 AND 到一起，
 * 所以外部条件最多把结果查窄，不能把可信范围查宽。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class DataScope {

    public static final String DEFAULT_ORG_FIELD = "org_id";

    public static final String DEFAULT_SELF_FIELD = "user_id";

    public static final String ORG_AND_CHILDREN_OPERATOR = "org-and-children";

    private static final DataScope NONE = new DataScope(null, FieldScope.unrestricted(), List.of());

    private final ConditionGroup condition;

    private final FieldScope fields;

    private final List<TenantScope> tenantScopes;

    private DataScope(ConditionGroup condition, FieldScope fields) {
        this(condition, fields, List.of());
    }

    private DataScope(ConditionGroup condition, FieldScope fields, List<TenantScope> tenantScopes) {
        this.condition = condition;
        this.fields = Objects.requireNonNull(fields, "field scope must not be null");
        this.tenantScopes = List.copyOf(tenantScopes);
    }

    public static DataScope none() {
        return NONE;
    }

    /**
     * 全部数据。
     *
     * <p>这里的“全部”只表示 DataScope 自己不再加条件。它不会清掉默认租户范围，也不会绕过调用方传入的
     * 其他可信 scope，所以 SaaS 场景里依然会被 TenantScope 收住。</p>
     */
    public static DataScope all() {
        return NONE;
    }

    /**
     * 使用一棵可信条件树创建行级范围。空条件等价于 {@link #none()}。
     * 这里接收的是内部 AST，不接收前端 JSON。
     */
    public static DataScope where(ConditionGroup condition) {
        ConditionGroup safeCondition = Objects.requireNonNull(condition, "data scope condition must not be null");
        if (ConditionGroups.isEmpty(safeCondition)) {
            return none();
        }
        return new DataScope(safeCondition, FieldScope.unrestricted());
    }

    /**
     * 创建可信租户范围，并额外记录租户字段和值供写入侧补齐或校验。
     * 普通 {@link #where(ConditionGroup)} 不会被反向猜成租户身份。
     */
    public static DataScope tenant(String field, Object value) {
        TenantScope scope = TenantScope.of(field, value);
        return new DataScope(scope.toCondition(), FieldScope.unrestricted(), List.of(scope));
    }

    /**
     * 把时间窗口包装成普通数据范围，后续仍然和租户、组织等范围继续 AND。
     */
    public static DataScope time(TimeScope scope) {
        return where(Objects.requireNonNull(scope, "time scope must not be null").toCondition());
    }

    /**
     * 当前组织及下级组织的数据。
     *
     * <p>组织树怎么查属于使用方的数据模型，不应该在 core 写死。这里固定生成 `org-and-children` 这个扩展 term，
     * 调用方或 rdb 渲染器注册它的 SQL 翻译即可，参数仍然走占位符。</p>
     */
    public static DataScope orgAndChildren(Object orgId) {
        return orgAndChildren(DEFAULT_ORG_FIELD, orgId);
    }

    /**
     * 当前组织及下级组织的数据，组织字段由调用方明确指定。
     */
    public static DataScope orgAndChildren(String field, Object orgId) {
        return preset(field, ORG_AND_CHILDREN_OPERATOR, orgId, "organization id");
    }

    /**
     * 当前组织的数据，不包含下级组织。
     */
    public static DataScope orgOnly(Object orgId) {
        return orgOnly(DEFAULT_ORG_FIELD, orgId);
    }

    /**
     * 当前组织的数据，组织字段由调用方明确指定。
     */
    public static DataScope orgOnly(String field, Object orgId) {
        return preset(field, "=", orgId, "organization id");
    }

    /**
     * 当前用户自己的数据。
     */
    public static DataScope self(Object userId) {
        return self(DEFAULT_SELF_FIELD, userId);
    }

    /**
     * 当前用户自己的数据，用户字段由调用方明确指定。
     */
    public static DataScope self(String field, Object userId) {
        return preset(field, "=", userId, "user id");
    }

    /**
     * 追加字段范围，并与当前字段范围取交集，不改变行级条件和可信租户信息。
     *
     * <p>交集语义保证链式调用只能继续收窄字段读写权限，不能通过后一次调用放宽已有范围。
     * 未设置字段范围时仍可用本方法初始化限制。</p>
     *
     * @param fieldScope 新的字段读写范围
     * @return 新 DataScope，原对象保持不变
     */
    public DataScope withFields(FieldScope fieldScope) {
        return new DataScope(condition, intersect(fields, fieldScope), tenantScopes);
    }

    /**
     * 安全合并两个可信范围。行条件始终 AND，字段白名单取交集，租户声明全部保留。
     * 交集语义保证后加入的范围只能继续收窄权限，不能把已有权限放宽。
     */
    public DataScope and(DataScope other) {
        DataScope safeOther = Objects.requireNonNull(other, "other data scope must not be null");
        ConditionGroup merged = merge(condition, safeOther.condition);
        FieldScope mergedFields = intersect(fields, safeOther.fields);
        List<TenantScope> mergedTenantScopes = new ArrayList<>(tenantScopes.size() + safeOther.tenantScopes.size());
        mergedTenantScopes.addAll(tenantScopes);
        mergedTenantScopes.addAll(safeOther.tenantScopes);
        return new DataScope(merged, mergedFields, mergedTenantScopes);
    }

    /** @return 没有行条件、租户声明且读写字段都不受限时为 true */
    public boolean empty() {
        return condition == null
                && tenantScopes.isEmpty()
                && fields.unrestrictedRead()
                && fields.unrestrictedWrite();
    }

    /** @return 可选的行级条件 AST */
    public Optional<ConditionGroup> condition() {
        return Optional.ofNullable(condition);
    }

    /** @return 当前字段读写范围 */
    public FieldScope fields() {
        return fields;
    }

    /**
     * 找到调用方通过 {@link #tenant(String, Object)} 明确给出的可信租户范围。
     *
     * <p>普通 {@link #where(ConditionGroup)} 不会被反向猜成租户范围，避免把外部筛选条件误当成可信身份信息。</p>
     *
     * @param field 租户字段名
     * @return 当前范围中的租户值
     */
    public Optional<TenantScope> tenantScope(String field) {
        String safeField = requireText(field, "tenant field");
        TenantScope result = null;
        for (TenantScope scope : tenantScopes) {
            if (!scope.field().equalsIgnoreCase(safeField)) {
                continue;
            }
            if (result != null && !Objects.deepEquals(result.value(), scope.value())) {
                throw new IllegalArgumentException("conflicting tenant scope values");
            }
            result = scope;
        }
        return Optional.ofNullable(result);
    }

    private static ConditionGroup merge(ConditionGroup left, ConditionGroup right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return ConditionGroups.and(left, right);
    }

    private static FieldScope intersect(FieldScope left, FieldScope right) {
        return FieldScope.intersect(left, right);
    }

    private static DataScope preset(String field, String operator, Object value, String valueName) {
        Objects.requireNonNull(value, valueName + " must not be null");
        return where(ConditionGroup.and().where(field, operator, value).build());
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
