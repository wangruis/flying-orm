package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;

import java.util.Objects;

/**
 * 给 operator 查询、更新和删除使用的短条件 DSL。
 *
 * <p>严格方法会保留调用方传入的空值，并在条件编译阶段按 operator 规则处理；带 {@code IfPresent}
 * 的方法才会清理空字符串、纯空白文本和空集合并跳过无效条件。字段名始终只按 SQL 标识符处理，
 * operator 必须来自注册表，值始终走参数绑定。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class WhereDsl {

    private final ConditionGroup.Builder builder;

    /** 默认构造只在本包的基础契约测试中使用，正式 Operator 会传入自己的 SqlRenderer。 */
    WhereDsl() {
        this.builder = ConditionGroup.and();
    }

    /** 使用执行链已经装好的 renderer 创建条件，扩展 term 不需要再注册第二遍。 */
    WhereDsl(SqlRenderer renderer) {
        this.builder = Objects.requireNonNull(renderer, "sql renderer must not be null").conditions();
    }

    /**
     * 添加严格的等值条件。值为 null 或空文本时不会在这里静默丢弃。
     *
     * @param field 字段名
     * @param value 条件值
     * @return 当前 DSL
     */
    public WhereDsl is(String field, Object value) {
        builder.where(SqlIdentifiers.requireIdentifier(field, "where field"), "=", value);
        return this;
    }

    /**
     * 添加严格的扩展条件，例如 {@code term("userId", "user-in-org", orgId)}。
     *
     * @param field 字段名
     * @param operator 已注册的通用或业务 operator
     * @param value 条件值
     * @return 当前 DSL
     */
    public WhereDsl term(String field, String operator, Object value) {
        builder.where(SqlIdentifiers.requireIdentifier(field, "where field"), operator, value);
        return this;
    }

    /**
     * 添加可选等值条件。清理后没有有效值时不生成条件，适合搜索表单的非必填参数。
     *
     * @param field 字段名
     * @param value 可选条件值
     * @return 当前 DSL
     */
    public WhereDsl isIfPresent(String field, Object value) {
        builder.whereIfPresent(SqlIdentifiers.requireIdentifier(field, "where field"), "=", value);
        return this;
    }

    /**
     * 添加可选扩展条件。是否跳过只由统一的值清理策略决定，不允许 operator 自己拼 SQL。
     *
     * @param field 字段名
     * @param operator 已注册的 operator
     * @param value 可选条件值
     * @return 当前 DSL
     */
    public WhereDsl termIfPresent(String field, String operator, Object value) {
        builder.whereIfPresent(SqlIdentifiers.requireIdentifier(field, "where field"), operator, value);
        return this;
    }

    /**
     * 添加 IS NULL 条件，不需要传入占位参数。
     *
     * @param field 字段名
     * @return 当前 DSL
     */
    public WhereDsl isNull(String field) {
        builder.whereNull(SqlIdentifiers.requireIdentifier(field, "where field"));
        return this;
    }

    /**
     * 添加 IS NOT NULL 条件，不需要传入占位参数。
     *
     * @param field 字段名
     * @return 当前 DSL
     */
    public WhereDsl isNotNull(String field) {
        builder.whereNotNull(SqlIdentifiers.requireIdentifier(field, "where field"));
        return this;
    }

    ConditionGroup build() {
        return builder.build();
    }
}
