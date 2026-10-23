package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;

import java.util.Objects;
import java.util.function.Consumer;

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
public final class WhereDsl implements ConditionDsl<String, WhereDsl> {

    private ConditionGroup.Builder builder;
    private final SqlRenderer renderer;
    private ConditionGroup snapshot;

    /** 使用执行链已经装好的 renderer 创建条件，扩展 term 不需要再注册第二遍。 */
    WhereDsl(SqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.builder = renderer.conditions();
    }

    private WhereDsl(ConditionGroup.Builder builder) {
        this.builder = builder;
        this.renderer = null;
    }

    /** 命令接管回调的独立快照，避免调用方保留 DSL 后修改正在执行的请求。 */
    WhereDsl(SqlRenderer renderer, ConditionGroup initial) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.snapshot = Objects.requireNonNull(initial, "initial condition must not be null");
    }

    /** 添加标准或已注册的业务条件，例如 {@code where("userId", "user-in-org", orgId)}。 */
    public WhereDsl where(String field, String operator, Object value) {
        mutableBuilder().add(SqlIdentifiers.requireIdentifier(field), operator, value);
        return this;
    }

    /** 追加括号包裹的 AND 组；组内继承当前条件注册表。 */
    public WhereDsl and(Consumer<WhereDsl> consumer) {
        Objects.requireNonNull(consumer, "AND condition consumer must not be null");
        mutableBuilder().and(nested -> consumer.accept(new WhereDsl(nested)));
        return this;
    }

    /** 追加括号包裹的 OR 组；该组作为整体与外层条件组合。 */
    public WhereDsl or(Consumer<WhereDsl> consumer) {
        Objects.requireNonNull(consumer, "OR condition consumer must not be null");
        mutableBuilder().or(nested -> consumer.accept(new WhereDsl(nested)));
        return this;
    }

    /**
     * 添加严格的等值条件。值为 null 或空文本时不会在这里静默丢弃。
     *
     * @param field 字段名
     * @param value 条件值
     * @return 当前 DSL
     */
    public WhereDsl is(String field, Object value) {
        return where(field, "=", value);
    }

    /**
     * 添加可选扩展条件。是否跳过只由统一的值清理策略决定，不允许 operator 自己拼 SQL。
     *
     * @param field 字段名
     * @param operator 已注册的 operator
     * @param value 可选条件值
     * @return 当前 DSL
     */
    public WhereDsl whereIfPresent(String field, String operator, Object value) {
        mutableBuilder().whereIfPresent(SqlIdentifiers.requireIdentifier(field), operator, value);
        return this;
    }

    ConditionGroup build() {
        if (snapshot == null) {
            snapshot = builder.build();
        }
        return snapshot;
    }

    /** 只追加已冻结的新节点；不重建已有条件，OR 组仍作为整体保留。 */
    void append(ConditionGroup conditions) {
        if (conditions.children().isEmpty()) {
            return;
        }
        appendTo(mutableBuilder(), conditions);
    }

    private static void appendTo(ConditionGroup.Builder target, ConditionGroup conditions) {
        if (conditions.operator() == LogicalOperator.AND) {
            conditions.children().forEach(target::add);
        } else {
            target.add(conditions);
        }
    }

    private ConditionGroup.Builder mutableBuilder() {
        if (builder == null) {
            builder = renderer.conditions();
            appendTo(builder, snapshot);
        }
        snapshot = null;
        return builder;
    }
}
