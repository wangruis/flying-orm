package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinFieldPair;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinOrder;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;

import java.util.Map;
import java.util.Objects;

/**
 * 在 JOIN SQL 生成前按每个源的 FieldScope 校验投影、ON、条件和排序字段。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinReadGuard {

    private JoinReadGuard() {
    }

    static void validate(JoinQuerySpec spec, Map<JoinSource, DynamicForm> readableForms) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, DynamicForm> safeForms = Objects.requireNonNull(
                readableForms, "join readable forms must not be null");
        for (JoinProjection projection : safeSpec.projections()) {
            requireField(projection.field(), safeForms);
        }
        for (JoinOrder order : safeSpec.orders()) {
            requireField(order.field(), safeForms);
            requireUnencrypted(order.field(), "encrypted field must not be used for join ordering");
        }
        for (JoinClause join : safeSpec.joins()) {
            for (JoinFieldPair pair : join.on()) {
                requireField(pair.left(), safeForms);
                requireField(pair.right(), safeForms);
                requireUnencrypted(pair.left(), "encrypted field must not be used as a join key");
                requireUnencrypted(pair.right(), "encrypted field must not be used as a join key");
            }
        }
        for (JoinSource source : safeSpec.sources()) {
            validateCondition(safeSpec.where(source), source, safeForms);
        }
    }

    private static void validateCondition(ConditionNode node,
                                          JoinSource source,
                                          Map<JoinSource, DynamicForm> readableForms) {
        if (node instanceof TermCondition term) {
            requireField(new JoinFieldRef(source, term.field()), readableForms);
            return;
        }
        if (node instanceof ConditionGroup group) {
            group.children().forEach(child -> validateCondition(child, source, readableForms));
            return;
        }
        throw new IllegalArgumentException("unsupported join condition node");
    }

    private static void requireField(JoinFieldRef field, Map<JoinSource, DynamicForm> readableForms) {
        DynamicForm readable = readableForms.get(field.source());
        if (readable == null) {
            throw new IllegalArgumentException("join source is not part of the readable query");
        }
        readable.field(field.field());
    }

    private static void requireUnencrypted(JoinFieldRef field, String message) {
        DynamicForm form = field.source().form();
        if (form.protections().encrypted(field.field()).isPresent()) {
            throw new IllegalArgumentException(message);
        }
    }
}
