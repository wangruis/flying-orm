package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinOrder;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

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

    /** 输出形状独立校验；聚合等消费者不必声明普通行的投影或排序用途。 */
    static void validateOutput(JoinQuerySpec spec,
                               Map<JoinSource, DynamicForm> readableForms,
                               FieldUseRequirements.Builder requirements,
                               QueryShapeBudget budget) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, DynamicForm> safeForms = Objects.requireNonNull(
                readableForms, "join readable forms must not be null");
        if (budget != null) {
            budget.addProjections(safeSpec.projections().size());
            budget.addSorts(safeSpec.orders().size());
        }
        for (JoinProjection projection : safeSpec.projections()) {
            if (projection.expression() == null) {
                requireGrouped(spec, projection.field());
                requireField(projection.field(), safeForms);
                require(requirements, projection.field(), FieldUse.PROJECT);
            } else projection.expression().forEachField(source -> {
                JoinFieldRef field = new JoinFieldRef(source.source(), source.identity().name());
                requireGrouped(spec, field);
                requireField(field, safeForms);
                require(requirements, field, FieldUse.PROJECT);
                require(requirements, field, FieldUse.FULL_VALUE);
            });
        }
        for (JoinOrder order : safeSpec.orders()) {
            if (order.expression() == null) requireSort(spec, order.field(), safeForms, requirements);
            else order.expression().forEachField(reference -> requireSort(
                    spec, new JoinFieldRef(reference.source(), reference.identity().name()), safeForms, requirements));
        }
    }

    private static void requireGrouped(JoinQuerySpec spec, JoinFieldRef field) {
        if (!spec.groups().isEmpty() && !spec.groups().contains(field)) {
            throw new IllegalArgumentException("grouped join query requires grouped source fields");
        }
    }

    private static void requireSort(JoinQuerySpec spec, JoinFieldRef field, Map<JoinSource, DynamicForm> forms,
                                    FieldUseRequirements.Builder requirements) {
        requireGrouped(spec, field);
        requireField(field, forms);
        requireUnencrypted(field, "encrypted field must not be used for join ordering");
        require(requirements, field, FieldUse.SORT);
    }

    /** 校验和字段用途/形状计费共用这一遍关系输入遍历。 */
    static void validateRelations(JoinQuerySpec spec,
                                  Map<JoinSource, DynamicForm> readableForms,
                                  FieldUseRequirements.Builder requirements,
                                  QueryShapeBudget budget,
                                  FormDataSqlRenderer renderer) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, DynamicForm> safeForms = Objects.requireNonNull(
                readableForms, "join readable forms must not be null");
        if (budget != null) {
            budget.addJoins(safeSpec.joins().size());
            budget.addGroups(safeSpec.groups().size());
        }
        for (JoinFieldRef group : safeSpec.groups()) {
            requireField(group, safeForms);
            requireUnencrypted(group, "encrypted field must not be used for join grouping");
            require(requirements, group, FieldUse.GROUP);
        }
        for (JoinClause join : safeSpec.joins()) {
            join.condition().forEachLeaf(leaf -> {
                if (leaf instanceof JoinCondition.Comparison comparison) {
                    requireField(comparison.left(), safeForms);
                    requireField(comparison.right(), safeForms);
                    requireUnencrypted(comparison.left(), "encrypted field must not be used as a join key");
                    requireUnencrypted(comparison.right(), "encrypted field must not be used as a join key");
                    require(requirements, comparison.left(), FieldUse.JOIN);
                    require(requirements, comparison.right(), FieldUse.JOIN);
                } else if (leaf instanceof JoinCondition.Values values) {
                    validateCondition(values.condition(), values.source(), safeForms, requirements, renderer, FieldUse.JOIN);
                }
            });
        }
        if (safeSpec.where() != null) safeSpec.where().forEachLeaf(leaf -> {
            if (leaf instanceof JoinCondition.Values values) {
                validateCondition(values.condition(), values.source(), safeForms, requirements, renderer, FieldUse.FILTER);
            }
        });
    }

    private static void validateCondition(ConditionNode node,
                                          JoinSource source,
                                          Map<JoinSource, DynamicForm> readableForms,
                                          FieldUseRequirements.Builder requirements,
                                          FormDataSqlRenderer renderer,
                                          FieldUse use) {
        ConditionNodes.forEachTerm(node, term -> {
            JoinFieldRef field = new JoinFieldRef(source, term.field());
            requireField(field, readableForms);
            require(requirements, field, use);
            if (renderer != null) {
                FieldUseGuard.approveTermExtension(renderer, term, use);
            }
        });
    }

    private static void require(FieldUseRequirements.Builder requirements,
                                JoinFieldRef field,
                                FieldUse use) {
        if (requirements != null) {
            requirements.requireJoin(field, use);
        }
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
