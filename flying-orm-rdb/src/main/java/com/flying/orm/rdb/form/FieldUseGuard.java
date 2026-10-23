package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldDecision;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.dialect.DialectCapabilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 字段用途与查询形状的中央审批点。
 *
 * <p>受治理路径在一次结构遍历里同时收集字段用途；bind 数量和 SQL 长度直接读取已经生成的
 * {@link SqlRequest}，不会再次扫描条件树。审批结果只属于当前调用，不能进入结构缓存。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class FieldUseGuard {

    private FieldUseGuard() {
    }

    /** 无限制配置使用普通路径，不因等值副本创建 requirements、budget 或 envelope。 */
    static boolean governed(FieldUsePolicy policy, QueryShapeLimits limits) {
        return Objects.requireNonNull(policy, "field use policy must not be null")
                       != FieldUsePolicy.unrestricted()
                || !Objects.requireNonNull(limits, "query shape limits must not be null")
                       .equals(QueryShapeLimits.defaults());
    }

    static <T> GovernedPlanEnvelope<T> query(T plan,
                                              FormDataSqlRenderer renderer,
                                              QuerySpec spec,
                                              ConditionGroup businessWhere,
                                              List<String> outputFields,
                                              List<String> sortFields,
                                              int callerSortCount,
                                              boolean publishesCursor,
                                              DataScope scope,
                                              SqlRequest request,
                                              FieldUsePolicy policy,
                                              QueryShapeBudget budget) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        List<String> safeOutputFields = Objects.requireNonNull(
                outputFields, "query output fields must not be null");
        List<String> safeSortFields = Objects.requireNonNull(
                sortFields, "query sort fields must not be null");
        if (callerSortCount < 0 || callerSortCount > safeSortFields.size()) {
            throw new IllegalArgumentException("caller sort count must match effective query sorts");
        }
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        for (String field : safeOutputFields) {
            ValueExpression expression = safeSpec.expressions().isEmpty() ? null
                    : safeSpec.expressions().get(FieldIdentity.of(field).key());
            if (expression == null) requirements.require(field, FieldUse.PROJECT);
            else expression.forEachField(source -> {
                requirements.require(source.identity().name(), FieldUse.PROJECT);
                requirements.require(source.identity().name(), FieldUse.FULL_VALUE);
            });
        }
        safeSpec.groups().forEach(field -> requirements.require(field, FieldUse.GROUP));
        for (int index = 0; index < safeSortFields.size(); index++) {
            String field = safeSortFields.get(index);
            if (publishesCursor) {
                // legacy cursor 直接公开排序值，不是上层签名或加密后的不透明令牌。
                requirements.require(field, FieldUse.PROJECT, FieldUseOrigin.CALLER);
            }
            FieldUseOrigin origin = index < callerSortCount
                    ? FieldUseOrigin.CALLER : FieldUseOrigin.INTERNAL_TIE_BREAKER;
            ValueExpression expression = safeSpec.expressions().isEmpty() ? null
                    : safeSpec.expressions().get(FieldIdentity.of(field).key());
            if (expression == null) requirements.require(field, FieldUse.SORT, origin);
            else expression.forEachField(source -> requirements.require(source.identity().name(), FieldUse.SORT, origin));
        }
        collectCondition(businessWhere, FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         renderer.conditionRenderer().terms(), renderer.dialectCapabilities());
        accountRenderedRequest(budget, request);
        FieldUseSnapshot snapshot = approve(
                safeSpec.form().id(), requirements.build(), scope.fields(), policy,
                safeSpec.projections().isEmpty());
        if (!safeSpec.expressions().isEmpty() && !snapshot.isUnrestricted()) {
            List<FieldDecision> decisions = new ArrayList<>(snapshot.decisions());
            for (String field : safeOutputFields) {
                if (safeSpec.expressions().containsKey(FieldIdentity.of(field).key())) {
                    // 别名来自已获准 PROJECT/FULL_VALUE 的全部来源，不把别名当作实体字段授权。
                    decisions.add(new FieldDecision(field, FieldUse.PROJECT, FieldUseOrigin.CALLER, true,
                            FieldVisibility.FULL));
                }
            }
            snapshot = FieldUseSnapshot.of(decisions);
        }
        if (publishesCursor) {
            requireFullCursorVisibility(safeSpec.form().id(), safeSortFields, snapshot);
        }
        return new GovernedPlanEnvelope<>(plan, snapshot);
    }

    private static void requireFullCursorVisibility(String resource,
                                                    List<String> sortFields,
                                                    FieldUseSnapshot snapshot) {
        for (String field : sortFields) {
            if (snapshot.visibility(field) != FieldVisibility.FULL) {
                throw new ScopeAccessException(
                        ScopeErrorCode.FIELD_NOT_READABLE,
                        resource,
                        field,
                        "cursor field [" + field + "] requires FULL visibility");
            }
        }
    }

    static <T> GovernedPlanEnvelope<T> write(T plan,
                                              FormDataSqlRenderer renderer,
                                              WriteSpec spec,
                                              DataScope scope,
                                              SqlRequest request,
                                              FieldUsePolicy policy,
                                              QueryShapeLimits limits) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "write spec must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        FieldUse use = switch (safeSpec.operation()) {
            case INSERT -> FieldUse.INSERT;
            case UPDATE -> FieldUse.UPDATE;
            case DELETE -> null;
        };
        if (use != null) {
            safeSpec.ownedValues().keySet().forEach(field -> requirements.require(field, use));
            FormUpdateExpressions.sources(safeSpec.ownedValues(), field -> requirements.require(field, FieldUse.FULL_VALUE));
        }
        collectCondition(safeSpec.where(), FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         renderer.conditionRenderer().terms(), renderer.dialectCapabilities());
        safeSpec.lock().ifPresent(lock -> {
            requirements.require(lock.field(), FieldUse.FILTER, FieldUseOrigin.INTERNAL_VERSION);
            if (safeSpec.operation() == com.flying.orm.rdb.form.spec.WriteOperation.UPDATE) {
                requirements.require(lock.field(), FieldUse.UPDATE, FieldUseOrigin.INTERNAL_VERSION);
            }
        });
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        accountRenderedRequest(budget, request);
        FieldUseSnapshot snapshot = approve(
                safeSpec.form().id(), requirements.build(), scope.fields(), policy, false);
        return new GovernedPlanEnvelope<>(plan, snapshot);
    }

    /** 批量 insert/upsert 在首行及字段布局切换时审批，未变化的布局复用审批结果。 */
    static void approveBatchInsert(DynamicForm form, Map<String, Object> firstRow, DataScope scope,
                                   boolean upsert, FieldUsePolicy policy, List<String> conflictFields) {
        DynamicForm safeForm = Objects.requireNonNull(form, "batch form must not be null");
        Map<String, Object> safeRow = Objects.requireNonNull(firstRow, "batch first row must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "batch data scope must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        RepositoryUpsertValues staged = upsert && safeRow instanceof RepositoryUpsertValues values
                ? values : null;
        Map<String, Object> inserts = staged == null ? safeRow : staged.insertValues();
        inserts.keySet().forEach(field -> requirements.require(field, FieldUse.INSERT));
        if (upsert) {
            Map<String, Object> updates = staged == null ? safeRow : staged.updateValues();
            updates.keySet().forEach(field -> {
                DynamicField definition = safeForm.field(field);
                if (!conflictFields.contains(definition.normalizedName())
                        && UpsertFieldPlan.updateCandidate(safeForm, definition, staged != null)) {
                    requirements.require(field, FieldUse.UPDATE);
                }
            });
        }
        approve(safeForm.id(), requirements.build(), safeScope.fields(), policy, false);
    }

    /** 固定的 UPSERT 业务条件只检查一次，不作为可信 Scope 绕过字段用途。 */
    static void approveUpsertCondition(FormDataSqlRenderer renderer, DynamicForm form, ConditionGroup condition,
                                       DataScope scope, FieldUsePolicy policy) {
        if (condition.children().isEmpty() || policy == FieldUsePolicy.unrestricted()
                && renderer.conditionRenderer().terms() == TermRegistry.standard()
                && scope.fields().unrestrictedRead()) return;
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        collectCondition(condition, FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                renderer.conditionRenderer().terms(), renderer.dialectCapabilities());
        approve(form.id(), requirements.build(), scope.fields(), policy, false);
    }

    /** 每行独立审批 SET、条件与版本字段；未配置治理时不分配审批结构。 */
    static void approveBatchUpdate(FormDataSqlRenderer renderer,
                                   DynamicForm form,
                                   BatchUpdate firstUpdate,
                                   DataScope scope,
                                   FieldUsePolicy policy) {
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data SQL renderer must not be null");
        DynamicForm safeForm = Objects.requireNonNull(form, "batch form must not be null");
        BatchUpdate safeUpdate = Objects.requireNonNull(
                firstUpdate, "batch first update must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "batch data scope must not be null");
        if (policy == FieldUsePolicy.unrestricted()
                && safeRenderer.conditionRenderer().terms() == TermRegistry.standard()
                && safeScope.fields().unrestrictedRead() && safeScope.fields().unrestrictedWrite()) return;
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        safeUpdate.ownedValues().keySet().forEach(field -> requirements.require(field, FieldUse.UPDATE));
        FormUpdateExpressions.sources(safeUpdate.ownedValues(), field -> requirements.require(field, FieldUse.FULL_VALUE));
        collectCondition(safeUpdate.where(), FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         safeRenderer.conditionRenderer().terms(), safeRenderer.dialectCapabilities());
        if (safeUpdate.lock() != null) {
            requirements.require(safeUpdate.lock().field(), FieldUse.FILTER, FieldUseOrigin.INTERNAL_VERSION);
            requirements.require(safeUpdate.lock().field(), FieldUse.UPDATE, FieldUseOrigin.INTERNAL_VERSION);
        }
        approve(safeForm.id(), requirements.build(), safeScope.fields(), policy, false);
    }

    static <T> GovernedPlanEnvelope<T> keyset(
            T plan,
            FormDataSqlRenderer renderer,
            QuerySpec spec,
            ConditionGroup businessWhere,
            List<String> outputFields,
            KeysetPageNormalizer.NormalizedKeysetPage page,
            DataScope scope,
            SqlRequest request,
            FieldUsePolicy policy,
            QueryShapeBudget budget) {
        return query(plan, renderer, spec, businessWhere, outputFields,
                page.sorts().stream().map(com.flying.orm.core.page.KeysetSort::field).toList(),
                page.callerSortCount(), true, scope, request, policy, budget);
    }

    /**
     * 类型化聚合的单次中央审批入口。planner 在验证字段/别名的同一遍遍历中给出计数，
     * 这里继续复用统一预算错误和字段拒绝语义，并在 SQL 生成后补记 bind 与文本长度。
     */
    static FieldUseSnapshot approveAggregate(String resource,
                                             FieldUseRequirements requirements,
                                             FieldScope scope,
                                             SqlRequest request,
                                             FieldUsePolicy policy,
                                             QueryShapeLimits limits,
                                             int projectionCount,
                                             int groupCount,
                                             int aggregateCount,
                                             int havingNodeCount,
                                             int sortCount) {
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        budget.addProjections(projectionCount);
        budget.addGroups(groupCount);
        budget.addAggregates(aggregateCount);
        budget.addHavingNodes(havingNodeCount);
        budget.addSorts(sortCount);
        accountRenderedRequest(budget, request);
        return approve(resource, requirements, scope, policy, false);
    }

    /** 受治理结果先以 FULL 解码，再在发布边界逐字段选择 full、masked 或 hidden。 */
    static DynamicRow applyVisibility(FormDataSqlRenderer renderer,
                                      DynamicForm form,
                                      DynamicRow row,
                                      FieldUseSnapshot snapshot) {
        return FieldVisibilityPublisher.publish(renderer, form, row, snapshot);
    }

    /** 按同一读取计划发布普通字段或关联别名，执行终端不再分别判断来源。 */
    static DynamicRow applyVisibility(FormDataSqlRenderer renderer, FormReadPlanner.RowReadPlan plan,
                                      DynamicRow row, FieldUseSnapshot snapshot) {
        return plan.joinSpec() == null ? applyVisibility(renderer, plan.form(), row, snapshot)
                : applyJoinVisibility(renderer, plan.joinSpec(), row, snapshot);
    }

    /** 在规划边界按策略来源选择基础显示模式；Scope 审批快照不能证明启用了显式显示授权。 */
    static SensitiveDisplayMode effectiveDisplayMode(FieldUsePolicy policy,
                                                      SensitiveDisplayMode declared) {
        return Objects.requireNonNull(policy, "field use policy must not be null").isUnrestricted()
                ? Objects.requireNonNull(declared, "declared display mode must not be null")
                : SensitiveDisplayMode.FULL;
    }

    /** JOIN 的策略快照按源字段判定、按结果别名发布。 */
    static DynamicRow applyJoinVisibility(FormDataSqlRenderer renderer,
                                          JoinQuerySpec spec,
                                          DynamicRow row,
                                          FieldUseSnapshot snapshot) {
        return FieldVisibilityPublisher.publishJoin(renderer, spec, row, snapshot);
    }

    static void collectCondition(ConditionNode node,
                                 FieldUse use,
                                 FieldUseOrigin origin,
                                 FieldUseRequirements.Builder requirements) {
        ConditionNode safeNode = Objects.requireNonNull(node, "condition node must not be null");
        ConditionNodes.forEachTerm(safeNode, term -> requirements.require(term.field(), use, origin));
    }

    static void collectCondition(ConditionNode node,
                                 FieldUse use,
                                 FieldUseOrigin origin,
                                 FieldUseRequirements.Builder requirements,
                                 TermRegistry terms,
                                 DialectCapabilities capabilities) {
        ConditionNode safeNode = Objects.requireNonNull(node, "condition node must not be null");
        ConditionNodes.forEachTerm(safeNode, term -> {
            requirements.require(term.field(), use, origin);
            GovernedTermGuard.require(term, use, terms, capabilities);
        });
    }

    /** 聚合/JOIN 等独立规划器在自己的既有遍历中复用同一条扩展治理规则。 */
    static void approveTermExtension(FormDataSqlRenderer renderer,
                                     TermCondition term,
                                     FieldUse use) {
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data SQL renderer must not be null");
        GovernedTermGuard.require(
                Objects.requireNonNull(term, "term condition must not be null"),
                Objects.requireNonNull(use, "term field use must not be null"),
                safeRenderer.conditionRenderer().terms(),
                safeRenderer.dialectCapabilities());
    }

    static FieldUseSnapshot approve(String resource,
                                    FieldUseRequirements requirements,
                                    FieldScope scope,
                                    FieldUsePolicy policy,
                                    boolean implicitProjection) {
        FieldUseSnapshot snapshot = Objects.requireNonNull(policy, "field use policy must not be null")
                                                  .approve(requirements, scope);
        if (implicitProjection && !snapshot.isUnrestricted()) {
            List<FieldDecision> effective = snapshot.decisions().stream()
                    .filter(decision -> decision.allowed()
                            || decision.use() != FieldUse.PROJECT
                            || decision.origin() != FieldUseOrigin.CALLER)
                    .toList();
            snapshot = FieldUseSnapshot.of(effective);
        }
        if (snapshot.allowed()) return snapshot;
        for (FieldDecision decision : snapshot.deniedDecisions()) {
            boolean write = decision.use().write();
            throw new ScopeAccessException(
                    write ? ScopeErrorCode.FIELD_NOT_WRITABLE : ScopeErrorCode.FIELD_NOT_READABLE,
                    resource,
                    decision.field(),
                    "field [" + decision.field() + "] is not allowed for "
                            + decision.use() + " from " + decision.origin());
        }
        return snapshot;
    }

    static void accountRenderedRequest(QueryShapeBudget budget, SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "planned SQL request must not be null");
        budget.addBinds(safeRequest.statement().parameterCount());
        budget.addSqlLength(safeRequest.sql().length());
    }
}
