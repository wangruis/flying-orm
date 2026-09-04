package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicForm;
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
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.dialect.DialectCapabilities;

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
public final class FieldUseGuard {

    private FieldUseGuard() {
    }

    /** 默认 singleton 组合就是 legacy 快路，不创建 requirements、budget 或 envelope。 */
    static boolean governed(FieldUsePolicy policy, QueryShapeLimits limits) {
        return Objects.requireNonNull(policy, "field use policy must not be null")
                       != FieldUsePolicy.unrestricted()
                || Objects.requireNonNull(limits, "query shape limits must not be null")
                       != QueryShapeLimits.defaults();
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
                                              QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        List<String> safeOutputFields = List.copyOf(Objects.requireNonNull(
                outputFields, "query output fields must not be null"));
        List<String> safeSortFields = Objects.requireNonNull(
                sortFields, "query sort fields must not be null");
        if (callerSortCount < 0 || callerSortCount > safeSortFields.size()) {
            throw new IllegalArgumentException("caller sort count must match effective query sorts");
        }
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        budget.addProjections(safeOutputFields.size());
        for (String field : safeOutputFields) {
            requirements.require(field, FieldUse.PROJECT);
        }
        budget.addGroups(safeSpec.groups().size());
        safeSpec.groups().forEach(field -> requirements.require(field, FieldUse.GROUP));
        budget.addSorts(safeSortFields.size());
        for (int index = 0; index < safeSortFields.size(); index++) {
            String field = safeSortFields.get(index);
            if (publishesCursor) {
                // legacy cursor 直接公开排序值，不是上层签名或加密后的不透明令牌。
                requirements.require(field, FieldUse.PROJECT, FieldUseOrigin.CALLER);
            }
            requirements.require(
                    field,
                    FieldUse.SORT,
                    index < callerSortCount
                            ? FieldUseOrigin.CALLER : FieldUseOrigin.INTERNAL_TIE_BREAKER);
        }
        collectCondition(businessWhere, FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         renderer.conditionRenderer().terms(), renderer.dialectCapabilities());
        accountRenderedRequest(budget, request);
        FieldUseSnapshot snapshot = approve(
                safeSpec.form().id(), requirements.build(), scope.fields(), policy,
                safeSpec.projections().isEmpty());
        if (publishesCursor) {
            requireFullCursorVisibility(safeSpec.form().id(), safeSortFields, snapshot);
        }
        return new GovernedPlanEnvelope<>(plan, snapshot);
    }

    private static void requireFullCursorVisibility(String resource,
                                                    List<String> sortFields,
                                                    FieldUseSnapshot snapshot) {
        for (String field : sortFields) {
            if (snapshot.visibility(field) != com.flying.orm.core.scope.FieldVisibility.FULL) {
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

    /** 批量 insert/upsert 只审批首行已经固定的共享字段布局；后续行必须匹配同一布局。 */
    static void approveBatchInsert(DynamicForm form,
                                   Map<String, Object> firstRow,
                                   DataScope scope,
                                   boolean upsert,
                                   FieldUsePolicy policy) {
        DynamicForm safeForm = Objects.requireNonNull(form, "batch form must not be null");
        Map<String, Object> safeRow = Objects.requireNonNull(firstRow, "batch first row must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "batch data scope must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        safeRow.keySet().forEach(field -> {
            requirements.require(field, FieldUse.INSERT);
            if (upsert) {
                requirements.require(field, FieldUse.UPDATE);
            }
        });
        approve(safeForm.id(), requirements.build(), safeScope.fields(), policy, false);
    }

    /** 批量 update 的共享布局、条件与版本字段在第一行编译 SQL 前一次审批。 */
    static void approveBatchUpdate(FormDataSqlRenderer renderer,
                                   DynamicForm form,
                                   BatchOptimisticUpdate firstUpdate,
                                   DataScope scope,
                                   FieldUsePolicy policy) {
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data SQL renderer must not be null");
        DynamicForm safeForm = Objects.requireNonNull(form, "batch form must not be null");
        BatchOptimisticUpdate safeUpdate = Objects.requireNonNull(
                firstUpdate, "batch first update must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "batch data scope must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        safeUpdate.ownedValues().keySet().forEach(field -> requirements.require(field, FieldUse.UPDATE));
        collectCondition(safeUpdate.where(), FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         safeRenderer.conditionRenderer().terms(), safeRenderer.dialectCapabilities());
        requirements.require(safeUpdate.lock().field(), FieldUse.FILTER, FieldUseOrigin.INTERNAL_VERSION);
        requirements.require(safeUpdate.lock().field(), FieldUse.UPDATE, FieldUseOrigin.INTERNAL_VERSION);
        approve(safeForm.id(), requirements.build(), safeScope.fields(), policy, false);
    }

    static <T> GovernedPlanEnvelope<T> keyset(
            T plan,
            FormDataSqlRenderer renderer,
            QuerySpec spec,
            ConditionGroup businessWhere,
            List<String> outputFields,
            KeysetPageNormalizer.NormalizedKeysetPage page,
            HiddenProjectionLayout layout,
            DataScope scope,
            SqlRequest request,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        List<String> safeOutputFields = List.copyOf(Objects.requireNonNull(
                outputFields, "keyset output fields must not be null"));
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        safeOutputFields.forEach(field -> requirements.require(field, FieldUse.PROJECT));
        KeysetCursorVisibilityGuard.collectRequirements(requirements, page);
        collectCondition(businessWhere, FieldUse.FILTER, FieldUseOrigin.CALLER, requirements,
                         renderer.conditionRenderer().terms(), renderer.dialectCapabilities());
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        budget.addProjections(layout.selections().size());
        budget.addSorts(page.sorts().size());
        accountRenderedRequest(budget, request);
        FieldUseSnapshot snapshot = approve(
                safeSpec.form().id(), requirements.build(), scope.fields(), policy,
                safeSpec.projections().isEmpty());
        KeysetCursorVisibilityGuard.requireFull(safeSpec.form().id(), page, snapshot);
        return new GovernedPlanEnvelope<>(plan, snapshot);
    }

    static FieldUseSnapshot approveCollected(String resource,
                                             FieldUseRequirements requirements,
                                             FieldScope scope,
                                             SqlRequest request,
                                             FieldUsePolicy policy,
                                             QueryShapeBudget budget) {
        accountRenderedRequest(budget, request);
        return approve(resource, requirements, scope, policy, false);
    }

    /**
     * 类型化聚合的单次中央审批入口。planner 在验证字段/别名的同一遍遍历中给出计数，
     * 这里继续复用统一预算错误和字段拒绝语义，并在 SQL 生成后补记 bind 与文本长度。
     */
    @com.flying.orm.rdb.internal.InternalApi
    public static FieldUseSnapshot approveAggregate(String resource,
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
        if (safeNode instanceof TermCondition term) {
            requirements.require(term.field(), use, origin);
            return;
        }
        if (safeNode instanceof ConditionGroup group) {
            for (ConditionNode child : group.children()) {
                collectCondition(child, use, origin, requirements);
            }
            return;
        }
        throw new IllegalArgumentException("unsupported condition node for field governance");
    }

    static void collectCondition(ConditionNode node,
                                 FieldUse use,
                                 FieldUseOrigin origin,
                                 FieldUseRequirements.Builder requirements,
                                 TermRegistry terms,
                                 DialectCapabilities capabilities) {
        ConditionNode safeNode = Objects.requireNonNull(node, "condition node must not be null");
        if (safeNode instanceof TermCondition term) {
            requirements.require(term.field(), use, origin);
            GovernedTermGuard.require(term, use, terms, capabilities);
            return;
        }
        if (safeNode instanceof ConditionGroup group) {
            for (ConditionNode child : group.children()) {
                collectCondition(child, use, origin, requirements, terms, capabilities);
            }
            return;
        }
        throw new IllegalArgumentException("unsupported condition node for field governance");
    }

    /** 聚合/JOIN 等独立规划器在自己的既有遍历中复用同一条扩展治理规则。 */
    @com.flying.orm.rdb.internal.InternalApi
    public static void approveTermExtension(FormDataSqlRenderer renderer,
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
        budget.addBinds(safeRequest.parameters().size());
        budget.addSqlLength(safeRequest.sql().length());
    }
}
