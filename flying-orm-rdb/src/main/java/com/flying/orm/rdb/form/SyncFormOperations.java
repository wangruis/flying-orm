package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.lock.LockingReadRequiredTransactionException;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;

/**
 * 原生 JDBC 表单单条操作执行边界。
 *
 * <p>安全条件和 SQL 全部由 {@link FormOperationPlanner} 生成，本类只把计划交给同步执行器并整理结果。
 * 它不引用 Reactor，也不从响应式客户端等待结果。</p>
 */
final class SyncFormOperations {

    private final SyncSqlExecutor executor;
    private final FormOperationPlanner planner;
    private final FormResultDecoder decoder;
    private final SyncJoinOperations joins;
    private final SyncFormWriteOperations writes;
    private final ProtectedContainsResultSupport containsResults;
    private final FormDataSqlRenderer renderer;
    private final FieldUsePolicy fieldUsePolicy;
    private final QueryShapeLimits queryShapeLimits;
    private final boolean governed;

    /** 保留包内既有构造契约；legacy 调用固定使用两个静态默认 singleton。 */
    SyncFormOperations(SyncSqlExecutor executor,
                       FormDataSqlRenderer renderer,
                       StructuredConditionResolver resolver,
                       com.flying.orm.core.scope.DataScope defaultDataScope,
                       com.flying.orm.rdb.execution.SqlExecutionOptions defaultExecutionOptions,
                       EntityModelRegistry entityModels) {
        this(executor, renderer, resolver, defaultDataScope, defaultExecutionOptions, entityModels,
             FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
    }

    SyncFormOperations(SyncSqlExecutor executor,
                       FormDataSqlRenderer renderer,
                       StructuredConditionResolver resolver,
                       com.flying.orm.core.scope.DataScope defaultDataScope,
                       com.flying.orm.rdb.execution.SqlExecutionOptions defaultExecutionOptions,
                       EntityModelRegistry entityModels,
                       FieldUsePolicy fieldUsePolicy,
                       QueryShapeLimits queryShapeLimits) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        this.renderer = safeRenderer;
        this.fieldUsePolicy = Objects.requireNonNull(fieldUsePolicy, "field use policy must not be null");
        this.queryShapeLimits = Objects.requireNonNull(queryShapeLimits, "query shape limits must not be null");
        this.governed = FieldUseGuard.governed(this.fieldUsePolicy, this.queryShapeLimits);
        FormScopeSupport scopes = new FormScopeSupport(safeRenderer, resolver, defaultDataScope);
        this.planner = new FormOperationPlanner(safeRenderer, scopes, defaultExecutionOptions);
        this.containsResults = new ProtectedContainsResultSupport(safeRenderer);
        this.decoder = new FormResultDecoder(safeRenderer, entityModels);
        this.joins = new SyncJoinOperations(
                executor, safeRenderer, scopes, defaultExecutionOptions, decoder,
                this.fieldUsePolicy, this.queryShapeLimits, governed);
        this.writes = new SyncFormWriteOperations(
                executor, planner, this.fieldUsePolicy, this.queryShapeLimits, governed);
    }

    List<DynamicRow> selectJoin(JoinQuerySpec spec, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        return joins.select(spec, options);
    }

    FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) {
        return joins.previewFieldUse(spec);
    }

    <T> List<T> selectJoin(JoinQuerySpec spec,
                           com.flying.orm.rdb.execution.SqlExecutionOptions options,
                           RowMapper<T> mapper) {
        return joins.selectMapped(spec, options, mapper);
    }

    PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                    PageQuery page,
                                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        return joins.page(spec, page, options);
    }

    List<DynamicRow> select(QuerySpec spec) {
        return governed ? selectGoverned(spec, fieldUsePolicy, queryShapeLimits)
                : select(planner.select(spec));
    }

    List<DynamicRow> selectGoverned(QuerySpec spec,
                                    FieldUsePolicy policy,
                                    QueryShapeLimits limits) {
        return select(planner.selectGoverned(
                Objects.requireNonNull(spec, "query spec must not be null"),
                Objects.requireNonNull(policy, "field use policy must not be null"),
                Objects.requireNonNull(limits, "query shape limits must not be null")));
    }

    List<DynamicRow> lockingRead(LockingReadSpec spec) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingRead> envelope =
                    planner.lockingReadGoverned(spec, fieldUsePolicy, queryShapeLimits);
            requireCallerManagedTransaction();
            return select(new GovernedPlanEnvelope<>(
                    envelope.plan().query(), envelope.fieldUse()));
        }
        FormOperationPlanner.PlannedLockingRead plan = planner.lockingRead(spec);
        requireCallerManagedTransaction();
        return select(plan.query());
    }

    <T> List<T> lockingRead(LockingReadSpec spec, Class<T> type) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingRead> envelope =
                    planner.lockingReadGoverned(spec, fieldUsePolicy, queryShapeLimits);
            requireCallerManagedTransaction();
            return selectMapped(new GovernedPlanEnvelope<>(
                    envelope.plan().query(), envelope.fieldUse()), type, 0);
        }
        FormOperationPlanner.PlannedLockingRead plan = planner.lockingRead(spec);
        requireCallerManagedTransaction();
        return selectMapped(plan.query(), type, 0);
    }

    FieldUseSnapshot previewFieldUse(QuerySpec spec) {
        return governed
                ? planner.selectGoverned(spec, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    private List<DynamicRow> select(GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> envelope) {
        FormOperationPlanner.PlannedQuery plan = envelope.plan();
        SensitiveDisplayMode displayMode = plan.displayMode();
        List<DynamicRow> decoded;
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.request(), plan.options());
            ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
            List<DynamicRow> full = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(),
                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
            decoded = containsResults.finish(plan.form(), plan.containsQuery(), full,
                                             plan.outputFields(), displayMode);
        } else {
            return executor.queryMapped(
                    plan.request(), plan.options(), governedRowMapper(envelope), 0);
        }
        return decoded.stream()
                      .map(row -> FieldUseGuard.applyVisibility(renderer, plan.form(), row, envelope.fieldUse()))
                      .toList();
    }

    private List<DynamicRow> select(FormOperationPlanner.PlannedQuery plan) {
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.request(), plan.options());
            ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(),
                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
            return containsResults.finish(plan.form(), plan.containsQuery(), decoded,
                                          plan.outputFields(), plan.displayMode());
        }
        return decoder.decodeRows(
                plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                plan.scope(), plan.displayMode());
    }

    <T> List<T> select(QuerySpec spec, Class<T> type) {
        if (governed) {
            return selectMapped(
                    planner.selectGoverned(spec, fieldUsePolicy, queryShapeLimits), type, 0);
        }
        return selectMapped(planner.select(spec), type, 0);
    }

    <T> T selectOne(QuerySpec spec, Class<T> type) {
        if (governed) {
            List<T> rows = selectMapped(
                    planner.selectGoverned(spec, fieldUsePolicy, queryShapeLimits), type, 2);
            if (rows.isEmpty()) {
                return null;
            }
            if (rows.size() != 1) {
                throw new IllegalStateException("entity query expected zero or one row but received " + rows.size());
            }
            return rows.getFirst();
        }
        FormOperationPlanner.PlannedQuery plan = planner.select(spec);
        List<T> rows = selectMapped(plan, type, 2);
        if (rows.isEmpty()) {
            return null;
        }
        if (rows.size() != 1) {
            throw new IllegalStateException("entity query expected zero or one row but received " + rows.size());
        }
        return rows.getFirst();
    }

    PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedPage> envelope =
                    planner.pageGoverned(spec, page, fieldUsePolicy, queryShapeLimits);
            PageResult<DynamicRow> result = page(
                    envelope.plan(), envelope.plan().displayMode());
            List<DynamicRow> rows = result.rows().stream()
                    .map(row -> FieldUseGuard.applyVisibility(
                            renderer, envelope.plan().form(), row, envelope.fieldUse()))
                    .toList();
            return new PageResult<>(rows, result.total(), result.page(), result.size());
        }
        FormOperationPlanner.PlannedPage plan = planner.page(spec, page);
        return page(plan, plan.displayMode());
    }

    private PageResult<DynamicRow> page(FormOperationPlanner.PlannedPage plan,
                                        SensitiveDisplayMode displayMode) {
        return SyncFormPageResultSupport.page(
                executor, decoder, containsResults, plan, displayMode);
    }

    <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "page result type must not be null");
        return FormResultMappingSupport.mapPage(page(spec, page), mapper);
    }

    CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedCursorPage> envelope =
                    planner.cursorPageGoverned(spec, page, fieldUsePolicy, queryShapeLimits);
            CursorPageResult<DynamicRow> result = cursorPage(
                    envelope.plan(), envelope.plan().displayMode());
            List<DynamicRow> rows = result.rows().stream()
                    .map(row -> FieldUseGuard.applyVisibility(
                            renderer, envelope.plan().form(), row, envelope.fieldUse()))
                    .toList();
            return new CursorPageResult<>(rows, result.nextCursor(), result.hasMore());
        }
        FormOperationPlanner.PlannedCursorPage plan = planner.cursorPage(spec, page);
        return cursorPage(plan, plan.displayMode());
    }

    private CursorPageResult<DynamicRow> cursorPage(FormOperationPlanner.PlannedCursorPage plan,
                                                    SensitiveDisplayMode displayMode) {
        return SyncFormPageResultSupport.cursorPage(
                executor, decoder, containsResults, plan, displayMode);
    }

    <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "cursor page result type must not be null");
        return FormResultMappingSupport.mapCursorPage(cursorPage(spec, page), mapper);
    }

    KeysetPageResult<DynamicRow> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedKeysetPage> envelope =
                    planner.keysetPageGoverned(spec, page, fieldUsePolicy, queryShapeLimits);
            return keysetPage(envelope.plan(), envelope.fieldUse());
        }
        return keysetPage(planner.keysetPage(spec, page), null);
    }

    KeysetPageResult<DynamicRow> lockingRead(
            LockingReadSpec spec,
            KeysetPageQuery page) {
        if (governed) {
            GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingKeysetRead> envelope =
                    planner.lockingKeysetReadGoverned(
                            spec, page, fieldUsePolicy, queryShapeLimits);
            requireCallerManagedTransaction();
            return keysetPage(envelope.plan().query(), envelope.fieldUse());
        }
        FormOperationPlanner.PlannedLockingKeysetRead plan =
                planner.lockingKeysetRead(spec, page);
        requireCallerManagedTransaction();
        return keysetPage(plan.query(), null);
    }

    <T> KeysetPageResult<T> lockingRead(
            LockingReadSpec spec,
            KeysetPageQuery page,
            Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(
                type, "locking keyset result type must not be null");
        return FormResultMappingSupport.mapKeysetPage(lockingRead(spec, page), mapper);
    }

    private KeysetPageResult<DynamicRow> keysetPage(
            FormOperationPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse) {
        return SyncFormPageResultSupport.keysetPage(
                executor, decoder, renderer, plan, fieldUse);
    }

    <T> KeysetPageResult<T> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "keyset page result type must not be null");
        return FormResultMappingSupport.mapKeysetPage(keysetPage(spec, page), mapper);
    }

    long insert(WriteSpec spec) {
        return writes.insert(spec);
    }

    /** 生成键和影响行数必须来自同一个 JDBC Statement，不能在 insert 后另查当前序列值。 */
    SqlWriteResult insertReturningKeys(WriteSpec spec) {
        return writes.insertReturningKeys(spec);
    }

    long update(WriteSpec spec) {
        return writes.update(spec);
    }

    long delete(WriteSpec spec) {
        return writes.delete(spec);
    }

    long physicalDelete(WriteSpec spec) {
        return writes.physicalDelete(spec);
    }

    private void requireCallerManagedTransaction() {
        java.util.Optional<com.flying.orm.rdb.transaction.JdbcTransactionContext> transaction =
                Objects.requireNonNull(
                        executor.currentTransaction(),
                        "current JDBC transaction lookup must not return null");
        if (transaction.isEmpty()) {
            throw new LockingReadRequiredTransactionException();
        }
    }

    private <T> List<T> selectMapped(FormOperationPlanner.PlannedQuery plan, Class<T> type, int rowLimit) {
        if (plan.contains()) {
            RowMapper<T> mapper = decoder.rowMapper(type, "form result type must not be null");
            return select(plan).stream().map(mapper::map).toList();
        }
        RowMapper<DynamicRow> rowDecoder = decoder.rowDecoder(
                plan.form(), plan.options(), plan.scope(), plan.displayMode());
        RowMapper<T> entityMapper = decoder.rowMapper(type, "form result type must not be null");
        return executor.queryMapped(
                plan.request(), plan.options(), row -> entityMapper.map(rowDecoder.map(row)), rowLimit);
    }

    private RowMapper<DynamicRow> governedRowMapper(
            GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> envelope) {
        FormOperationPlanner.PlannedQuery plan = envelope.plan();
        RowMapper<DynamicRow> rowDecoder = decoder.rowDecoder(
                plan.form(), plan.options(), plan.scope(),
                plan.displayMode());
        return row -> FieldUseGuard.applyVisibility(
                renderer, plan.form(), rowDecoder.map(row), envelope.fieldUse());
    }

    private <T> List<T> selectMapped(
            GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> envelope,
            Class<T> type,
            int rowLimit) {
        FormOperationPlanner.PlannedQuery plan = envelope.plan();
        RowMapper<T> entityMapper = decoder.rowMapper(type, "form result type must not be null");
        if (plan.contains()) {
            List<DynamicRow> rows = select(envelope);
            int size = rowLimit == 0 ? rows.size() : Math.min(rowLimit, rows.size());
            java.util.ArrayList<T> mapped = new java.util.ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                mapped.add(entityMapper.map(rows.get(index)));
            }
            return List.copyOf(mapped);
        }
        RowMapper<DynamicRow> rowMapper = governedRowMapper(envelope);
        return executor.queryMapped(
                plan.request(), plan.options(), row -> entityMapper.map(rowMapper.map(row)), rowLimit);
    }

}
