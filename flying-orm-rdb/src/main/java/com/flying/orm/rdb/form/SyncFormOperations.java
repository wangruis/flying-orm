package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
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
    private final JoinQueryPlanner joinPlanner;
    private final JoinResultProtector joinResults;
    private final ProtectedContainsResultSupport containsResults;

    SyncFormOperations(SyncSqlExecutor executor,
                       FormDataSqlRenderer renderer,
                       StructuredConditionResolver resolver,
                       com.flying.orm.core.scope.DataScope defaultDataScope,
                       com.flying.orm.rdb.execution.SqlExecutionOptions defaultExecutionOptions,
                       EntityModelRegistry entityModels) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        FormScopeSupport scopes = new FormScopeSupport(safeRenderer, resolver, defaultDataScope);
        this.planner = new FormOperationPlanner(safeRenderer, scopes, defaultExecutionOptions);
        this.joinPlanner = new JoinQueryPlanner(safeRenderer, scopes, defaultExecutionOptions);
        this.joinResults = new JoinResultProtector(safeRenderer);
        this.containsResults = new ProtectedContainsResultSupport(safeRenderer);
        this.decoder = new FormResultDecoder(safeRenderer, entityModels);
    }

    List<DynamicRow> selectJoin(JoinQuerySpec spec, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        JoinQueryPlanner.PlannedJoin plan = joinPlanner.plan(spec, options);
        return decoder.decodeRows(plan.resultForm(),
                                  executor.query(plan.request(), plan.options()),
                                  plan.options())
                      .stream()
                      .map(row -> joinResults.transform(
                              spec, row, plan.scopes(), spec.sensitiveDisplayMode()))
                      .toList();
    }

    PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                    PageQuery page,
                                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        JoinQueryPlanner.PlannedJoinPage plan = joinPlanner.page(spec, page, options);
        List<DynamicRow> countRows = executor.query(plan.countRequest(), plan.options());
        long total = countRows.isEmpty() ? 0L : CountResultReader.read(countRows.getFirst());
        if (total == 0L) {
            return PageResult.of(List.of(), 0L, plan.page());
        }
        List<DynamicRow> rows = decoder.decodeRows(
                plan.resultForm(), executor.query(plan.dataRequest(), plan.options()), plan.options())
                                       .stream()
                                       .map(row -> joinResults.transform(
                                               spec, row, plan.scopes(), spec.sensitiveDisplayMode()))
                                       .toList();
        return PageResult.of(rows, total, plan.page());
    }

    List<DynamicRow> select(QuerySpec spec) {
        FormOperationPlanner.PlannedQuery plan = planner.select(spec);
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.request(), plan.options());
            requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(),
                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
            return containsResults.finish(plan.form(), plan.containsQuery(), decoded,
                                          plan.outputFields(), plan.displayMode(),
                                          ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
        }
        return decoder.decodeRows(
                plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                plan.scope(), plan.displayMode());
    }

    <T> List<T> select(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "form result type must not be null");
        return select(spec).stream().map(mapper::map).toList();
    }

    PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) {
        FormOperationPlanner.PlannedPage plan = planner.page(spec, page);
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.dataRequest(), plan.options());
            requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(),
                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
            List<DynamicRow> verified = containsResults.finish(
                    plan.form(), plan.containsQuery(), decoded, plan.outputFields(), plan.displayMode(),
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            int from = (int) Math.min(plan.page().offset(), verified.size());
            int to = Math.min(from + plan.page().size(), verified.size());
            return PageResult.of(verified.subList(from, to), verified.size(), plan.page());
        }
        List<DynamicRow> countRows = executor.query(plan.countRequest(), plan.options());
        long total = countRows.isEmpty() ? 0L : CountResultReader.read(countRows.getFirst());
        if (total == 0L) {
            return PageResult.of(List.of(), 0L, plan.page());
        }
        List<DynamicRow> rows = decoder.decodeRows(
                plan.form(), executor.query(plan.dataRequest(), plan.options()), plan.options(),
                plan.scope(), plan.displayMode());
        return PageResult.of(rows, total, plan.page());
    }

    <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "page result type must not be null");
        return FormResultMappingSupport.mapPage(page(spec, page), mapper);
    }

    CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page) {
        FormOperationPlanner.PlannedCursorPage plan = planner.cursorPage(spec, page);
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.request(), plan.options());
            requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(),
                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
            List<DynamicRow> verified = containsResults.finish(
                    plan.form(), plan.containsQuery(), decoded, plan.outputFields(), plan.displayMode(),
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return FormCursorResults.from(verified, plan.page());
        }
        List<DynamicRow> rows = decoder.decodeRows(
                plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                plan.scope(), plan.displayMode());
        return FormCursorResults.from(rows, plan.page());
    }

    <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "cursor page result type must not be null");
        return FormResultMappingSupport.mapCursorPage(cursorPage(spec, page), mapper);
    }

    long insert(WriteSpec spec) {
        return execute(planner.insert(spec));
    }

    /** 生成键和影响行数必须来自同一个 JDBC Statement，不能在 insert 后另查当前序列值。 */
    SqlWriteResult insertReturningKeys(WriteSpec spec) {
        FormOperationPlanner.PlannedWrite plan = planner.insert(spec);
        SqlWriteResult result = plan.protectedWriteRequired()
                ? executor.atomicProtectedWrite(plan.protectedWrite(), plan.options())
                : rowsUpdatedReturningKeys(plan);
        plan.requireSuccess(result.affectedRows());
        return result;
    }

    private SqlWriteResult rowsUpdatedReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        return plan.generatedKeyColumn()
                   .map(column -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options(), column))
                   .orElseGet(() -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options()));
    }

    long update(WriteSpec spec) {
        return execute(planner.update(spec));
    }

    long delete(WriteSpec spec) {
        return execute(planner.delete(spec));
    }

    long physicalDelete(WriteSpec spec) {
        return execute(planner.physicalDelete(spec));
    }

    private long execute(FormOperationPlanner.PlannedWrite plan) {
        long affectedRows = plan.protectedWriteRequired()
                ? executor.atomicProtectedWrite(plan.protectedWrite(), plan.options()).affectedRows()
                : executor.rowsUpdated(plan.request(), plan.options());
        return plan.requireSuccess(affectedRows);
    }

    private static void requireCandidateLimit(int actual) {
        if (actual > ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT) {
            throw new ProtectedSearchCandidateLimitExceededException(
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT, actual);
        }
    }
}
