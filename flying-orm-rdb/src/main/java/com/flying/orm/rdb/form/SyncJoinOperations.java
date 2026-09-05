package com.flying.orm.rdb.form;

import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;

/** 同步 JOIN 的规划、解码与治理结果发布协作者。 */
final class SyncJoinOperations {

    private final SyncSqlExecutor executor;
    private final FormResultDecoder decoder;
    private final JoinQueryPlanner planner;
    private final FormDataSqlRenderer renderer;
    private final FieldUsePolicy fieldUsePolicy;
    private final com.flying.orm.core.condition.QueryShapeLimits queryShapeLimits;
    private final boolean governed;

    SyncJoinOperations(SyncSqlExecutor executor,
                       FormDataSqlRenderer renderer,
                       FormScopeSupport scopes,
                       SqlExecutionOptions defaultExecutionOptions,
                       FormResultDecoder decoder,
                       FieldUsePolicy fieldUsePolicy,
                       com.flying.orm.core.condition.QueryShapeLimits queryShapeLimits,
                       boolean governed) {
        this.executor = Objects.requireNonNull(executor, "sync SQL executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form data SQL renderer must not be null");
        this.decoder = Objects.requireNonNull(decoder, "form result decoder must not be null");
        this.fieldUsePolicy = Objects.requireNonNull(
                fieldUsePolicy, "field use policy must not be null");
        this.queryShapeLimits = Objects.requireNonNull(
                queryShapeLimits, "query shape limits must not be null");
        this.governed = governed;
        this.planner = new JoinQueryPlanner(renderer, scopes, defaultExecutionOptions);
    }

    List<DynamicRow> select(JoinQuerySpec spec, SqlExecutionOptions options) {
        GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoin> envelope = governed
                ? planner.planGoverned(spec, options, fieldUsePolicy, queryShapeLimits)
                : null;
        JoinQueryPlanner.PlannedJoin plan = governed ? envelope.plan() : planner.plan(spec, options);
        return executor.queryMapped(
                plan.request(), plan.options(), publishedRowMapper(plan, envelope), 0);
    }

    FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) {
        return governed
                ? planner.planGoverned(spec, null, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    <T> List<T> selectMapped(JoinQuerySpec spec,
                             SqlExecutionOptions options,
                             RowMapper<T> mapper) {
        GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoin> envelope = governed
                ? planner.planGoverned(spec, options, fieldUsePolicy, queryShapeLimits)
                : null;
        JoinQueryPlanner.PlannedJoin plan = governed ? envelope.plan() : planner.plan(spec, options);
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        RowMapper<DynamicRow> rowMapper = publishedRowMapper(plan, envelope);
        RowMapper<T> mappedRow = row -> safeMapper.map(rowMapper.map(row));
        return executor.queryMapped(plan.request(), plan.options(), mappedRow, 0);
    }

    PageResult<DynamicRow> page(JoinQuerySpec spec,
                                PageQuery page,
                                SqlExecutionOptions options) {
        GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoinPage> envelope = governed
                ? planner.pageGoverned(spec, page, options, fieldUsePolicy, queryShapeLimits)
                : null;
        JoinQueryPlanner.PlannedJoinPage plan = governed
                ? envelope.plan() : planner.page(spec, page, options);
        List<DynamicRow> countRows = executor.query(plan.countRequest(), plan.options());
        long total = countRows.isEmpty() ? 0L : CountResultReader.read(countRows.getFirst());
        if (total == 0L) {
            return PageResult.of(List.of(), 0L, plan.page());
        }
        List<DynamicRow> rows = executor.queryMapped(
                plan.dataRequest(), plan.options(),
                publishedRowMapper(plan, envelope),
                0);
        return PageResult.of(rows, total, plan.page());
    }

    private RowMapper<DynamicRow> publishedRowMapper(
            JoinQueryPlanner.PlannedJoin plan,
            GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoin> envelope) {
        return publishedRowMapper(
                plan.spec(), plan.resultForm(), plan.options(), plan.resultPlan(), plan.decodingPlan(),
                governed ? envelope.fieldUse() : null);
    }

    private RowMapper<DynamicRow> publishedRowMapper(
            JoinQueryPlanner.PlannedJoinPage plan,
            GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoinPage> envelope) {
        return publishedRowMapper(
                plan.spec(), plan.resultForm(), plan.options(), plan.resultPlan(), plan.decodingPlan(),
                governed ? envelope.fieldUse() : null);
    }

    private RowMapper<DynamicRow> publishedRowMapper(
            JoinQuerySpec spec,
            com.flying.orm.core.form.DynamicForm resultForm,
            SqlExecutionOptions options,
            JoinResultProtector.ResultPlan resultPlan,
            FormFieldDecodingPlan decodingPlan,
            FieldUseSnapshot fieldUse) {
        RowMapper<DynamicRow> rowDecoder = decoder.rowDecoder(
                resultForm, options, com.flying.orm.core.scope.DataScope.none(),
                com.flying.orm.core.protection.SensitiveDisplayMode.FULL, decodingPlan);
        return row -> {
            DynamicRow decoded = rowDecoder.map(row);
            DynamicRow transformed = resultPlan.direct()
                    ? decoded : resultPlan.transform(decoded);
            return governed
                    ? FieldUseGuard.applyJoinVisibility(
                            renderer, spec, transformed, fieldUse)
                    : transformed;
        };
    }
}
