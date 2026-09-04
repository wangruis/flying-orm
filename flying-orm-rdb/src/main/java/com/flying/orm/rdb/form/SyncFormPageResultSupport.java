package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;

/**
 * JDBC 分页计划的执行与结果整理，不参与 SQL 规划或事务管理。
 *
 * @author wangr
 * @version v3.2
 */
final class SyncFormPageResultSupport {

    private SyncFormPageResultSupport() {
    }

    static List<DynamicRow> transformJoinRows(JoinResultProtector.ResultPlan resultPlan,
                                              List<DynamicRow> rows) {
        return resultPlan.direct() ? rows : rows.stream().map(resultPlan::transform).toList();
    }

    static PageResult<DynamicRow> page(SyncSqlExecutor executor,
                                       FormResultDecoder decoder,
                                       ProtectedContainsResultSupport containsResults,
                                       FormOperationPlanner.PlannedPage plan,
                                       SensitiveDisplayMode displayMode) {
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.dataRequest(), plan.options());
            ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(), SensitiveDisplayMode.FULL);
            List<DynamicRow> verified = containsResults.finish(
                    plan.form(), plan.containsQuery(), decoded, plan.outputFields(), displayMode);
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
                plan.scope(), displayMode);
        return PageResult.of(rows, total, plan.page());
    }

    static CursorPageResult<DynamicRow> cursorPage(
            SyncSqlExecutor executor,
            FormResultDecoder decoder,
            ProtectedContainsResultSupport containsResults,
            FormOperationPlanner.PlannedCursorPage plan,
            SensitiveDisplayMode displayMode) {
        if (plan.contains()) {
            List<DynamicRow> rawRows = executor.query(plan.request(), plan.options());
            ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
            List<DynamicRow> decoded = decoder.decodeRows(
                    plan.form(), rawRows, plan.options(), plan.scope(), SensitiveDisplayMode.FULL);
            List<DynamicRow> verified = containsResults.finish(
                    plan.form(), plan.containsQuery(), decoded, plan.outputFields(), displayMode);
            return FormCursorResults.from(verified, plan.page());
        }
        List<DynamicRow> rows = decoder.decodeRows(
                plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                plan.scope(), displayMode);
        return FormCursorResults.from(rows, plan.page());
    }

    static KeysetPageResult<DynamicRow> keysetPage(
            SyncSqlExecutor executor,
            FormResultDecoder decoder,
            FormDataSqlRenderer renderer,
            FormOperationPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse) {
        SensitiveDisplayMode displayMode = plan.displayMode();
        List<DynamicRow> physicalRows = executor.query(plan.request(), plan.options());
        List<DynamicRow> rowsForDecoding = plan.layout().hasHiddenSelections()
                ? physicalRows.stream().map(plan.layout()::logicalRowForDecoding).toList()
                : physicalRows;
        List<DynamicRow> decoded = decoder.decodeRows(
                plan.form(), rowsForDecoding, plan.options(), plan.scope(), displayMode);
        List<DynamicRow> decodedPhysicalRows = plan.layout().hasHiddenSelections()
                ? decoded.stream().map(plan.layout()::physicalRowAfterDecoding).toList()
                : decoded;
        return plan.layout().finish(
                decodedPhysicalRows,
                plan.page().size(),
                fieldUse == null
                        ? java.util.function.UnaryOperator.identity()
                        : row -> FieldUseGuard.applyVisibility(renderer, plan.form(), row, fieldUse));
    }
}
