package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteOperation;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 把表单查询和单条写入规格编译成与驱动无关的安全 SQL 计划。
 *
 * <p>这里统一完成结构化条件解析、Scope 合并、字段保护、逻辑删除和乐观锁 SQL 选择。计划里只有
 * {@link SqlRequest} 和执行保护，不含 JDBC、R2DBC、Publisher 或 Connection，因此两条执行链消费的是同一结果，
 * 不会因为分别实现 CRUD 而产生参数顺序或安全规则偏差。</p>
 */
final class FormOperationPlanner {

    private final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;
    private final SqlExecutionOptions defaultExecutionOptions;
    FormOperationPlanner(FormDataSqlRenderer renderer,
                         FormScopeSupport scopes,
                         SqlExecutionOptions defaultExecutionOptions) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = Objects.requireNonNull(scopes, "form scope support must not be null");
        this.defaultExecutionOptions = Objects.requireNonNull(
                defaultExecutionOptions, "default sql execution options must not be null");
    }

    PlannedQuery select(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        ScopedRead read = scopedRead(safeSpec);
        List<String> projections = FormQueryShapeGuard.readableProjections(safeSpec, read.form());
        List<String> groups = FormQueryShapeGuard.readableGroups(safeSpec, read.form());
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(
                safeSpec.form(), read.form(), safeSpec.sorts());
        FormQueryShapeGuard.requireValidGrouping(projections, groups, sorts);
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = renderer.protection()
                .prepareContainsQuery(safeSpec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(safeSpec);
            List<String> outputFields = FormQueryShapeGuard.outputFields(projections, read.form());
            SqlRequest request = renderer.protection().containsRows(
                    contains.orElseThrow(), sorts,
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new PlannedQuery(safeSpec.form(), request, executionOptions(safeSpec),
                                    read.scope(), safeSpec.sensitiveDisplayMode(),
                                    contains.orElseThrow(), outputFields);
        }
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                safeSpec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        SqlRequest request = renderer.protection().select(query, groups, sorts);
        return new PlannedQuery(safeSpec.form(), request, executionOptions(safeSpec),
                                read.scope(), safeSpec.sensitiveDisplayMode(), null, List.of());
    }

    PlannedPage page(QuerySpec spec, PageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "offset pagination");
        PageQuery requested = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopedRead(safeSpec);
        List<String> projections = FormQueryShapeGuard.readableProjections(safeSpec, read.form());
        List<PageSort> requestedSorts = safeSpec.sorts().isEmpty() ? requested.sorts() : safeSpec.sorts();
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(
                safeSpec.form(), read.form(), requestedSorts);
        PageQuery effectivePage = new PageQuery(requested.page(), requested.size(), sorts);
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = renderer.protection()
                .prepareContainsQuery(safeSpec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(safeSpec);
            SqlRequest request = renderer.protection().containsRows(
                    contains.orElseThrow(), effectivePage.sorts(),
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new PlannedPage(safeSpec.form(), null, request, effectivePage,
                                   executionOptions(safeSpec), read.scope(),
                                   safeSpec.sensitiveDisplayMode(), contains.orElseThrow(),
                                   FormQueryShapeGuard.outputFields(projections, read.form()));
        }
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                safeSpec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        return new PlannedPage(safeSpec.form(), renderer.protection().count(query),
                               renderer.protection().select(query, effectivePage), effectivePage,
                               executionOptions(safeSpec), read.scope(), safeSpec.sensitiveDisplayMode(),
                               null, List.of());
    }

    PlannedCursorPage cursorPage(QuerySpec spec, CursorPageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "cursor pagination");
        if (!safeSpec.sorts().isEmpty()) {
            throw new IllegalArgumentException(
                    "cursor pagination sorts must be declared with CursorPageQuery");
        }
        ScopedRead read = scopedRead(safeSpec);
        CursorPageQuery normalized = CursorPageNormalizer.normalize(
                read.form(), Objects.requireNonNull(page, "cursor page query must not be null"));
        FormQueryShapeGuard.requireReadableUnencryptedCursorSorts(
                safeSpec.form(), read.form(), normalized.sorts(), safeSpec.sensitiveDisplayMode());
        List<String> projections = FormQueryShapeGuard.readableProjections(safeSpec, read.form());
        FormQueryShapeGuard.requireCursorProjection(projections, normalized.sorts());
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = renderer.protection()
                .prepareContainsQuery(safeSpec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(safeSpec);
            SqlRequest request = renderer.protection().containsRows(
                    contains.orElseThrow(), normalized,
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new PlannedCursorPage(safeSpec.form(), request, normalized,
                                         executionOptions(safeSpec), read.scope(),
                                         safeSpec.sensitiveDisplayMode(), contains.orElseThrow(),
                                         FormQueryShapeGuard.outputFields(projections, read.form()));
        }
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                safeSpec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        return new PlannedCursorPage(safeSpec.form(), renderer.protection().select(query, normalized),
                                     normalized, executionOptions(safeSpec), read.scope(),
                                     safeSpec.sensitiveDisplayMode(), null, List.of());
    }

    PlannedWrite insert(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.INSERT, "insert");
        if (safeSpec.lock().isPresent()) {
            throw new IllegalArgumentException("insert spec must not contain optimistic lock options");
        }
        DynamicForm form = safeSpec.form();
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        Map<String, Object> values = scopes.prepareWriteValues(form, safeSpec.values(), effectiveScope);
        ProtectedFieldRuntime.PreparedWrite write = renderer.protection().prepareWrite(
                form, values, effectiveScope);
        SqlRequest request = renderer.protection().insert(write);
        ProtectedWriteWork protectedWrite = renderer.protection().protectedWrite(
                form, values, effectiveScope, request, null, ProtectedWriteWork.Kind.INSERT).orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec), null, protectedWrite);
    }

    PlannedWrite update(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.UPDATE, "update");
        DynamicForm form = safeSpec.form();
        Map<String, Object> values = safeSpec.values();
        ConditionGroup where = scopes.writableActiveWhere(
                form, values, safeSpec.where(), safeSpec.scope());
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        ProtectedFieldRuntime.PreparedWrite write = renderer.protection().prepareWrite(
                form, values, effectiveScope);
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, form, where, effectiveScope);
        SqlRequest request = safeSpec.lock()
                                     .map(lock -> renderer.protection().update(write, query.where(), lock))
                                     .orElseGet(() -> renderer.protection().update(write, query.where()));
        ProtectedFieldRuntime.PreparedQuery ownerQuery = safeSpec.lock()
                .map(lock -> renderer.protection().prepareQuery(
                        form, form, withExpectedVersion(where, lock), effectiveScope))
                .orElse(query);
        ProtectedWriteWork protectedWrite = renderer.protection().protectedWrite(
                form, values, effectiveScope, request, ownerQuery, ProtectedWriteWork.Kind.UPDATE)
                                                       .orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec),
                                safeSpec.lock().orElse(null), protectedWrite);
    }

    PlannedWrite delete(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.DELETE, "delete");
        DynamicForm form = safeSpec.form();
        ConditionGroup scopedWhere = scopes.scopedWhere(form, safeSpec.where(), safeSpec.scope());
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(form, scopedWhere);
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        SqlRequest request = FormLogicDeletes.deleteValues(form)
                .map(values -> {
                    ProtectedFieldRuntime.PreparedWrite write = renderer.protection().prepareWrite(
                            form, values, effectiveScope);
                    ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                            form, form, activeWhere, effectiveScope);
                    return lock == null
                            ? renderer.protection().update(write, query.where())
                            : renderer.protection().update(write, query.where(), lock);
                })
                .orElseGet(() -> {
                    ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                            form, form, scopedWhere, effectiveScope);
                    return renderer.protection().delete(query, lock);
                });
        return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null);
    }

    PlannedWrite physicalDelete(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.DELETE, "delete");
        DynamicForm form = safeSpec.form();
        ConditionGroup where = scopes.scopedWhere(form, safeSpec.where(), safeSpec.scope());
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, form, where, effectiveScope);
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        SqlRequest request = renderer.protection().delete(query, lock);
        return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null);
    }

    private ScopedRead scopedRead(QuerySpec spec) {
        return spec.structuredInput()
                   .map(input -> scopes.scopedStructuredRead(
                           spec.form(), input,
                           spec.structuredPolicy().orElse(StructuredConditionPolicy.defaults()), spec.scope()))
                   .orElseGet(() -> scopes.scopedRead(spec.form(), spec.where(), spec.scope()));
    }

    private static ProtectedFieldRuntime.PreparedQuery withProjection(ProtectedFieldRuntime.PreparedQuery query,
                                                                      List<String> projections) {
        return projections.isEmpty()
                ? query
                : new ProtectedFieldRuntime.PreparedQuery(query.physicalForm(), query.where(), projections);
    }

    private static ConditionGroup withExpectedVersion(ConditionGroup where, OptimisticLockOptions lock) {
        ConditionGroup.Builder builder = ConditionGroup.and();
        where.children().forEach(builder::add);
        return builder.where(lock.field(), "=", lock.expectedValue()).build();
    }

    private SqlExecutionOptions executionOptions(QuerySpec spec) {
        return spec.executionOptions().orElse(defaultExecutionOptions);
    }

    private SqlExecutionOptions executionOptions(WriteSpec spec) {
        return spec.executionOptions().orElse(defaultExecutionOptions);
    }

    private static WriteSpec requireOperation(WriteSpec spec, WriteOperation expected, String action) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, action + " spec must not be null");
        if (safeSpec.operation() != expected) {
            throw new IllegalArgumentException(
                    "write spec operation " + safeSpec.operation() + " cannot execute as " + expected);
        }
        return safeSpec;
    }

    record PlannedQuery(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        com.flying.orm.core.scope.DataScope scope,
                        com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                        ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                        List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }
    }

    record PlannedPage(DynamicForm form,
                       SqlRequest countRequest,
                       SqlRequest dataRequest,
                       PageQuery page,
                       SqlExecutionOptions options,
                       com.flying.orm.core.scope.DataScope scope,
                       com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                       ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                       List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }
    }

    record PlannedCursorPage(DynamicForm form,
                             SqlRequest request,
                             CursorPageQuery page,
                             SqlExecutionOptions options,
                             com.flying.orm.core.scope.DataScope scope,
                             com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                             ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                             List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }
    }

    record PlannedWrite(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        OptimisticLockOptions lock,
                        ProtectedWriteWork protectedWrite) {

        boolean protectedWriteRequired() {
            return protectedWrite != null;
        }

        java.util.Optional<String> generatedKeyColumn() {
            List<String> columns = form.fields().stream()
                                       .filter(field -> field.primaryKey() && field.generation().generated())
                                       .map(DynamicField::name)
                                       .toList();
            return columns.size() == 1 ? java.util.Optional.of(columns.getFirst()) : java.util.Optional.empty();
        }

        long requireSuccess(long affectedRows) {
            if (lock != null && affectedRows == 0L) {
                throw new com.flying.orm.rdb.lock.OptimisticLockConflictException(
                        form.table(), lock.field(), lock.expectedValue());
            }
            return affectedRows;
        }
    }
}
