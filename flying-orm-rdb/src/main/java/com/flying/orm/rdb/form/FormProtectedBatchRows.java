package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.form.spec.BatchOperation;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.batch.BatchPublishers.IndexedMapper;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * 共享动态表单批量行的纯规划及可选侧索引装配。
 *
 * <p>真实参数始终保留随机密文；侧索引工作由原生执行器在绑定前剥离，
 * 不会作为参数进入业务 SQL、结果或日志。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class FormProtectedBatchRows {

    private FormProtectedBatchRows() {
    }

    /** 首行已经审批；执行入口提供行映射管线，保留各自的订阅、调度和错误传播。 */
    static BatchWriteRequest updateRequest(
            FormDataSqlRenderer renderer, FormScopeSupport scopes,
            DynamicForm form, BatchUpdate sourceFirst, DataScope scope, BatchWriteOptions options,
            FieldUsePolicy fieldUsePolicy, ProtectedContainsLayout protectionLayout, boolean supportsLayouts,
            Function<IndexedMapper<BatchUpdate, Object[]>, Publisher<Object[]>> rows) {
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = form.protections().encryptedFields().isEmpty()
                ? null : renderer.protection().writeOperation(
                form, physicalForm, scope, protectionLayout);
        FormScopeSupport.PreparedBatchScope batchScope = scopes.prepareBatchScope(form, physicalForm, scope);
        FormScopeSupport.PreparedBatchUpdate first = scopes.prepareBatchUpdate(
                form, physicalForm, sourceFirst, batchScope, protection);
        BatchUpdatePlan plan = renderer.batchUpdatePlan(
                first.form(), first.values(), first.lock(), first.request());
        BatchUpdatePlan.Cursor cursor = new BatchUpdatePlan.Cursor(plan, supportsLayouts);
        Publisher<Object[]> parameters = rows.apply((update, index) -> {
            if (index != 0L) FieldUseGuard.approveBatchUpdate(renderer, form, update, scope, fieldUsePolicy);
            FormScopeSupport.PreparedBatchUpdate prepared = index == 0L ? first : scopes.prepareBatchUpdate(
                    form, physicalForm, update, batchScope, protection);
            return cursor.attach(update(
                    protection, prepared, cursor.parameters(renderer, prepared, index), protectionLayout));
        });
        return plan.request(parameters, options);
    }

    /** 插入和 upsert 共用一套相邻布局、生成键及保护值规划，不缓存历史行。 */
    static BatchWriteRequest insertRequest(
            FormDataSqlRenderer renderer, FormScopeSupport scopes, BatchSpec spec,
            Map<String, Object> sourceFirstRow, Map<String, Object> firstValues, DataScope scope,
            BatchWriteOptions options, FieldUsePolicy fieldUsePolicy, ProtectedContainsLayout protectionLayout,
            boolean supportsLayouts,
            Function<IndexedMapper<Map<String, Object>, Object[]>, Publisher<Object[]>> rows) {
        DynamicForm form = spec.form();
        boolean upsert = spec.operation() == BatchOperation.UPSERT && !spec.ignoresConflicts();
        if (upsert) FieldUseGuard.approveUpsertCondition(renderer, form, spec.updateCondition(), scope, fieldUsePolicy);
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = form.protections().encryptedFields().isEmpty()
                ? null : renderer.protection().writeOperation(
                form, physicalForm, scope, protectionLayout);
        FormPreparedWrite first = protection == null ? null : protection.prepare(firstValues);
        BatchInsertPlan plan = scopes.prepareBatchInsert(spec, firstValues, first, sourceFirstRow, scope);
        BatchInsertPlan.Cursor cursor = new BatchInsertPlan.Cursor(sourceFirstRow, plan, supportsLayouts);
        Publisher<Object[]> parameters = rows.apply((source, index) -> {
            boolean conflictUpdate = upsert && !BatchInsertPlan.generatedInsert(spec, source);
            boolean changed = index != 0L && !cursor.matches(source);
            if (changed) FieldUseGuard.approveBatchInsert(form, source, scope, conflictUpdate,
                    fieldUsePolicy, spec.conflictFields());
            Map<String, Object> logical = index == 0L ? firstValues : scopes.prepareWriteValues(form, source, scope);
            FormPreparedWrite write = index == 0L ? first : protection == null ? null : protection.prepare(logical);
            if (changed) cursor.select(source, scopes.prepareBatchInsert(spec, logical, write, source, scope));
            Object[] values = index == 0L || changed ? cursor.plan().firstParameters()
                    : cursor.plan().parameters(write == null ? logical : write.values(), index);
            return cursor.attach(insert(protection, write, cursor.plan(), values, conflictUpdate, protectionLayout),
                    !spec.generatedKeys().required() || !conflictUpdate);
        });
        return plan.request(parameters, options, spec.generatedKeys());
    }

    /** 组装 insert/upsert 行。 */
    static Object[] insert(FormProtectionSqlSupport.WriteOperation protection,
                           FormPreparedWrite write,
                           BatchInsertPlan plan,
                           Object[] parameters,
                           boolean upsert,
                           ProtectedContainsLayout layout) {
        ProtectedWriteWork work = null;
        if (layout != null) {
            com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                    plan.statement(), Arrays.asList(parameters));
            work = protection.protectedWrite(
                    write, request, null,
                    upsert ? ProtectedWriteWork.Kind.UPSERT : ProtectedWriteWork.Kind.INSERT,
                    protection.insertOwner(plan, parameters)).orElse(null);
        }
        return extend(parameters, work);
    }

    /** 为普通及乐观锁更新参数附加受保护字段的侧索引工作。 */
    static Object[] update(FormProtectionSqlSupport.WriteOperation protection,
                           FormScopeSupport.PreparedBatchUpdate prepared,
                           Object[] parameters,
                           ProtectedContainsLayout layout) {
        ProtectedWriteWork work = null;
        if (layout != null) {
            work = protection.protectedWrite(
                    prepared.write(), prepared.request(), prepared.ownerQuery(),
                    ProtectedWriteWork.Kind.UPDATE, Map.of()).orElse(null);
        }
        return extend(parameters, work);
    }

    private static Object[] extend(Object[] parameters, ProtectedWriteWork work) {
        return work == null ? parameters : ProtectedBatchRows.extend(parameters, work);
    }

}
