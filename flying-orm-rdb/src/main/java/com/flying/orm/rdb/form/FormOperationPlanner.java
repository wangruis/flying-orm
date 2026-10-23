package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteOperation;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把表单查询和单条写入规格编译成与驱动无关的安全 SQL 计划。
 *
 * <p>这里统一完成结构化条件解析、Scope 合并、字段保护、逻辑删除和乐观锁 SQL 选择。计划里只有
 * {@link SqlRequest} 和执行保护，不含 JDBC、R2DBC、Publisher 或 Connection，因此两条执行链消费的是同一结果，
 * 不会因为分别实现 CRUD 而产生参数顺序或安全规则偏差。</p>
 */
final class FormOperationPlanner {

    final FormReadPlanner reads;
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
        this.reads = new FormReadPlanner(this.renderer, this.scopes, this.defaultExecutionOptions);
    }

    enum WriteAction { INSERT, UPDATE, DELETE, PHYSICAL_DELETE }

    PlannedWrite write(WriteSpec spec, WriteAction action, boolean governed,
                       FieldUsePolicy policy, QueryShapeLimits limits) {
        PlannedWrite plan = switch (action) {
            case INSERT -> insert(spec);
            case UPDATE -> update(spec);
            case DELETE -> delete(spec);
            case PHYSICAL_DELETE -> physicalDelete(spec);
        };
        return governed
                ? FieldUseGuard.write(plan, renderer, spec, plan.scope(), plan.request(), policy, limits).plan()
                : plan;
    }

    PlannedWrite insert(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.INSERT, "insert");
        if (safeSpec.lock().isPresent()) {
            throw new IllegalArgumentException("insert spec must not contain optimistic lock options");
        }
        DynamicForm form = safeSpec.form();
        DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        Map<String, Object> values = scopes.prepareWriteValues(form,
                RepositoryUpsertValues.prepareInsert(form, safeSpec.ownedValues(), true, Set.of()), effectiveScope);
        if (form.protections().encryptedFields().isEmpty()) {
            return new PlannedWrite(form, renderer.insert(form, values), executionOptions(safeSpec),
                    null, null, effectiveScope);
        }
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, effectiveScope);
        FormPreparedWrite write = protection.prepare(values);
        BatchInsertPlan insert = renderer.batchRenderer.insertPlan(write.physicalForm(), write.values());
        SqlRequest request = insert.singleRequest();
        ProtectedWriteWork protectedWrite = protection.protectedWrite(
                write, request, null, ProtectedWriteWork.Kind.INSERT,
                protection.insertOwner(insert, insert.firstParameters())).orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec), null, protectedWrite, effectiveScope);
    }

    PlannedWrite update(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.UPDATE, "update");
        DynamicForm form = safeSpec.form();
        Map<String, Object> values = safeSpec.ownedValues();
        DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        ConditionGroup where = scopes.applyUpdateScope(form, values, safeSpec.where(), effectiveScope);
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        scopes.validateTenantLock(form, lock, effectiveScope);
        if (form.protections().encryptedFields().isEmpty()) {
            ConditionGroup preparedWhere = FormDataScopes.unwrapTrustedValues(form, where, effectiveScope);
            SqlRequest request = lock == null
                    ? renderer.update(form, values, preparedWhere)
                    : renderer.update(form, values, preparedWhere, lock);
            return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null, effectiveScope);
        }
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, effectiveScope);
        FormPreparedWrite write = protection.prepare(values);
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, physicalForm, where, effectiveScope, List.of());
        SqlRequest request = lock == null
                ? renderer.update(write.physicalForm(), write.values(), query.where())
                : renderer.update(write.physicalForm(), write.values(), query.where(), lock);
        ProtectedFieldRuntime.PreparedQuery ownerQuery = null;
        if (!write.containsTokens().isEmpty()) {
            // owner 查询复用已准备的保护条件，不能再次生成检索令牌。
            ownerQuery = lock == null ? query : new ProtectedFieldRuntime.PreparedQuery(
                    query.physicalForm(), FormScopeSupport.withExpectedVersion(query.where(), lock), query.visibleFields());
        }
        ProtectedWriteWork protectedWrite = protection.protectedWrite(
                write, request, ownerQuery, ProtectedWriteWork.Kind.UPDATE, Map.of()).orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec),
                                lock, protectedWrite, effectiveScope);
    }

    PlannedWrite delete(WriteSpec spec) {
        return delete(spec, false);
    }

    PlannedWrite physicalDelete(WriteSpec spec) {
        return delete(spec, true);
    }

    /** 删除共用 Scope 与版本语义；逻辑删除只替换赋值和未删除条件。 */
    private PlannedWrite delete(WriteSpec spec, boolean physical) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.DELETE, "delete");
        DynamicForm form = safeSpec.form();
        DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        ConditionGroup where = scopes.applyWriteScope(form, safeSpec.where(), effectiveScope);
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        Map<String, Object> values = physical ? null : FormLogicDeletes.deleteValues(form).orElse(null);
        if (values != null) {
            scopes.validateTenantLock(form, lock, effectiveScope);
            where = FormLogicDeletes.activeWhere(form, where);
        }
        DynamicForm writeForm = form;
        if (form.protections().encryptedFields().isEmpty()) {
            where = FormDataScopes.unwrapTrustedValues(form, where, effectiveScope);
        } else {
            // 仅已声明加密的表单转换物理模型；普通删除不创建保护规划对象。
            ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                    form, where, effectiveScope, List.of());
            writeForm = query.physicalForm();
            where = query.where();
            if (values != null) {
                values = renderer.protection().prepareWrite(form, writeForm, values, effectiveScope).values();
            }
        }
        SqlRequest request = values == null
                ? lock == null ? renderer.delete(writeForm, where) : renderer.delete(writeForm, where, lock)
                : lock == null ? renderer.update(writeForm, values, where) : renderer.update(writeForm, values, where, lock);
        return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null, effectiveScope);
    }

    private SqlExecutionOptions executionOptions(WriteSpec spec) {
        return spec.executionOptions().orElse(defaultExecutionOptions);
    }

    private static WriteSpec requireOperation(WriteSpec spec, WriteOperation expected, String action) {
        if (spec == null) {
            throw new NullPointerException(action + " spec must not be null");
        }
        if (spec.operation() != expected) {
            throw new IllegalArgumentException(
                    "write spec operation " + spec.operation() + " cannot execute as " + expected);
        }
        return spec;
    }

    record PlannedWrite(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        OptimisticLockOptions lock,
                        ProtectedWriteWork protectedWrite,
                        DataScope scope) {

        boolean protectedWriteRequired() {
            return protectedWrite != null;
        }

        java.util.Optional<String> generatedKeyColumn() {
            String column = null;
            for (DynamicField field : form.fields()) {
                if (field.primaryKey() && field.generation().generated()) {
                    if (column != null) {
                        return java.util.Optional.empty();
                    }
                    column = field.name();
                }
            }
            return java.util.Optional.ofNullable(column);
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
