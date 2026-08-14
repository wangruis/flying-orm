package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Scope 是动态表单最重要的安全边界。直接测试守卫可以保证客户端今后继续瘦身时，
 * 自动租户填充和越租户拒绝不会依赖某个入口恰好调用了正确的私有方法。
 */
class FormScopeGuardTest {

    @Test
    void autoTenantUsesTheEffectiveScopeAndRejectsConflictingInput() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(),
                RdbDialect.mysql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.tenant("tenant_id", "t1"));
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();

        assertEquals(Map.of("id", "u1", "tenant_id", "t1"),
                     guard.prepareWriteValues(form, Map.of("id", "u1"), guard.effectiveScope(DataScope.none())));

        ScopeAccessException error = assertThrows(
                ScopeAccessException.class,
                () -> guard.prepareWriteValues(form,
                                               Map.of("id", "u1", "tenant_id", "t2"),
                                               guard.effectiveScope(DataScope.none())));
        assertEquals(ScopeErrorCode.TENANT_VALUE_MISMATCH, error.code());
    }

    /** 批量更新也必须提供调用方业务谓词，租户 Scope 不能替空 where 决定被更新的业务行。 */
    @Test
    void rejectsBatchUpdateWithoutBusinessWhereBeforeApplyingTenantScope() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(),
                RdbDialect.mysql());
        FormScopeGuard guard = new FormScopeGuard(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.tenant("tenant_id", "t1"));
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                      .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .addField(DynamicField.of("version", "BIGINT"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("name", "updated"),
                ConditionGroup.and().build(),
                OptimisticLockOptions.increment("version", 1L));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> guard.batchUpdateWhere(form, update, guard.effectiveScope(DataScope.none())));

        assertEquals("write business where condition must not be empty", error.getMessage());
    }
}
