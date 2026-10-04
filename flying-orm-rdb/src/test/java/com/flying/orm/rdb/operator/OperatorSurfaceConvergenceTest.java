package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定 Operator 内部状态的唯一所有者，避免已经收回的单调用方胶水类型再次出现。 */
class OperatorSurfaceConvergenceTest {

    @Test
    void syncOperatorConfigurationIsPublicWhileAssemblyStaysInternal() throws Exception {
        assertFalse(SyncDatabaseOperator.class.getMethod("withDefaultDataScope", DataScope.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withDefaultBatchWriteOptions", BatchWriteOptions.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withSchemaExecutionOptions", SchemaMigrationExecutionOptions.class)
                .isAnnotationPresent(InternalApi.class));
        assertFalse(SyncDatabaseOperator.class.getMethod("withSchemaObserver", SchemaMigrationObserver.class)
                .isAnnotationPresent(InternalApi.class));
        assertTrue(SyncDatabaseOperator.class.getMethod("create", SyncDatabaseOperator.NativeComponents.class)
                .isAnnotationPresent(InternalApi.class));
        assertTrue(SyncDatabaseOperator.NativeComponents.class.isAnnotationPresent(InternalApi.class));
    }

    @Test
    void dynamicWriteCommandOwnsItsTransientFormClosure() {
        DmlWriteCommand command = DmlWriteCommand.update(
                SqlRenderer.builder().addDefaultTerms().build(), "accounts");
        command.set("name", "Ada");
        command.where(where -> where.is("id", 7L).is("tenant_id", "tenant-a"));
        command.optimisticLock(OptimisticLockOptions.increment("version", 3L));
        command.logicDelete("deleted", 0, 1);

        WriteSpec spec = command.spec();
        DynamicForm form = spec.form();

        assertEquals("accounts", form.table());
        assertEquals(List.of("name", "id", "tenant_id", "version", "deleted"),
                     form.fields().stream().map(field -> field.name()).toList());
        assertEquals(List.of("name"), spec.values().keySet().stream().toList());
        assertEquals("version", spec.lock().orElseThrow().field());
        assertTrue(form.logicDelete().isPresent());
    }

    @Test
    void singleCallerDmlFormBuilderTypeIsAbsent() {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.rdb.operator.DmlFormBuilder"));
    }
}
