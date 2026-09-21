package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormOptimisticConditionRegressionTest {

    @Test
    void changingOnlyAnOrdinaryFieldDoesNotPrepareABatchOwnerQuery() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormDataSqlRenderer renderer = renderer(runtime);
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("external_id", "changed"), businessWhere(),
                    OptimisticLockOptions.increment("version", 7L));

            FormScopeSupport.PreparedBatchUpdate prepared =
                    scopes(renderer).prepareBatchUpdate(form(), update, DataScope.none());

            assertNull(prepared.ownerQuery());
            assertTrue(prepared.request().sql().contains("\"version\" = ?"));
        }
    }

    @Test
    void clearingAContainsValueStillCapturesOwnersAndRejectsPrimaryKeyChanges() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormOperationPlanner planner = planner(renderer(runtime));
            OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 7L);
            FormOperationPlanner.PlannedWrite cleared = planner.update(
                    WriteSpec.update(form(), java.util.Collections.singletonMap("secret", null), businessWhere())
                             .withLock(lock));

            assertNotNull(cleared.protectedWrite());
            assertNotNull(cleared.protectedWrite().ownerQuery());
            assertThrows(IllegalArgumentException.class, () -> planner.update(
                    WriteSpec.update(form(), Map.of("id", 5L), businessWhere()).withLock(lock)));
        }
    }

    @Test
    void publicConflictExceptionCannotModifyThePlannedOptionsSnapshot() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            OptimisticLockOptions lock = OptimisticLockOptions.increment("version", new byte[]{1, 2});
            FormOperationPlanner.PlannedWrite plan = planner(renderer(runtime)).update(
                    WriteSpec.update(form(), Map.of("secret", "updated secret"), businessWhere()).withLock(lock));

            OptimisticLockConflictException conflict = assertThrows(
                    OptimisticLockConflictException.class, () -> plan.requireSuccess(0));
            ((byte[]) conflict.expectedValue())[0] = 9;

            assertArrayEquals(new byte[]{1, 2}, (byte[]) lock.expectedValue());
        }
    }

    @Test
    void protectedUpdateKeepsRootOrGroupedBeforeExpectedVersion() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormDataSqlRenderer renderer = renderer(runtime);
            FormOperationPlanner planner = planner(renderer);

            FormOperationPlanner.PlannedWrite plan = planner.update(
                    WriteSpec.update(form(), Map.of("secret", "updated secret"), businessWhere())
                             .withLock(OptimisticLockOptions.increment("version", 7L)));

            ProtectedWriteWork work = plan.protectedWrite();
            assertNotNull(work);
            assertTrue(plan.request().statement().prepared());
            assertEquals(
                    "update \"users\" set \"secret\" = $1, \"version\" = \"version\" + 1 "
                            + "where (\"id\" = $2 or \"external_id\" = $3) and \"version\" = $4",
                    plan.request().statement().transportSql("postgresql").orElseThrow());
            assertTrue(work.ownerQuery().sql().contains(
                    "where (\"id\" = ? or \"external_id\" = ?) and \"version\" = ?"));
            assertEquals(java.util.List.of(1L, "external-1", 7L), work.ownerQuery().parameters());
        }
    }

    @Test
    void protectedBatchUpdateKeepsRootOrGroupedBeforeExpectedVersion() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormDataSqlRenderer renderer = renderer(runtime);
            FormScopeSupport scopes = scopes(renderer);
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("secret", "updated secret"), businessWhere(),
                    OptimisticLockOptions.increment("version", 7L));

            ConditionGroup ownerWhere = scopes.prepareBatchUpdate(form(), update, DataScope.none())
                                              .ownerQuery()
                                              .where();

            assertEquals(LogicalOperator.AND, ownerWhere.operator());
            ConditionGroup businessGroup = assertInstanceOf(ConditionGroup.class, ownerWhere.children().getFirst());
            assertEquals(LogicalOperator.OR, businessGroup.operator());
            assertEquals(2, businessGroup.children().size());
        }
    }

    @Test
    void protectedBatchUpdateReusesOnePhysicalFormForThePreparedRow() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormDataSqlRenderer renderer = renderer(runtime);
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("secret", "updated secret"), businessWhere(),
                    OptimisticLockOptions.increment("version", 7L));

            FormScopeSupport.PreparedBatchUpdate prepared =
                    scopes(renderer).prepareBatchUpdate(form(), update, DataScope.none());

            assertSame(prepared.form(), prepared.ownerQuery().physicalForm());
        }
    }

    @Test
    void rejectsVersionFieldInRegularValuesWhenOptimisticLockOwnsIt() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            FormOperationPlanner planner = planner(renderer(runtime));
            WriteSpec spec = WriteSpec.update(form(), Map.of("version", 8L), businessWhere())
                                      .withLock(OptimisticLockOptions.increment("VERSION", 7L));

            IllegalArgumentException failure = assertThrows(
                    IllegalArgumentException.class, () -> planner.update(spec));

            assertTrue(failure.getMessage().contains("optimistic lock field"));
        }
    }

    @Test
    void mutatingCodecCannotChangeRepeatedOptimisticWriteParameters() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new MutatingBytesCodec());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("binary_versions", "binary_versions")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .addField(DynamicField.of("version", "OTHER"))
                                      .build();
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();
        OptimisticLockOptions lock = OptimisticLockOptions.assign(
                "version", new byte[]{1}, new byte[]{2});

        SqlRequest firstUpdate = renderer.update(form, Map.of("name", "updated"), where, lock);
        SqlRequest secondUpdate = renderer.update(form, Map.of("name", "updated"), where, lock);
        SqlRequest firstDelete = renderer.delete(form, where, lock);
        SqlRequest secondDelete = renderer.delete(form, where, lock);

        assertEquals(firstUpdate.parameters(), secondUpdate.parameters());
        assertEquals(firstDelete.parameters(), secondDelete.parameters());
    }

    private static final class MutatingBytesCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == byte[].class;
        }

        @Override
        public Object write(Object value) {
            byte[] bytes = (byte[]) value;
            return "encoded:" + ++bytes[0];
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }

    private static FormOperationPlanner planner(FormDataSqlRenderer renderer) {
        return new FormOperationPlanner(renderer, scopes(renderer), SqlExecutionOptions.safeDefaults());
    }

    private static FormScopeSupport scopes(FormDataSqlRenderer renderer) {
        return new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none());
    }

    private static FormDataSqlRenderer renderer(ProtectedFieldRuntime runtime) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                                  .withProtectedFields(runtime);
    }

    private static ProtectedFieldRuntime runtime() {
        return ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("users", "users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("external_id", "VARCHAR"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .addField(DynamicField.of("version", "BIGINT"))
                          .encrypted("secret", EncryptedFieldDefinition.builder()
                                                                        .searchModes(EncryptedSearchMode.CONTAINS)
                                                                        .build())
                          .build();
    }

    private static ConditionGroup businessWhere() {
        return ConditionGroup.or()
                             .where("id", "=", 1L)
                             .where("external_id", "=", "external-1")
                             .build();
    }
}
