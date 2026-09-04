package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlainFormProtectionPlanningTest {

    @Test
    void absentProtectionAndScopeDoNotSnapshotQueryValuesAgain() {
        AtomicInteger copies = new AtomicInteger();
        DynamicForm form = form();
        ConditionGroup where = ConditionGroup.and()
                .where("created_at", "=", new CountingDate(1L, copies)).build();
        FormProtectionSqlSupport protection = renderer().protection();
        copies.set(0);

        assertSame(where, protection.prepareQuery(form, form, where, DataScope.none()).where());
        assertEquals(0, copies.get());
    }

    @Test
    void absentContainsAndScopeDoNotSnapshotQueryValuesAgain() {
        AtomicInteger copies = new AtomicInteger();
        DynamicForm form = form();
        ConditionGroup where = ConditionGroup.and()
                .where("created_at", "=", new CountingDate(1L, copies)).build();
        FormProtectionSqlSupport protection = renderer().protection();
        copies.set(0);

        assertTrue(protection.prepareContainsQuery(form, form, where, DataScope.none()).isEmpty());
        assertEquals(0, copies.get());
    }

    @Test
    void ordinaryOptimisticBatchHasNoOwnerQueryButKeepsScopeAndVersionPredicates() {
        FormDataSqlRenderer renderer = renderer();
        FormScopeSupport scopes = new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none());
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("name", "changed"),
                ConditionGroup.or().where("id", "=", 1L).where("id", "=", 2L).build(),
                OptimisticLockOptions.increment("version", 7L));

        FormScopeSupport.PreparedBatchUpdate prepared =
                scopes.prepareBatchUpdate(form(), update, DataScope.where(
                        ConditionGroup.and().where("tenant_id", "=", 9L).build()));

        assertEquals(List.of("changed", 1L, 2L, 9L, 7L), prepared.request().parameters());
        assertTrue(prepared.request().sql().contains(
                "where (\"id\" = ? or \"id\" = ?) and \"tenant_id\" = ? and \"version\" = ?"));
        assertNull(prepared.ownerQuery(), "ordinary updates do not consume a CONTAINS owner query");
    }

    @Test
    void ordinaryLockedUpdateRetainsConflictAndVersionOwnershipChecks() {
        FormDataSqlRenderer renderer = renderer();
        FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
        ConditionGroup where = ConditionGroup.or().where("id", "=", 1L).where("id", "=", 2L).build();
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 7L);

        FormOperationPlanner.PlannedWrite plan = planner.update(
                WriteSpec.update(form(), Map.of("name", "changed"), where).withLock(lock));

        assertNull(plan.protectedWrite());
        assertEquals(List.of("changed", 1L, 2L, 7L), plan.request().parameters());
        assertThrows(OptimisticLockConflictException.class, () -> plan.requireSuccess(0));
        assertThrows(IllegalArgumentException.class, () -> planner.update(
                WriteSpec.update(form(), Map.of("version", 8L), where).withLock(lock)));
    }

    @Test
    void effectiveScopeStillUnwrapsAFieldHiddenByTheReadableView() {
        DynamicForm physical = form();
        DynamicForm visible = DynamicForm.builder("plain_users", "plain_users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DataScope scope = DataScope.where(ConditionGroup.and().where("tenant_id", "=", 9L).build());
        ConditionGroup scoped = FormDataScopes.apply(visible, ConditionGroup.and().build(), scope);

        ConditionGroup prepared = renderer().protection().prepareQuery(
                physical, physical, visible, scoped, scope).where();

        TermCondition term = (TermCondition) prepared.children().getFirst();
        assertEquals(9L, term.value());
        assertFalse(term.value() instanceof FormDataScopes.TrustedScopeValue);
    }

    private static DynamicForm form() {
        return DynamicForm.builder("plain_users", "plain_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("created_at", "DATE"))
                .addField(DynamicField.of("version", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }
}
