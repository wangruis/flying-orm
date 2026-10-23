package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

class QueryLockContractTest {

    @Test
    void retainsQueryAndExplicitSqlLockSemantics() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());

        QuerySpec spec = query.withLock(ReadLock.updateNowait());

        assertSame(query.form(), spec.form());
        assertSame(query.where(), spec.where());
        assertEquals(ReadLockStrength.UPDATE, spec.lock().orElseThrow().strength());
        assertEquals(ReadLockWait.NOWAIT, spec.lock().orElseThrow().waitMode());
        assertTrue(query.lock().isEmpty());
        QuerySpec.Builder builder = QuerySpec.builder(form).lock(ReadLock.updateSkipLocked());
        QuerySpec frozen = builder.build();
        QuerySpec.Builder copy = builder.copy();
        builder.lock(ReadLock.update());
        assertEquals(ReadLockWait.SKIP_LOCKED, frozen.lock().orElseThrow().waitMode());
        assertEquals(ReadLockWait.SKIP_LOCKED, copy.build().lock().orElseThrow().waitMode());
        assertEquals(ReadLockWait.SKIP_LOCKED, frozen.withWhere(query.where()).withExcludedFields(
                java.util.List.of("missing")).lock().orElseThrow().waitMode());
        assertTrue(builder.buildCount().lock().isEmpty());
    }
}
