package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.execution.QueryRoutingIntent;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LockingReadIntentAvailableBeforeTransactionTest {

    @Test
    void exposesPrimaryRoutingBeforeAnyConnectionOrTransactionLookup() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());

        LockingReadSpec spec = LockingReadSpec.of(query, ReadLock.updateNowait());

        assertEquals(QueryRoutingIntent.PRIMARY_REQUIRED, spec.routingIntent());
        assertEquals(ReadLockStrength.UPDATE, spec.lock().strength());
        assertEquals(ReadLockWait.NOWAIT, spec.lock().waitMode());
    }
}
