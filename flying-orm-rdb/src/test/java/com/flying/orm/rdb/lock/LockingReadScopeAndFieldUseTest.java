package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockingReadScopeAndFieldUseTest {

    @Test
    void lockingReadReusesScopeAndGovernedFieldApproval() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.jdbcTransaction(new AtomicInteger());
        var client = LockingReadTestSupport.syncClient(
                        RdbDialect.postgresql(), () -> Optional.of(transaction), queries, request)
                .withDefaultDataScope(DataScope.tenant("tenant_id", 9L))
                .withFieldUsePolicy(FieldUsePolicy.builder()
                        .visibility("id", FieldVisibility.FULL)
                        .allow("id", FieldUse.PROJECT, FieldUse.FILTER)
                        .allowInternal("tenant_id",
                                com.flying.orm.core.scope.FieldUseOrigin.INTERNAL_TENANT,
                                FieldUse.FILTER)
                        .build());
        QuerySpec query = QuerySpec.of(
                        LockingReadTestSupport.form(),
                        ConditionGroup.and().where("id", "=", 7L).build())
                .withProjection(List.of("id"), List.of());

        client.lockingRead(LockingReadSpec.of(query, ReadLock.update()));

        assertEquals(List.of(7L, 9L), request.get().parameters());
        assertTrue(request.get().sql().contains("tenant_id"), request.get().sql());
        assertEquals(1, queries.get());

        var denied = client.withFieldUsePolicy(FieldUsePolicy.builder()
                .visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.PROJECT)
                .build());
        assertThrows(ScopeAccessException.class,
                     () -> denied.lockingRead(LockingReadSpec.of(query, ReadLock.update())));
        assertEquals(1, queries.get());
    }
}
