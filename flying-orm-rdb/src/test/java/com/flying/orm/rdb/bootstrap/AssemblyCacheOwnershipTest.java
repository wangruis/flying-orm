package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.SignalType;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssemblyCacheOwnershipTest {
    private static final JdbcConnectionAccess JDBC = new JdbcConnectionAccess() {
        public java.sql.Connection getConnection(SqlRequest request) { throw new AssertionError("assembly acquired JDBC"); }
        public void releaseConnection(java.sql.Connection connection, SqlRequest request) {
            throw new AssertionError("assembly released JDBC");
        }
    };
    private static final R2dbcConnectionAccess REACTIVE = new R2dbcConnectionAccess() {
        public Publisher<? extends io.r2dbc.spi.Connection> getConnection(SqlRequest request) {
            throw new AssertionError("assembly acquired R2DBC");
        }
        public Publisher<Void> releaseConnection(SignalType signal, io.r2dbc.spi.Connection connection, SqlRequest request) {
            throw new AssertionError("assembly released R2DBC");
        }
    };

    @Test
    void dualRuntimeSchemasAndCloseShareOneFinalInvalidationOwner() throws Exception {
        try (var clients = FlyingOrmClients.builder(JDBC, REACTIVE).dialect(RdbDialect.h2()).build()) {
            Object coordinator = cacheOwner(clients);
            assertSame(coordinator, field(clients.schema(), "metadataInvalidator"));
            assertSame(coordinator, field(clients.syncSchema(), "metadataInvalidator"));
            assertSame(coordinator, field(clients.syncOperator(), "metadataInvalidator"));
            assertSame(clients.forms().entityModels(), clients.syncForms().entityModels());
        }
    }

    @Test
    void jdbcOnlyCreatesNoReactiveRuntimeAndUsesFinalCacheOwner() throws Exception {
        try (var clients = FlyingOrmClients.builder(JDBC).dialect(RdbDialect.h2()).build()) {
            assertTrue(clients.jdbcAvailable());
            assertFalse(clients.reactiveAvailable());
            assertSame(cacheOwner(clients),
                    field(clients.syncSchema(), "metadataInvalidator"));
        }
    }

    @Test
    void reactiveOnlyCreatesNoJdbcRuntimeAndUsesFinalCacheOwner() throws Exception {
        try (var clients = FlyingOrmClients.builder(REACTIVE).dialect(RdbDialect.h2()).build()) {
            assertTrue(clients.reactiveAvailable());
            assertFalse(clients.jdbcAvailable());
            assertSame(cacheOwner(clients),
                    field(clients.schema(), "metadataInvalidator"));
        }
    }

    private static Object cacheOwner(FlyingOrmClients clients) throws Exception {
        return field(field(clients, "sharedResources"), "metadata");
    }

    @Test
    void sharedCacheClosesOnlyAfterTheLastViewWithoutReadingMetadata() {
        AtomicInteger invalidations = new AtomicInteger();
        JdbcFormMetadataReader metadata = new JdbcFormMetadataReader() {
            @Override
            public DynamicForm readForm(String id, RelationIdentity relation) {
                throw new AssertionError("closing must not read metadata");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            @Override
            public void invalidateAll() {
                invalidations.incrementAndGet();
            }
        };
        try (FlyingOrmClients original = FlyingOrmClients.builder(JDBC).dialect(RdbDialect.h2())
                .metadataReader(metadata).build();
             FlyingOrmClients derived = original.withDefaultDataScope(DataScope.none())) {
            original.close();
            original.close();
            assertEquals(0, invalidations.get());
            assertTrue(derived.jdbcAvailable());
            derived.close();
            derived.close();
            assertEquals(1, invalidations.get());
        }
        assertEquals(1, invalidations.get());
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }
}
