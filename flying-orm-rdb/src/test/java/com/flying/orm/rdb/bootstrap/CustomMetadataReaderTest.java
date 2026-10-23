package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomMetadataReaderTest {
    private static final JdbcConnectionAccess JDBC = new JdbcConnectionAccess() {
        public java.sql.Connection getConnection(SqlRequest request) { throw new AssertionError("unexpected JDBC I/O"); }
        public void releaseConnection(java.sql.Connection connection, SqlRequest request) {
            throw new AssertionError("unexpected JDBC release");
        }
    };
    private static final R2dbcConnectionAccess REACTIVE = new R2dbcConnectionAccess() {
        public Publisher<? extends io.r2dbc.spi.Connection> getConnection(SqlRequest request) {
            throw new AssertionError("unexpected R2DBC I/O");
        }
        public Publisher<Void> releaseConnection(SignalType signal, io.r2dbc.spi.Connection connection, SqlRequest request) {
            throw new AssertionError("unexpected R2DBC release");
        }
    };
    private static final RelationalTableDefinition TABLE = RelationalTableDefinition.builder(RelationIdentity.table("records"))
            .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build();

    @Test
    void customDialectUsesBothSuppliedReadersWithoutHiddenCachingOrIo() {
        RdbDialect base = RdbDialect.h2();
        RdbDialect custom = RdbDialect.of("application_dialect", base.schema(), base.pagination(),
                base.upsert(), base.json());
        JdbcReader jdbc = new JdbcReader();
        ReactiveReader reactive = new ReactiveReader();
        try (FlyingOrmClients clients = FlyingOrmClients.builder(JDBC, REACTIVE).dialect(custom)
                .metadataReader(jdbc).metadataReader(reactive).build()) {
            assertSame(reactive, clients.metadata());
            assertEquals(0, jdbc.formReads);
            assertEquals(0, reactive.formReads);
            clients.syncOperator().metadata().readForm("recordForm", "records");
            clients.syncOperator().metadata().readForm("recordForm", "records");
            clients.syncOperator().metadata().readTable("public", "records");
            assertEquals(3, jdbc.formReads);
            Mono<DynamicForm> pending = clients.metadata().readForm("recordForm", "public", "records");
            assertEquals(0, reactive.formReads);
            assertEquals("public.records", pending.block().table());
            pending.block();
            assertEquals(2, reactive.formReads);
            clients.syncOperator().metadata().invalidate("public", "records");
            assertEquals(List.of("public.records"), jdbc.invalidated);
            assertEquals(jdbc.invalidated, reactive.invalidated);
        }
        assertEquals(1, jdbc.clears);
        assertEquals(1, reactive.clears);
    }

    @Test
    void schemaReviewUsesCustomFreshSnapshotsAndCoverageOnBothPaths() {
        JdbcReader jdbc = new JdbcReader();
        ReactiveReader reactive = new ReactiveReader();
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.4.240", dialect);
        try (FlyingOrmClients clients = FlyingOrmClients.builder(JDBC, REACTIVE).dialect(dialect)
                .metadataReader(jdbc).metadataReader(reactive).build()) {
            assertTrue(clients.syncOperator().ddl().plan(database, TABLE).steps().isEmpty());
            assertTrue(clients.syncOperator().ddl().plan(database, TABLE).steps().isEmpty());
            assertEquals(2, jdbc.snapshotReads);
            assertEquals(0, jdbc.formReads);
            Mono<?> pending = clients.operator().ddl().plan(database, TABLE);
            assertEquals(0, reactive.snapshotReads);
            pending.block();
            pending.block();
            assertEquals(2, reactive.snapshotReads);
            assertEquals(0, reactive.formReads);
        }
    }

    @Test
    void minimalJdbcExtensionUsesDefaultsWithoutInventingCompleteFacts() {
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
            public DynamicForm readForm(String formId, RelationIdentity relation) {
                String table = relation.qualifiedName();
                return form(formId, table);
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

        };
        assertEquals(1, reader.readTable("records").columns().size());
        assertTrue(reader.snapshotCoverage().observedFacts().isEmpty());
        assertFalse(reader.readSnapshot("records").unknownAttributes().isEmpty());
        assertEquals(RelationIdentity.of(null, "public", "records"),
                reader.readSnapshot(RelationIdentity.of(null, "public", "records")).identity());
        assertEquals(RelationIdentity.table("literal.dot"),
                reader.readSnapshot(RelationIdentity.table("literal.dot")).identity());
        assertEquals(RelationIdentity.of("catalog", "public", "records"),
                reader.readSnapshot(RelationIdentity.of("catalog", "public", "records")).identity());
        reader.invalidate("records");
        reader.invalidateAll();
    }

    private static DynamicForm form(String formId, String table) {
        return DynamicForm.builder(formId, table).addField(DynamicField.of("id", "BIGINT")).build();
    }

    private static final class JdbcReader extends JdbcFormMetadataReader {
        private int formReads;
        private int snapshotReads;
        private int clears;
        private final List<String> invalidated = new ArrayList<>();

        public DynamicForm readForm(String formId, RelationIdentity relation) {
            String table = relation.qualifiedName();
            formReads++;
            return form(formId, table);
        }

        @Override
        public RelationalTableDefinition readTable(RelationIdentity relation) {
            return readForm(relation.table(), relation).toTableDefinition(relation);
        }

        public SchemaSnapshotCoverage snapshotCoverage() { return SchemaSnapshotCoverage.complete(); }
        public SchemaSnapshot readSnapshot(RelationIdentity relation) {
            snapshotReads++;
            assertEquals(TABLE.identity(), relation);
            return SchemaSnapshot.present(TABLE);
        }
        public void invalidate(com.flying.orm.core.metadata.RelationIdentity relation) {
            String table = relation.qualifiedName(); invalidated.add(table); }
        public void invalidateAll() { clears++; }
    }

    private static final class ReactiveReader implements ReactiveFormMetadataReader, MetadataCacheInvalidator {
        private int formReads;
        private int snapshotReads;
        private int clears;
        private final List<String> invalidated = new ArrayList<>();

        public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
            String table = relation.qualifiedName();
            return Mono.fromSupplier(() -> {
                formReads++;
                return form(formId, table);
            });
        }

        @Override
        public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
            return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
        }

        public SchemaSnapshotCoverage snapshotCoverage() { return SchemaSnapshotCoverage.complete(); }
        public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
            return Mono.fromSupplier(() -> {
                snapshotReads++;
                assertEquals(TABLE.identity(), relation);
                return SchemaSnapshot.present(TABLE);
            });
        }
        public void invalidate(com.flying.orm.core.metadata.RelationIdentity relation) {
            String table = relation.qualifiedName(); invalidated.add(table); }
        public void invalidateAll() { clears++; }
    }
}
