package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.EntitySchemaSyncMode;
import com.flying.orm.rdb.schema.EntitySchemaSynchronizer;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandaloneEntitySchemaInvalidationTest {

    private static final String TABLE = "standalone_account";
    @TestFactory
    List<DynamicTest> fullUpdateInvalidatesItsReaderAfterDdlStarts() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean fails : List.of(false, true)) {
            tests.add(DynamicTest.dynamicTest("fails=" + fails, () -> {
                try (Fixture fixture = new Fixture(false, fails)) {
                    assertEquals(1, fixture.reader.readTable(TABLE).columns().size());
                    assertEquals(1, fixture.reader.readForm(TABLE, TABLE).fields().size());
                    if (fails) {
                        SqlExecutionSequenceException failure = assertThrows(
                                SqlExecutionSequenceException.class, fixture::synchronize);
                        assertSame(fixture.ddlFailure, failure.getCause());
                        assertEquals(SqlExecutionPhase.WORK, failure.phase());
                        assertEquals(0, failure.stepIndex());
                        assertTrue(failure.completedWorkSteps().isEmpty());
                    } else {
                        fixture.synchronize();
                    }
                    assertAll(
                            () -> assertEquals(1, fixture.ddl),
                            () -> assertEquals(List.of(TABLE), fixture.readerInvalidations),
                            () -> assertEquals(List.of(TABLE), fixture.clientInvalidations),
                            () -> assertEquals(2, fixture.reader.readTable(TABLE).columns().size()),
                            () -> assertEquals(2, fixture.reader.readForm(TABLE, TABLE).fields().size()));
                }
            }));
        }
        return tests;
    }

    @org.junit.jupiter.api.Test
    void fullUpdateWithoutSqlDoesNotInvalidate() {
        try (Fixture fixture = new Fixture(true, false)) {
            RelationalTableDefinition cached = fixture.reader.readTable(TABLE);
            fixture.synchronize();
            assertEquals(0, fixture.ddl);
            assertTrue(fixture.readerInvalidations.isEmpty());
            assertTrue(fixture.clientInvalidations.isEmpty());
            assertSame(cached, fixture.reader.readTable(TABLE));
        }
    }

    @TableName(TABLE)
    private static final class Account {
        @TableId
        private Long id;
        private String note;
    }

    private static final class Fixture implements AutoCloseable {
        private final EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled());
        private final DynamicForm desired = models.schemaDescriptor(Account.class).form();
        private final List<String> readerInvalidations = new ArrayList<>();
        private final List<String> clientInvalidations = new ArrayList<>();
        private final IllegalStateException ddlFailure = new IllegalStateException("DDL result failed");
        private final JdbcFormMetadataReader reader;
        private final JdbcSchemaClient client;
        private final EntitySchemaSynchronizer synchronizer;
        private RelationalTableDefinition current;
        private int ddl;

        private Fixture(boolean complete, boolean fails) {
            RelationalTableDefinition.Builder initial = RelationalTableDefinition.builder(TABLE);
            RelationalTableDefinition definition = desired.toTableDefinition();
            definition.primaryKey().ifPresent(initial::primaryKey);
            definition.columns().stream()
                    .filter(column -> complete || definition.primaryKey().orElseThrow().columns().contains(column.name()))
                    .forEach(initial::addColumn);
            current = initial.build();
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    assertEquals("columns", request.sql());
                    return current.columns().stream().map(column -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("COLUMN_NAME", column.name());
                        row.put("DATA_TYPE", column.databaseType().declaration());
                        row.put("PRIMARY_KEY", current.primaryKey().orElseThrow().columns().contains(column.name()));
                        row.put("NULLABLE", column.nullable());
                        row.put("CHARACTER_MAXIMUM_LENGTH", column.length());
                        return DynamicRow.copyOf(row);
                    }).toList();
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    ddl++;
                    assertTrue(request.sql().contains("note"));
                    current = desired.toTableDefinition();
                    if (fails) {
                        throw ddlFailure;
                    }
                    return 0L;
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("DDL does not request generated keys");
                }
            };
            MetadataCacheInvalidator dependent = new MetadataCacheInvalidator() {
                @Override
                public void invalidate(com.flying.orm.core.metadata.RelationIdentity relation) {
            String table = relation.qualifiedName();
                    readerInvalidations.add(table);
                }

                @Override
                public void invalidateAll() {
                    throw new AssertionError("single-table DDL must use table invalidation");
                }
            };
            reader = new DefaultJdbcFormMetadataReader(executor, new MetadataQueryProfile(new MetadataQueryProfile.Queries(
                    (schema, table) -> new SqlRequest("columns", List.of()), null, null, type -> type)), CacheRegionPolicy.metadataDefaults(), dependent);
            client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                    .withMetadataInvalidator(relation -> clientInvalidations.add(relation.qualifiedName()));
            JdbcFormMetadataReader schemaReader = new JdbcFormMetadataReader() {
                public com.flying.orm.rdb.schema.SchemaSnapshotCoverage snapshotCoverage() {
                    return com.flying.orm.rdb.schema.SchemaSnapshotCoverage.complete();
                }
            public com.flying.orm.rdb.schema.SchemaSnapshot readSnapshot(RelationIdentity identity) {
                return com.flying.orm.rdb.schema.SchemaSnapshot.present(current);
            }
            public DynamicForm readForm(String id, RelationIdentity relation) {
                String table = relation.qualifiedName();
                return reader.readForm(id, table);
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

                public void invalidate(com.flying.orm.core.metadata.RelationIdentity relation) {
            String table = relation.qualifiedName(); reader.invalidate(table); }
            };
            synchronizer = new EntitySchemaSynchronizer(models, null, null, client, schemaReader);
        }

        private void synchronize() {
            synchronizer.synchronize(com.flying.orm.rdb.dialect.DatabaseDescriptor.of("H2", "2.3", RdbDialect.h2()),
                    EntitySchemaSyncMode.FULL_UPDATE, List.of(Account.class));
        }

        @Override
        public void close() {
            models.close();
        }
    }
}
