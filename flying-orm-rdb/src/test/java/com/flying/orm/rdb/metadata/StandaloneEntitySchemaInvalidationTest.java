package com.flying.orm.rdb.metadata;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.EntitySchemaSyncMode;
import com.flying.orm.rdb.schema.EntitySchemaSynchronizer;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationApproval;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
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
    private static final SchemaMigrationOptions FULL_OPTIONS = SchemaMigrationOptions.safe()
            .allowDropColumn().allowColumnChange().allowPrimaryKeyChange().allowDropIndex().allowRebuildIndex();

    @TestFactory
    List<DynamicTest> fullUpdateInvalidatesItsReaderAfterDdlStarts() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean approved : List.of(false, true)) {
            for (boolean fails : List.of(false, true)) {
                tests.add(DynamicTest.dynamicTest("approved=" + approved + "/fails=" + fails, () -> {
                    try (Fixture fixture = new Fixture(false, fails)) {
                        assertEquals(1, fixture.reader.readTable(TABLE).columns().size());
                        assertEquals(1, fixture.reader.readForm(TABLE, TABLE).fields().size());
                        Map<String, SchemaMigrationApproval> approvals = fixture.approvals(approved);
                        if (fails) {
                            assertSame(fixture.ddlFailure, assertThrows(IllegalStateException.class,
                                    () -> fixture.synchronize(approvals)));
                        } else {
                            fixture.synchronize(approvals);
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
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> fullUpdateWithoutSqlDoesNotInvalidate() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean approved : List.of(false, true)) {
            tests.add(DynamicTest.dynamicTest("approved=" + approved, () -> {
                try (Fixture fixture = new Fixture(true, false)) {
                    TableMetadata cached = fixture.reader.readTable(TABLE);
                    fixture.synchronize(fixture.approvals(approved));
                    assertEquals(0, fixture.ddl);
                    assertTrue(fixture.readerInvalidations.isEmpty());
                    assertTrue(fixture.clientInvalidations.isEmpty());
                    assertSame(cached, fixture.reader.readTable(TABLE));
                }
            }));
        }
        return tests;
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
        private TableMetadata current;
        private int ddl;

        private Fixture(boolean complete, boolean fails) {
            TableMetadata.Builder initial = TableMetadata.builder(TABLE);
            desired.toTableMetadata().columns().stream()
                    .filter(column -> complete || column.primaryKey()).forEach(initial::addColumn);
            current = initial.build();
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    assertEquals("columns", request.sql());
                    return current.columns().stream().map(column -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("COLUMN_NAME", column.name());
                        row.put("DATA_TYPE", column.dataType());
                        row.put("PRIMARY_KEY", column.primaryKey());
                        row.put("NULLABLE", column.nullable());
                        row.put("CHARACTER_MAXIMUM_LENGTH", column.length());
                        return DynamicRow.copyOf(row);
                    }).toList();
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    ddl++;
                    assertTrue(request.sql().contains("note"));
                    current = desired.toTableMetadata();
                    if (fails) {
                        throw ddlFailure;
                    }
                    return 0L;
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("DDL does not request generated keys");
                }
            };
            MetadataCacheInvalidator dependent = new MetadataCacheInvalidator() {
                @Override
                public void invalidate(String table) {
                    readerInvalidations.add(table);
                }

                @Override
                public void invalidateAll() {
                    throw new AssertionError("single-table DDL must use table invalidation");
                }
            };
            reader = new JdbcFormMetadataReader(executor, new InformationSchemaFormMetadataReader.Queries(
                    (schema, table) -> new SqlRequest("columns", List.of()), null, null, type -> type),
                    CacheRegionPolicy.metadataDefaults(), dependent);
            client = JdbcSchemaClient.create(executor, RdbDialect.h2())
                    .withMetadataInvalidator(clientInvalidations::add);
            synchronizer = new EntitySchemaSynchronizer(models, null, null, client, reader);
        }

        private Map<String, SchemaMigrationApproval> approvals(boolean approved) {
            if (!approved) {
                return Map.of();
            }
            var reviewed = client.reviewCreateOrAlter(desired, List.of(), List.of(), reader,
                    FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
            return Map.of(TABLE, SchemaMigrationApproval.approve(reviewed, "reviewed fixture DDL"));
        }

        private void synchronize(Map<String, SchemaMigrationApproval> approvals) {
            synchronizer.synchronize(EntitySchemaSyncMode.FULL_UPDATE, approvals, List.of(Account.class));
        }

        @Override
        public void close() {
            models.close();
        }
    }
}
