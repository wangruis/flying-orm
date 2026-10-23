package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.EntitySchemaSyncMode;
import com.flying.orm.rdb.schema.EntitySchemaSyncReport;
import com.flying.orm.rdb.schema.EntitySchemaSynchronizer;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntitySchemaSynchronizerPlanningTest {

    private static final List<Class<?>> TYPES = List.of(First.class, Second.class, Third.class);
    private static final SchemaMigrationOptions FULL_OPTIONS = SchemaMigrationOptions.safe()
            .allowDropColumn().allowColumnChange().allowDropIndex().allowRebuildIndex();

    @Test
    void jdbcFullUpdateReadsEachTargetOnceAndRetainsTheFullReview() {
        try (Fixture fixture = new Fixture(false)) {
            List<SchemaMigrationPlan> expectedPlans = fixture.expectedPlans(false);
            fixture.reads = 0;

            EntitySchemaSyncReport report = fixture.synchronize(false, EntitySchemaSyncMode.FULL_UPDATE);

            assertNoDifferences(report, expectedPlans);
            assertEquals(3, fixture.reads, "FULL_UPDATE must not read an abandoned SAFE plan first");
            assertEquals(0, fixture.ddl);
        }
    }

    @Test
    void reactiveFullUpdateIsColdAndReadsEachTargetOncePerSubscription() {
        try (Fixture fixture = new Fixture(false)) {
            List<SchemaMigrationPlan> expectedPlans = fixture.expectedPlans(true);
            fixture.reads = 0;
            Mono<EntitySchemaSyncReport> pending = fixture.synchronizer.synchronizeReactive(
                    database(), EntitySchemaSyncMode.FULL_UPDATE, TYPES);
            assertEquals(0, fixture.reads);

            assertNoDifferences(pending.block(), expectedPlans);
            assertEquals(3, fixture.reads, "FULL_UPDATE must not read an abandoned SAFE plan first");
            assertNoDifferences(pending.block(), expectedPlans);
            assertEquals(6, fixture.reads, "each subscription must independently review current metadata");
            assertEquals(0, fixture.ddl);
        }
    }

    @Test
    void jdbcValidateAndSafeUpdateReadEachTargetOnce() {
        assertOtherModes(false);
    }

    @Test
    void reactiveValidateAndSafeUpdateReadEachTargetOnce() {
        assertOtherModes(true);
    }

    @Test
    void jdbcFullUpdateExecutesAllExplicitChanges() {
        assertFullUpdateExecutes(false);
    }

    @Test
    void reactiveFullUpdateExecutesAllExplicitChanges() {
        assertFullUpdateExecutes(true);
    }

    private static void assertNoDifferences(EntitySchemaSyncReport report, List<SchemaMigrationPlan> expectedPlans) {
        assertEquals(EntitySchemaSyncMode.FULL_UPDATE, report.mode());
        assertEquals(3, report.plans().size());
        assertEquals(3, report.results().size());
        assertFalse(report.hasDifferences());
        assertEquals(expectedPlans.size(), report.plans().size());
        for (int index = 0; index < expectedPlans.size(); index++) {
            SchemaMigrationPlan expected = expectedPlans.get(index);
            SchemaMigrationPlan actual = report.plans().get(index);
            assertEquals(expected.target().name(), actual.target().name());
            assertEquals(expected.target().columns(), actual.target().columns());
            assertEquals(expected.requests(), actual.requests());
            assertEquals(expected.skippedChanges(), actual.skippedChanges());
        }
    }

    private static void assertOtherModes(boolean reactive) {
        try (Fixture fixture = new Fixture(false)) {
            EntitySchemaSyncReport validation = fixture.synchronize(reactive, EntitySchemaSyncMode.VALIDATE);
            assertEquals(3, fixture.reads);
            assertEquals(3, validation.plans().size());
            assertTrue(validation.results().isEmpty());

            fixture.reads = 0;
            EntitySchemaSyncReport safe = fixture.synchronize(reactive, EntitySchemaSyncMode.SAFE_UPDATE);
            assertEquals(3, fixture.reads, "SAFE_UPDATE executes the planned SQL without reading it again");
            assertEquals(3, safe.plans().size());
            assertEquals(3, safe.results().size());
            assertEquals(0, fixture.ddl);
        }
    }

    private static void assertFullUpdateExecutes(boolean reactive) {
        try (Fixture fixture = new Fixture(true)) {
            EntitySchemaSyncReport report = fixture.synchronize(reactive, EntitySchemaSyncMode.FULL_UPDATE);
            assertEquals(3, fixture.ddl);
            assertEquals(3, report.results().size());
            assertTrue(report.plans().stream().allMatch(plan -> plan.requests().size() == 1));
        }
    }
    private static final class Fixture implements AutoCloseable {
        private final EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
        private final Map<String, RelationalTableDefinition> tables = new LinkedHashMap<>();
        private final JdbcSchemaClient jdbc;
        private final ReactiveSchemaClient reactive;
        private final JdbcFormMetadataReader jdbcMetadata;
        private final ReactiveFormMetadataReader reactiveMetadata;
        private final EntitySchemaSynchronizer synchronizer;
        private int reads;
        private int ddl;

        private Fixture(boolean withLegacyColumn) {
            for (Class<?> type : TYPES) {
                DynamicForm form = models.metadata(type).toDynamicForm();
                RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(form.table());
                form.toTableDefinition().columns().forEach(table::addColumn);
                form.toTableDefinition().primaryKey().ifPresent(table::primaryKey);
                if (withLegacyColumn) {
                    table.addColumn(DynamicField.of("legacy", "BIGINT").column());
                }
                tables.put(form.table(), table.build());
            }
            SyncSqlExecutor syncExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("metadata comes from the explicit reader");
                }
                public long rowsUpdated(SqlRequest request) { ddl++; return 1; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("schema DDL must not request generated keys");
                }
            };
            jdbcMetadata = new JdbcFormMetadataReader() {
                public com.flying.orm.rdb.schema.SchemaSnapshotCoverage snapshotCoverage() {
                    return com.flying.orm.rdb.schema.SchemaSnapshotCoverage.complete();
                }
            public SchemaSnapshot readSnapshot(RelationIdentity identity) {
                reads++;
                return SchemaSnapshot.present(tables.get(identity.table()));
            }
            public RelationalTableDefinition readTableForSchema(RelationIdentity relation) {
                String table = relation.qualifiedName();
                reads++;
                return tables.get(table);
            }
            public DynamicForm readForm(String id, RelationIdentity relation) {
                throw new AssertionError("use table facts");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            };
            jdbc = JdbcSchemaClient.create(syncExecutor, RdbDialect.h2());
            reactiveMetadata = new ReactiveFormMetadataReader() {
                public com.flying.orm.rdb.schema.SchemaSnapshotCoverage snapshotCoverage() {
                    return jdbcMetadata.snapshotCoverage();
                }
            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity identity) {
                return Mono.fromSupplier(() -> jdbcMetadata.readSnapshot(identity));
            }
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                String table = relation.qualifiedName();
                return Mono.fromSupplier(() -> {
                    reads++;
                    return tables.get(table);
                });
            }

            public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
                return Mono.error(new AssertionError("schema review must read table metadata"));
            }

            };
            reactive = ReactiveSchemaClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("metadata comes from the explicit reader"));
                }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(() -> { ddl++; return 1L; });
                }
            }, RdbDialect.h2());
            synchronizer = new EntitySchemaSynchronizer(models, reactive, reactiveMetadata, jdbc, jdbcMetadata);
        }

        private List<SchemaMigrationPlan> expectedPlans(boolean useReactive) {
            return TYPES.stream().map(type -> review(useReactive, type)).toList();
        }

        private SchemaMigrationPlan review(boolean useReactive, Class<?> type) {
            var metadata = models.metadata(type);
            return useReactive
                    ? reactive.planCreateOrAlter(metadata.toDynamicForm(), metadata.targetIndexes(), List.of(),
                            reactiveMetadata, FULL_OPTIONS.preferConcurrentIndexes()).block()
                    : jdbc.planCreateOrAlter(metadata.toDynamicForm(), metadata.targetIndexes(), List.of(),
                            jdbcMetadata, FULL_OPTIONS.preferConcurrentIndexes());
        }

        private EntitySchemaSyncReport synchronize(boolean useReactive, EntitySchemaSyncMode mode) {
            return useReactive ? synchronizer.synchronizeReactive(database(), mode, TYPES).block()
                    : synchronizer.synchronize(database(), mode, TYPES);
        }

        public void close() { models.close(); }
    }

    private static com.flying.orm.rdb.dialect.DatabaseDescriptor database() {
        return com.flying.orm.rdb.dialect.DatabaseDescriptor.of("H2", "2.3", RdbDialect.h2());
    }

    @TableName("schema_sync_first")
    static class First { @TableId Long id; }

    @TableName("schema_sync_second")
    static class Second { @TableId Long id; }

    @TableName("schema_sync_third")
    static class Third { @TableId Long id; }
}
