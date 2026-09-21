package com.flying.orm.rdb.metadata;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
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
import com.flying.orm.rdb.schema.ReviewedSchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationApproval;
import com.flying.orm.rdb.schema.SchemaMigrationFailureCode;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationRejectedException;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntitySchemaSynchronizerPlanningTest {

    private static final List<Class<?>> TYPES = List.of(First.class, Second.class, Third.class);
    private static final SchemaMigrationOptions FULL_OPTIONS = SchemaMigrationOptions.safe()
            .allowDropColumn().allowColumnChange().allowPrimaryKeyChange().allowDropIndex().allowRebuildIndex();

    @Test
    void jdbcFullUpdateReadsEachTargetOnceAndRetainsTheFullReview() {
        try (Fixture fixture = new Fixture(false)) {
            List<String> fingerprints = fixture.fingerprints(false);
            fixture.reads = 0;

            EntitySchemaSyncReport report = fixture.synchronize(false, EntitySchemaSyncMode.FULL_UPDATE, Map.of());

            assertNoDifferences(report, fingerprints);
            assertEquals(3, fixture.reads, "FULL_UPDATE must not read an abandoned SAFE plan first");
            assertEquals(0, fixture.ddl);
        }
    }

    @Test
    void reactiveFullUpdateIsColdAndReadsEachTargetOncePerSubscription() {
        try (Fixture fixture = new Fixture(false)) {
            List<String> fingerprints = fixture.fingerprints(true);
            fixture.reads = 0;
            Mono<EntitySchemaSyncReport> pending = fixture.synchronizer.synchronizeReactive(
                    EntitySchemaSyncMode.FULL_UPDATE, Map.of(), TYPES);
            assertEquals(0, fixture.reads);

            assertNoDifferences(pending.block(), fingerprints);
            assertEquals(3, fixture.reads, "FULL_UPDATE must not read an abandoned SAFE plan first");
            assertNoDifferences(pending.block(), fingerprints);
            assertEquals(6, fixture.reads, "each subscription must independently review current metadata");
            assertEquals(0, fixture.ddl);
        }
    }

    @Test
    void jdbcValidateAndSafeUpdateKeepTheirExistingPlanningPasses() {
        assertOtherModes(false);
    }

    @Test
    void reactiveValidateAndSafeUpdateKeepTheirExistingPlanningPasses() {
        assertOtherModes(true);
    }

    @Test
    void jdbcChecksEveryDangerousApprovalBeforeExecutingAnyDdl() {
        assertApprovalsBeforeDdl(false);
    }

    @Test
    void reactiveChecksEveryDangerousApprovalBeforeExecutingAnyDdl() {
        assertApprovalsBeforeDdl(true);
    }

    private static void assertNoDifferences(EntitySchemaSyncReport report, List<String> fingerprints) {
        assertEquals(EntitySchemaSyncMode.FULL_UPDATE, report.mode());
        assertEquals(3, report.plans().size());
        assertEquals(3, report.results().size());
        assertFalse(report.hasDifferences());
        assertEquals(fingerprints, report.reviewedPlans().stream()
                                        .map(ReviewedSchemaMigrationPlan::fingerprint).toList());
    }

    private static void assertOtherModes(boolean reactive) {
        try (Fixture fixture = new Fixture(false)) {
            EntitySchemaSyncReport validation = fixture.synchronize(reactive, EntitySchemaSyncMode.VALIDATE, Map.of());
            assertEquals(3, fixture.reads);
            assertEquals(3, validation.plans().size());
            assertTrue(validation.reviewedPlans().isEmpty());
            assertTrue(validation.results().isEmpty());

            fixture.reads = 0;
            EntitySchemaSyncReport safe = fixture.synchronize(reactive, EntitySchemaSyncMode.SAFE_UPDATE, Map.of());
            assertEquals(6, fixture.reads);
            assertEquals(3, safe.plans().size());
            assertTrue(safe.reviewedPlans().isEmpty());
            assertEquals(3, safe.results().size());
            assertEquals(0, fixture.ddl);
        }
    }

    private static void assertApprovalsBeforeDdl(boolean reactive) {
        try (Fixture fixture = new Fixture(true)) {
            ReviewedSchemaMigrationPlan first = fixture.review(reactive, First.class);
            ReviewedSchemaMigrationPlan second = fixture.review(reactive, Second.class);
            assertTrue(first.requiresExplicitApproval());
            assertTrue(second.requiresExplicitApproval());
            Map<String, SchemaMigrationApproval> missingSecond = Map.of(
                    first.migration().target().table(), SchemaMigrationApproval.approve(first, "backup verified"));

            SchemaMigrationRejectedException missing = assertThrows(SchemaMigrationRejectedException.class,
                    () -> fixture.synchronize(reactive, EntitySchemaSyncMode.FULL_UPDATE, missingSecond));
            assertEquals(SchemaMigrationFailureCode.APPROVAL_REQUIRED, missing.failureCode());
            assertEquals(second.fingerprint(), missing.planFingerprint());
            assertEquals(0, fixture.ddl, "the first approved table must wait for all remaining approvals");

            Map<String, SchemaMigrationApproval> staleSecond = new LinkedHashMap<>(missingSecond);
            staleSecond.put(second.migration().target().table(),
                            new SchemaMigrationApproval("stale-plan-fingerprint", "old backup approval"));
            SchemaMigrationRejectedException stale = assertThrows(SchemaMigrationRejectedException.class,
                    () -> fixture.synchronize(reactive, EntitySchemaSyncMode.FULL_UPDATE, staleSecond));
            assertEquals(second.fingerprint(), stale.planFingerprint());
            assertEquals(0, fixture.ddl);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
        private final Map<String, TableMetadata> tables = new LinkedHashMap<>();
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
                TableMetadata.Builder table = TableMetadata.builder(form.table());
                form.toTableMetadata().columns().forEach(table::addColumn);
                if (withLegacyColumn) {
                    table.addColumn(ColumnMetadata.of("legacy", "BIGINT"));
                }
                tables.put(form.table(), table.build());
            }
            SyncSqlExecutor syncExecutor = new SyncSqlExecutor() {
                public List<DynamicRow> query(SqlRequest request) {
                    reads++;
                    return tables.get((String) request.parameters().getFirst()).columns().stream()
                                 .map(column -> DynamicRow.copyOf(Map.of(
                                         "COLUMN_NAME", column.name(), "DATA_TYPE", column.dataType(),
                                         "PRIMARY_KEY", column.primaryKey(), "NULLABLE", column.nullable())))
                                 .toList();
                }
                public long rowsUpdated(SqlRequest request) { ddl++; return 1; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("schema DDL must not request generated keys");
                }
            };
            jdbcMetadata = new JdbcFormMetadataReader(syncExecutor, new InformationSchemaFormMetadataReader.Queries(
                    (schema, table) -> new SqlRequest("select columns", List.of(table)), null, null, type -> type));
            jdbc = JdbcSchemaClient.create(syncExecutor, RdbDialect.h2());
            reactiveMetadata = new ReactiveFormMetadataReader() {
                public Mono<TableMetadata> readTable(String table) {
                    return Mono.fromSupplier(() -> { reads++; return tables.get(table); });
                }
                public Mono<DynamicForm> readForm(String formId, String table) {
                    return Mono.error(new AssertionError("schema review must read table metadata"));
                }
                public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                    return readForm(formId, table);
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

        private List<String> fingerprints(boolean useReactive) {
            return TYPES.stream().map(type -> review(useReactive, type).fingerprint()).toList();
        }

        private ReviewedSchemaMigrationPlan review(boolean useReactive, Class<?> type) {
            var metadata = models.metadata(type);
            return useReactive
                    ? reactive.reviewCreateOrAlter(metadata.toDynamicForm(), metadata.targetIndexes(), List.of(),
                            reactiveMetadata, FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline()).block()
                    : jdbc.reviewCreateOrAlter(metadata.toDynamicForm(), metadata.targetIndexes(), List.of(),
                            jdbcMetadata, FULL_OPTIONS, SchemaMigrationReviewPolicy.preferOnline());
        }

        private EntitySchemaSyncReport synchronize(boolean useReactive, EntitySchemaSyncMode mode,
                                                     Map<String, SchemaMigrationApproval> approvals) {
            return useReactive ? synchronizer.synchronizeReactive(mode, approvals, TYPES).block()
                    : synchronizer.synchronize(mode, approvals, TYPES);
        }

        public void close() { models.close(); }
    }

    @TableName("schema_sync_first")
    static class First { @TableId Long id; }

    @TableName("schema_sync_second")
    static class Second { @TableId Long id; }

    @TableName("schema_sync_third")
    static class Third { @TableId Long id; }
}
