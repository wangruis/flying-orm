package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.TableCheck;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableComment;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityRelationalSchemaExecutionTest {

    @Test
    void reactiveBatchCreatesSharedSequenceOnlyOnceAcrossTables() {
        assertSharedSequenceBatch(false, 1);
    }

    @Test
    void reactiveBatchReusesSequenceOwnedByLaterExistingTable() {
        assertSharedSequenceBatch(true, 0);
    }

    @Test
    void reactiveBatchReusesQualifiedSequenceReadBackFromLaterExistingTable() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(
                QualifiedSequenceFirst.class, QualifiedSequenceLater.class);
        MultiTableReactiveExecutor executor = new MultiTableReactiveExecutor(desired);
        RelationalTableDefinition later = desired.get("qualified_sequence_later");
        RelationalTableDefinition observed = RelationalTableDefinition.builder(later.identity())
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false)
                        .generation(ValueGeneration.sequence("shared_seq")).build())
                .primaryKey(later.primaryKey().orElseThrow())
                .build();
        executor.current.put("qualified_sequence_later", SchemaSnapshot.present(observed));
        MultiTableSnapshotReader reader = new MultiTableSnapshotReader(executor.current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(models, client, reader, null, null);
            EntityRelationalSchemaSyncException difference = assertThrows(
                    EntityRelationalSchemaSyncException.class,
                    () -> synchronizer.synchronizeRelationalReactive(
                            database, EntitySchemaSyncMode.VALIDATE,
                            List.of(QualifiedSequenceFirst.class, QualifiedSequenceLater.class)).block());
            EntityRelationalSchemaSyncReport report = difference.report();

            assertFalse(report.requiresManualAction());
            assertEquals(0, report.plans().stream().flatMap(plan -> plan.requests().stream())
                    .filter(request -> request.sql().startsWith("create sequence ")).count());
            assertTrue(executor.requests.isEmpty());
        }
    }

    private static void assertSharedSequenceBatch(boolean laterTableExists, long expectedCreates) {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(SequenceFirst.class, SequenceLater.class);
        MultiTableReactiveExecutor executor = new MultiTableReactiveExecutor(desired);
        if (laterTableExists) {
            executor.current.put("sequence_later", SchemaSnapshot.present(desired.get("sequence_later")));
        }
        MultiTableSnapshotReader reader = new MultiTableSnapshotReader(executor.current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(models, client, reader, null, null);
            EntityRelationalSchemaSyncReport report = synchronizer.synchronizeRelationalReactive(
                    database, EntitySchemaSyncMode.SAFE_UPDATE,
                    List.of(SequenceFirst.class, SequenceLater.class)).block();
            assertTrue(report.successful());
            assertEquals(expectedCreates, executor.requests.stream()
                    .filter(request -> request.sql().startsWith("create sequence ")).count());
        }
    }

    @TableName("sequence_first")
    @KeySequence("shared_seq")
    private static final class SequenceFirst {
        @TableId
        private Long id;
    }

    @TableName("sequence_later")
    @KeySequence("shared_seq")
    private static final class SequenceLater {
        @TableId
        private Long id;
    }

    @TableName(value = "qualified_sequence_first", schema = "tenant")
    @KeySequence("tenant.shared_seq")
    private static final class QualifiedSequenceFirst {
        @TableId
        private Long id;
    }

    @TableName(value = "qualified_sequence_later", schema = "tenant")
    @KeySequence("tenant.shared_seq")
    private static final class QualifiedSequenceLater {
        @TableId
        private Long id;
    }

    @Test
    void reactiveSynchronizationExecutesReferencedTableBeforeChildWhenCallerIsChildFirst() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(Parent.class, Child.class);
        MultiTableReactiveExecutor executor = new MultiTableReactiveExecutor(desired);
        MultiTableSnapshotReader reader = new MultiTableSnapshotReader(executor.current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, client, reader, null, null);
            Map<String, SchemaMigrationApproval> approvals = exactApprovals(
                    client, reader, database, desired);

            EntityRelationalSchemaSyncReport report = synchronizer.synchronizeRelationalReactive(
                    database,
                    EntitySchemaSyncMode.FULL_UPDATE,
                    approvals,
                    List.of(Child.class, Parent.class)).block();

            assertTrue(report.successful());
            assertEquals(List.of("parents", "children"), executor.createdTables);
        }
    }

    @Test
    void jdbcSynchronizationRejectsForeignKeyCycleBeforeSendingAnyDdl() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        EmptySyncExecutor executor = new EmptySyncExecutor();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, dialect);
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(executor, dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(CycleA.class, CycleB.class);
        Map<String, SchemaMigrationApproval> approvals = exactApprovals(
                client, reader, database, desired);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, null, null, client, reader);

            assertThrows(EntityRelationalSchemaSyncException.class, () -> synchronizer.synchronizeRelational(
                    database,
                    EntitySchemaSyncMode.FULL_UPDATE,
                    approvals,
                    List.of(CycleA.class, CycleB.class)));
            assertTrue(executor.updates.isEmpty());
        }
    }

    @Test
    void reactiveSynchronizationRejectsForeignKeyCycleBeforeSendingAnyDdl() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(CycleA.class, CycleB.class);
        MultiTableReactiveExecutor executor = new MultiTableReactiveExecutor(desired);
        MultiTableSnapshotReader reader = new MultiTableSnapshotReader(executor.current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        Map<String, SchemaMigrationApproval> approvals = exactApprovals(
                client, reader, database, desired);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, client, reader, null, null);

            assertThrows(EntityRelationalSchemaSyncException.class,
                    () -> synchronizer.synchronizeRelationalReactive(
                            database,
                            EntitySchemaSyncMode.FULL_UPDATE,
                            approvals,
                            List.of(CycleA.class, CycleB.class)).block());
            assertTrue(executor.requests.isEmpty());
        }
    }

    @Test
    void reactiveValidationAcceptsAnAlreadyMatchingForeignKeyCycle() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        Map<String, RelationalTableDefinition> desired = desiredTables(CycleA.class, CycleB.class);
        MultiTableReactiveExecutor executor = new MultiTableReactiveExecutor(desired);
        desired.forEach((table, definition) -> executor.current.put(
                table, SchemaSnapshot.present(definition)));
        MultiTableSnapshotReader reader = new MultiTableSnapshotReader(executor.current);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, client, reader, null, null);

            EntityRelationalSchemaSyncReport report = synchronizer.synchronizeRelationalReactive(
                    database,
                    EntitySchemaSyncMode.VALIDATE,
                    List.of(CycleA.class, CycleB.class)).block();

            assertFalse(report.hasDifferences());
            assertTrue(executor.requests.isEmpty());
        }
    }

    @Test
    void directExecutionRequiresAnExactApprovalBeforeSendingReviewedRiskSql() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition base = EntitySchemaDescriptor.builder(Account.class).build().table();
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(base.identity())
                .comment(base.comment());
        base.columns().forEach(builder::addColumn);
        base.primaryKey().ifPresent(builder::primaryKey);
        base.uniqueConstraints().forEach(builder::addUnique);
        base.indexes().forEach(builder::addIndex);
        base.checks().forEach(builder::addCheck);
        builder.addForeignKey(ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("id")
                .reference(base.identity())
                .addReferenceColumn("id")
                .onDelete(ReferentialAction.CASCADE)
                .build());
        RelationalTableDefinition desired = builder.build();

        AtomicReference<SchemaSnapshot> current = new AtomicReference<>(SchemaSnapshot.absent(desired.identity()));
        StatefulReactiveExecutor executor = new StatefulReactiveExecutor(current, desired);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        SnapshotReader reader = new SnapshotReader(current);
        ReviewedSchemaPlan plan = client.reviewRelational(
                database, desired, reader, SchemaCompatibilityMode.EXACT).block();

        SchemaExecutionReport rejected = client.executeReviewed(plan, reader).block();

        assertEquals(SchemaExecutionStatus.FAILED, rejected.status());
        assertFalse(rejected.steps().stream().anyMatch(SchemaExecutionReport.StepResult::sqlSent));
        assertTrue(executor.requests.isEmpty());

        SchemaExecutionReport approved = client.executeReviewed(
                plan, reader, SchemaMigrationApproval.approve(plan, "已审阅外键建表计划")).block();
        assertTrue(approved.successful());
    }

    @Test
    void entitySynchronizerExecutesTheCanonicalRelationOnTheReactivePath() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = EntitySchemaDescriptor.builder(Account.class).build().table();
        AtomicReference<SchemaSnapshot> current = new AtomicReference<>(SchemaSnapshot.absent(desired.identity()));
        StatefulReactiveExecutor executor = new StatefulReactiveExecutor(current, desired);
        ReactiveSchemaClient client = ReactiveSchemaClient.create(executor, dialect);
        SnapshotReader reader = new SnapshotReader(current);

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, client, reader, null, null);

            EntityRelationalSchemaSyncReport report = synchronizer.synchronizeRelationalReactive(
                    database, EntitySchemaSyncMode.SAFE_UPDATE, List.of(Account.class)).block();

            assertTrue(report.successful());
            assertEquals(1, report.plans().size());
            assertTrue(executor.requests.stream().anyMatch(
                    request -> request.sql().toLowerCase(Locale.ROOT).contains("constraint ck_accounts_id")));
        }
    }

    @Test
    void completeJdbcAndReactiveCoverageRetainFullDdl() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = EntitySchemaDescriptor.builder(Account.class).build().table();

        EmptySyncExecutor jdbcExecutor = new EmptySyncExecutor();
        JdbcSchemaClient jdbc = JdbcSchemaClient.create(jdbcExecutor, dialect);
        ReviewedSchemaPlan jdbcPlan = jdbc.reviewRelational(
                database,
                desired,
                JdbcFormMetadataReaders.create(jdbcExecutor, dialect),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        AtomicReference<SchemaSnapshot> current = new AtomicReference<>(SchemaSnapshot.absent(desired.identity()));
        StatefulReactiveExecutor reactiveExecutor = new StatefulReactiveExecutor(current, desired);
        ReactiveSchemaClient reactive = ReactiveSchemaClient.create(reactiveExecutor, dialect);
        SnapshotReader reader = new SnapshotReader(current);
        ReviewedSchemaPlan reactivePlan = reactive.reviewRelational(
                database, desired, reader, SchemaCompatibilityMode.SAFE_INCREMENTAL).block();

        assertCompleteEntityDdl(jdbcPlan);
        assertCompleteEntityDdl(reactivePlan);

        SchemaExecutionReport execution = reactive.executeReviewed(reactivePlan, reader).block();
        assertTrue(execution.successful());
        assertTrue(reactiveExecutor.requests.stream().anyMatch(
                request -> request.sql().toLowerCase(Locale.ROOT).contains("constraint uk_accounts_code")));
    }

    private static void assertCompleteEntityDdl(ReviewedSchemaPlan plan) {
        String sql = String.join("; ", plan.requests().stream().map(SqlRequest::sql).toList())
                .toLowerCase(Locale.ROOT);
        assertTrue(sql.contains("constraint pk_accounts primary key"));
        assertTrue(sql.contains("constraint uk_accounts_code unique"));
        assertTrue(sql.contains("constraint ck_accounts_id check"));
        assertTrue(sql.contains("index idx_accounts_code"));
        assertTrue(sql.contains("账户表"));
        assertTrue(sql.contains("账户编码"));
    }

    private static Map<String, RelationalTableDefinition> desiredTables(Class<?>... types) {
        Map<String, RelationalTableDefinition> desired = new LinkedHashMap<>();
        for (Class<?> type : types) {
            RelationalTableDefinition table = EntitySchemaDescriptor.builder(type).build().table();
            desired.put(table.identity().table(), table);
        }
        return desired;
    }

    private static Map<String, SchemaMigrationApproval> exactApprovals(
            ReactiveSchemaClient client,
            ReactiveFormMetadataReader reader,
            DatabaseDescriptor database,
            Map<String, RelationalTableDefinition> desired) {
        Map<String, SchemaMigrationApproval> approvals = new LinkedHashMap<>();
        desired.forEach((table, definition) -> {
            ReviewedSchemaPlan plan = client.reviewRelational(
                    database, definition, reader, SchemaCompatibilityMode.EXACT).block();
            approvals.put(table, SchemaMigrationApproval.approve(plan, "dependency order regression"));
        });
        return approvals;
    }

    private static Map<String, SchemaMigrationApproval> exactApprovals(
            JdbcSchemaClient client,
            JdbcFormMetadataReader reader,
            DatabaseDescriptor database,
            Map<String, RelationalTableDefinition> desired) {
        Map<String, SchemaMigrationApproval> approvals = new LinkedHashMap<>();
        desired.forEach((table, definition) -> {
            ReviewedSchemaPlan plan = client.reviewRelational(
                    database, definition, reader, SchemaCompatibilityMode.EXACT);
            approvals.put(table, SchemaMigrationApproval.approve(plan, "cycle rejection regression"));
        });
        return approvals;
    }

    @TableName("accounts")
    @TableComment("账户表")
    @TablePrimaryKey(name = "pk_accounts", properties = "id")
    @TableUnique(id = "account-code", name = "uk_accounts_code", properties = "code")
    @TableIndex(id = "account-code-index", name = "idx_accounts_code",
            columns = @TableIndexColumn(property = "code"))
    @TableCheck(id = "positive-id", name = "ck_accounts_id", property = "id",
            operator = TableCheck.Operator.GREATER_THAN, literalValues = "0")
    private static final class Account {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableColumn(databaseTypeId = "VARCHAR", length = 64,
                nullable = TableColumn.Nullability.NOT_NULL, comment = "账户编码")
        private String code;
    }

    @TableName("parents")
    private static final class Parent {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }

    @TableName("children")
    @TableForeignKey(
            id = "child-parent",
            name = "fk_children_parent",
            localProperties = "parentId",
            targetEntity = Parent.class,
            targetProperties = "id")
    private static final class Child {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long parentId;
    }

    @TableName("cycle_a")
    @TableForeignKey(
            id = "cycle-a-b",
            name = "fk_cycle_a_b",
            localProperties = "bId",
            targetEntity = CycleB.class,
            targetProperties = "id")
    private static final class CycleA {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long bId;
    }

    @TableName("cycle_b")
    @TableForeignKey(
            id = "cycle-b-a",
            name = "fk_cycle_b_a",
            localProperties = "aId",
            targetEntity = CycleA.class,
            targetProperties = "id")
    private static final class CycleB {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long aId;
    }

    private static final class EmptySyncExecutor implements SyncSqlExecutor {

        private final List<SqlRequest> updates = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            updates.add(request);
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class MultiTableReactiveExecutor implements ReactiveSqlExecutor {

        private final Map<String, RelationalTableDefinition> desired;
        private final Map<String, SchemaSnapshot> current = new LinkedHashMap<>();
        private final List<SqlRequest> requests = new ArrayList<>();
        private final List<String> createdTables = new ArrayList<>();

        private MultiTableReactiveExecutor(Map<String, RelationalTableDefinition> desired) {
            this.desired = desired;
            desired.forEach((table, definition) -> current.put(
                    table, SchemaSnapshot.absent(definition.identity())));
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            String sql = request.sql().toLowerCase(Locale.ROOT);
            if (sql.contains("create table")) {
                String target = desired.keySet().stream()
                        .filter(sql::contains)
                        .min(java.util.Comparator.comparingInt(sql::indexOf))
                        .orElseThrow();
                current.put(target, SchemaSnapshot.present(desired.get(target)));
                createdTables.add(target);
            }
            return Mono.just(0L);
        }
    }

    private static final class MultiTableSnapshotReader implements ReactiveFormMetadataReader {

        private final Map<String, SchemaSnapshot> current;

        private MultiTableSnapshotReader(Map<String, SchemaSnapshot> current) {
            this.current = current;
        }

        @Override
        public SchemaSnapshotCoverage snapshotCoverage() {
            return SchemaSnapshotCoverage.complete();
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(String table) {
            return Mono.just(current.get(table));
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
            SchemaSnapshot snapshot = current.get(table);
            assertEquals(schema, snapshot.identity().schema().orElseThrow());
            return Mono.just(snapshot);
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<TableMetadata> readTable(String table) {
            return Mono.error(new UnsupportedOperationException());
        }
    }

    private static final class StatefulReactiveExecutor implements ReactiveSqlExecutor {

        private final AtomicReference<SchemaSnapshot> current;
        private final RelationalTableDefinition desired;
        private final List<SqlRequest> requests = new ArrayList<>();

        private StatefulReactiveExecutor(AtomicReference<SchemaSnapshot> current,
                                         RelationalTableDefinition desired) {
            this.current = current;
            this.desired = desired;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            current.set(SchemaSnapshot.present(desired));
            return Mono.just(0L);
        }
    }

    private static final class SnapshotReader implements ReactiveFormMetadataReader {

        private final AtomicReference<SchemaSnapshot> current;

        private SnapshotReader(AtomicReference<SchemaSnapshot> current) {
            this.current = current;
        }

        @Override
        public SchemaSnapshotCoverage snapshotCoverage() {
            return SchemaSnapshotCoverage.complete();
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(String table) {
            return Mono.just(current.get());
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<TableMetadata> readTable(String table) {
            return Mono.error(new UnsupportedOperationException());
        }
    }
}
