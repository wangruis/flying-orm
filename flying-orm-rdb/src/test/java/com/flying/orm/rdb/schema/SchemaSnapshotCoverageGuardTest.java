package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaSnapshotCoverageGuardTest {

    @Test
    void everyBuiltInReaderDeclaresCompleteCoverage() {
        EmptySyncExecutor jdbcExecutor = new EmptySyncExecutor();
        EmptyReactiveExecutor reactiveExecutor = new EmptyReactiveExecutor();

        for (RdbDialect dialect : builtInDialects()) {
            SchemaSnapshotCoverage jdbc = JdbcFormMetadataReaders.create(
                    jdbcExecutor, dialect).snapshotCoverage();
            SchemaSnapshotCoverage reactive = ReactiveFormMetadataReaders.create(
                    reactiveExecutor, dialect).snapshotCoverage();

            assertEquals(jdbc, reactive, dialect.name());
            assertTrue(jdbc.observes(SchemaSnapshotCoverage.Fact.TABLE_EXISTENCE), dialect.name());
            assertTrue(jdbc.observes(SchemaSnapshotCoverage.Fact.TABLE_PARTITION), dialect.name());
            assertTrue(jdbc.observes(SchemaSnapshotCoverage.Fact.COLUMNS), dialect.name());
            assertTrue(jdbc.observes(SchemaSnapshotCoverage.Fact.CHECK_CONSTRAINTS), dialect.name());
            assertTrue(jdbc.observes(SchemaSnapshotCoverage.Fact.COLUMN_DEFAULT), dialect.name());
            assertTrue(jdbc.isComplete(), dialect.name());
        }
    }

    @Test
    void incompleteCoverageStopsRelationalReviewBeforeAnyDdlRequest() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = desired();

        assertThrows(IllegalStateException.class, () -> RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.none(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL));
    }

    @Test
    void explicitCompleteReactiveReaderKeepsTheExecutableClosure() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = desired();
        EmptyReactiveExecutor executor = new EmptyReactiveExecutor();
        CompleteReader reader = new CompleteReader(SchemaSnapshot.absent(desired.identity()));

        SchemaMigrationPlan plan = ReactiveSchemaClient.create(executor, dialect)
                .reviewRelational(database, desired, reader, SchemaCompatibilityMode.SAFE_INCREMENTAL)
                .block();

        assertFalse(plan.requests().isEmpty());
        assertEquals(SchemaSnapshotCoverage.complete(),
                     plan.snapshotCoverage());
    }

    private static List<RdbDialect> builtInDialects() {
        return List.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                       RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static RelationalTableDefinition desired() {
        return EntitySchemaDescriptor.builder(Account.class).build().table();
    }

    @TableName("coverage_accounts")
    private static final class Account {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;
    }

    private static final class EmptySyncExecutor implements com.flying.orm.rdb.support.ListQueryFixture {

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
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class EmptyReactiveExecutor implements ReactiveSqlExecutor {

        private final List<SqlRequest> updates = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            updates.add(request);
            return Mono.just(0L);
        }
    }

    private static class CompleteReader implements ReactiveFormMetadataReader {

        private final SchemaSnapshot snapshot;

        private CompleteReader(SchemaSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public SchemaSnapshotCoverage snapshotCoverage() {
            return SchemaSnapshotCoverage.complete();
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
            return Mono.just(snapshot);
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
            return Mono.error(new UnsupportedOperationException());
        }
    }

    private static final class CountingPartialReader extends CompleteReader {

        private final AtomicInteger snapshotReads = new AtomicInteger();

        private CountingPartialReader(SchemaSnapshot snapshot) {
            super(snapshot);
        }

        @Override
        public SchemaSnapshotCoverage snapshotCoverage() {
            return SchemaSnapshotCoverage.none();
        }

        @Override
        public Mono<SchemaSnapshot> readSnapshot(String table) {
            snapshotReads.incrementAndGet();
            return super.readSnapshot(table);
        }
    }
}
