package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaReviewedPlanExecutionDialectTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final RelationalTableDefinition PRESENT = RelationalTableDefinition
            .builder(ACCOUNTS)
            .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
            .build();

    @Test
    void jdbcAndReactiveRejectPlansForAnotherConfiguredDialectBeforeMetadataOrSql() {
        SchemaMigrationPlan plan = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", RdbDialect.postgresql()));
        AtomicInteger jdbcReads = new AtomicInteger();
        AtomicInteger jdbcWrites = new AtomicInteger();
        CountingSyncExecutor jdbcExecutor = new CountingSyncExecutor(jdbcReads, jdbcWrites);
        JdbcSchemaClient jdbc = JdbcSchemaClient.create(jdbcExecutor, RdbDialect.h2());

        assertThrows(IllegalArgumentException.class, () -> jdbc.execute(
                plan,
                JdbcFormMetadataReaders.create(jdbcExecutor, RdbDialect.h2())));
        assertEquals(0, jdbcReads.get());
        assertEquals(0, jdbcWrites.get());

        AtomicInteger reactiveReads = new AtomicInteger();
        AtomicInteger reactiveWrites = new AtomicInteger();
        CountingReactiveExecutor reactiveExecutor =
                new CountingReactiveExecutor(reactiveReads, reactiveWrites);
        ReactiveSchemaClient reactive = ReactiveSchemaClient.create(
                reactiveExecutor, RdbDialect.h2());

        assertThrows(IllegalArgumentException.class, () -> reactive.execute(
                plan,
                ReactiveFormMetadataReaders.create(reactiveExecutor, RdbDialect.h2())).block());
        assertEquals(0, reactiveReads.get());
        assertEquals(0, reactiveWrites.get());
    }

    @Test
    void customSchemaKeepsExplicitSqlAndTheConfiguredDialectCapabilities() {
        SchemaMigrationPlan regular = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", RdbDialect.postgresql()));
        SchemaMigrationPlan mismatchedCapabilities = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", "postgresql", DialectCapabilities.empty()));
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        CountingSyncExecutor executor = new CountingSyncExecutor(reads, writes);

        RdbDialect customDialect = RdbDialect.postgresql().withSchema(SchemaDialect.standard());
        JdbcSchemaClient custom = JdbcSchemaClient.create(executor, customDialect);
        custom.execute(
                regular,
                JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql()));
        assertEquals(0, reads.get());
        assertEquals(1, writes.get());

        CountingReactiveExecutor reactiveExecutor = new CountingReactiveExecutor(reads, writes);
        ReactiveSchemaClient.create(reactiveExecutor, customDialect)
                .execute(regular, ReactiveFormMetadataReaders.create(reactiveExecutor, RdbDialect.postgresql())).block();
        assertEquals(0, reads.get());
        assertEquals(2, writes.get());

        assertThrows(IllegalArgumentException.class, () -> custom.execute(
                mismatchedCapabilities,
                JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql())));
        assertEquals(0, reads.get());
        assertEquals(2, writes.get());
    }

    private static SchemaMigrationPlan absentPlan(DatabaseDescriptor database) {
        SchemaSnapshot actual = SchemaSnapshot.present(PRESENT);
        SchemaOperation drop = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                ACCOUNTS,
                ACCOUNTS.table(),
                PRESENT,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return SchemaMigrationPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .addStep(new SchemaPlanStep(drop, List.of(new SqlRequest("drop table \"accounts\"", List.of()))))
                .build();
    }

    private record CountingSyncExecutor(AtomicInteger reads, AtomicInteger writes)
            implements com.flying.orm.rdb.support.ListQueryFixture {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            reads.incrementAndGet();
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            writes.incrementAndGet();
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("schema DDL must not request generated keys");
        }
    }

    private record CountingReactiveExecutor(AtomicInteger reads, AtomicInteger writes)
            implements ReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            reads.incrementAndGet();
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            writes.incrementAndGet();
            return Mono.just(0L);
        }
    }
}
