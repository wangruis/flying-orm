package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaAbsentClientParityTest {

    @Test
    void jdbcAndReactiveReviewTheSameExplicitAbsentTargetInExactMode() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
        RelationIdentity target = RelationIdentity.of(null, "security", "accounts");
        EmptyMetadataExecutor jdbcExecutor = new EmptyMetadataExecutor();
        ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("review must not execute DDL"));
            }
        };
        JdbcSchemaClient jdbc = JdbcSchemaClient.create(jdbcExecutor, dialect);
        ReactiveSchemaClient reactive = ReactiveSchemaClient.create(reactiveExecutor, dialect);

        ReviewedSchemaPlan jdbcPlan = jdbc.reviewRelationalAbsent(
                database,
                target,
                JdbcFormMetadataReaders.create(jdbcExecutor, dialect));
        ReviewedSchemaPlan reactivePlan = reactive.reviewRelationalAbsent(
                database,
                target,
                ReactiveFormMetadataReaders.create(reactiveExecutor, dialect)).block();

        assertNotNull(reactivePlan);
        assertEquals(SchemaCompatibilityMode.EXACT, jdbcPlan.compatibilityMode());
        assertEquals(ReviewedSchemaPlan.TargetState.ABSENT, jdbcPlan.targetState().orElseThrow());
        assertEquals(target, jdbcPlan.targetIdentity().orElseThrow());
        assertTrue(jdbcPlan.steps().isEmpty());
        assertEquals(jdbcPlan.fingerprint(), reactivePlan.fingerprint());
    }

    private static final class EmptyMetadataExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("review must not execute DDL");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                       SqlExecutionOptions options) {
            throw new AssertionError("review must not request generated keys");
        }
    }
}
