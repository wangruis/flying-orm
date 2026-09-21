package com.flying.orm.rdb.metadata;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlServerMetadataRelationIdentityTest {

    @Test
    void jdbcKeepsUnqualifiedNamesAsOneObjectIdSegment() {
        for (String table : List.of("events", "log.events")) {
            List<SqlRequest> requests = new ArrayList<>();
            JdbcFormMetadataReaders.create(sync(requests), RdbDialect.sqlServer())
                    .readSnapshot(RelationIdentity.table(table));
            assertSingleSegment(requests, table);
        }
    }

    @Test
    void reactiveKeepsUnqualifiedNamesAsOneObjectIdSegment() {
        for (String table : List.of("events", "log.events")) {
            List<SqlRequest> requests = new ArrayList<>();
            ReactiveFormMetadataReaders.create(reactive(requests), RdbDialect.sqlServer())
                    .readSnapshot(RelationIdentity.table(table)).block(Duration.ofSeconds(5));
            assertSingleSegment(requests, table);
        }
    }

    @Test
    void explicitSchemaStillUsesLiteralNameEqualityOnBothBackends() {
        RelationIdentity identity = RelationIdentity.of(null, "audit", "log.events");
        List<SqlRequest> jdbc = new ArrayList<>();
        List<SqlRequest> r2dbc = new ArrayList<>();
        JdbcFormMetadataReaders.create(sync(jdbc), RdbDialect.sqlServer()).readSnapshot(identity);
        ReactiveFormMetadataReaders.create(reactive(r2dbc), RdbDialect.sqlServer())
                .readSnapshot(identity).block(Duration.ofSeconds(5));
        for (List<SqlRequest> requests : List.of(jdbc, r2dbc)) {
            assertEquals(7, requests.size());
            for (SqlRequest request : requests) {
                assertEquals(List.of("log.events", "audit"), request.parameters());
                assertFalse(request.sql().contains("object_id("), request.sql());
            }
        }
    }

    private static void assertSingleSegment(List<SqlRequest> requests, String table) {
        assertEquals(7, requests.size());
        for (SqlRequest request : requests) {
            assertEquals(List.of(table, table), request.parameters());
            assertTrue(request.sql().contains("object_id(quotename(?))"), request.sql());
        }
    }

    private static SyncSqlExecutor sync(List<SqlRequest> requests) {
        return new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                requests.add(request);
                return List.of();
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("metadata reading must not execute writes");
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("metadata reading must not execute writes");
            }
        };
    }

    private static ReactiveSqlExecutor reactive(List<SqlRequest> requests) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                requests.add(request);
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("metadata reading must not execute writes"));
            }
        };
    }
}
