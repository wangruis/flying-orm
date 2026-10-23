package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GeneratedKeyContractTest {
    @Test
    void unspecifiedAndNamedColumnsUseOneReactiveImplementation() {
        List<String> columns = new ArrayList<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.just(1L); }
            public Mono<SqlWriteResult> rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options, String column) {
                return Mono.fromSupplier(() -> {
                    columns.add(column);
                    return new SqlWriteResult(1, List.of());
                });
            }
        };
        SqlRequest request = new SqlRequest("insert into t default values", List.of());
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();
        executor.rowsUpdatedReturningKeys(request, options).block();
        executor.rowsUpdatedReturningKeys(request, options, "id").block();
        assertEquals(java.util.Arrays.asList(null, "id"), columns);
    }

    @Test
    void syncConvenienceDelegatesToTheColumnAwareContract() {
        List<String> columns = new ArrayList<>();
        SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return List.of(); }
            public long rowsUpdated(SqlRequest request) { return 1; }
            public SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options, String column) {
                columns.add(column);
                return new SqlWriteResult(1, List.of());
            }
        };
        SqlRequest request = new SqlRequest("insert into t default values", List.of());
        executor.rowsUpdatedReturningKeys(request, SqlExecutionOptions.unlimited());
        executor.rowsUpdatedReturningKeys(request, SqlExecutionOptions.unlimited(), "id");
        assertEquals(java.util.Arrays.asList(null, "id"), columns);
    }
}
