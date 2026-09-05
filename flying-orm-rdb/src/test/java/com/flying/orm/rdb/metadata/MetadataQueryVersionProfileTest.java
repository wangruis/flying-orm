package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.SqlServerVersion;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataQueryVersionProfileTest {

    @Test
    void mysqlJdbcAndReactiveDictionaryQueriesAdmitOnlyControlledTemporalAliases() {
        CapturingSyncExecutor jdbcExecutor = new CapturingSyncExecutor();
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        RdbDialect dialect = RdbDialect.mysql();

        JdbcFormMetadataReaders.create(jdbcExecutor, dialect).readSnapshot("app", "orders");
        ReactiveFormMetadataReaders.create(reactiveExecutor, dialect).readSnapshot("app", "orders").block();

        assertEquals(jdbcExecutor.sql, reactiveExecutor.sql);
        List<Pattern> expressions = Pattern.compile("regexp '([^']+)'")
                .matcher(jdbcExecutor.sql.getFirst()).results()
                .map(match -> Pattern.compile(match.group(1)))
                .toList();
        for (String expression : List.of("current_date", "current_time(6)", "current_timestamp(6)",
                "curdate()", "curtime()", "curtime(6)")) {
            assertTrue(expressions.stream().anyMatch(pattern -> pattern.matcher(expression).matches()), expression);
        }
        for (String expression : List.of("curdate() + interval 1 day", "curtime(6) + 1", "rand()")) {
            assertFalse(expressions.stream().anyMatch(pattern -> pattern.matcher(expression).matches()), expression);
        }
    }

    @Test
    void sqlServer2012Through2019UseOneJdbcReactiveCompatibleQueryProfile() {
        for (SqlServerVersion version : List.of(
                SqlServerVersion.V2012, SqlServerVersion.V2016, SqlServerVersion.V2019)) {
            CapturingSyncExecutor jdbcExecutor = new CapturingSyncExecutor();
            CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
            RdbDialect dialect = RdbDialect.sqlServer(version);

            JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.create(jdbcExecutor, dialect);
            ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.create(reactiveExecutor, dialect);
            jdbc.readSnapshot("app", "orders");
            reactive.readSnapshot("app", "orders").block();

            assertEquals(jdbcExecutor.sql, reactiveExecutor.sql);
            assertTrue(jdbc.snapshotCoverage().isComplete());
            assertEquals(jdbc.snapshotCoverage(), reactive.snapshotCoverage());
            String sql = String.join("\n", jdbcExecutor.sql).toLowerCase(java.util.Locale.ROOT);
            assertFalse(sql.contains("translate("));
            assertTrue(sql.contains("cc.definition as check_expression"));
            assertFalse(sql.contains("replace(replace(cc.definition"));
        }
    }

    @Test
    void oracle12cUsesThe12cCommonDictionarySurfaceAndFailsClosedOnUnobservableFacts() {
        CapturingSyncExecutor jdbcExecutor = new CapturingSyncExecutor();
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        RdbDialect dialect = RdbDialect.oracle(OracleVersion.V12C);

        JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.create(jdbcExecutor, dialect);
        ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.create(reactiveExecutor, dialect);
        jdbc.readSnapshot("app", "orders");
        reactive.readSnapshot("app", "orders").block();

        assertEquals(jdbcExecutor.sql, reactiveExecutor.sql);
        assertEquals(jdbc.snapshotCoverage(), reactive.snapshotCoverage());
        assertFalse(jdbc.snapshotCoverage().isComplete());
        assertFalse(jdbc.snapshotCoverage().observes(SchemaSnapshotCoverage.Fact.COLUMN_DEFAULT));
        assertFalse(jdbc.snapshotCoverage().observes(SchemaSnapshotCoverage.Fact.COLUMN_GENERATION));
        assertFalse(jdbc.snapshotCoverage().observes(SchemaSnapshotCoverage.Fact.COLUMN_COLLATION));
        assertTrue(jdbc.snapshotCoverage().observes(SchemaSnapshotCoverage.Fact.CHECK_CONSTRAINTS));
        String sql = String.join("\n", jdbcExecutor.sql).toUpperCase(java.util.Locale.ROOT);
        assertFalse(sql.contains("DEFAULT_ON_NULL"));
        assertTrue(sql.contains("SEARCH_CONDITION_VC"));
        assertFalse(sql.contains("SCALE_FLAG"));
        assertFalse(sql.contains("EXTEND_FLAG"));
        assertFalse(sql.contains("SHARDED_FLAG"));
        assertFalse(sql.contains("SESSION_FLAG"));
        assertFalse(sql.contains("KEEP_VALUE"));
        assertFalse(sql.contains("C.COLLATION"));
    }

    @Test
    void oracle19cKeepsTheCompleteMetadataContract() {
        CapturingSyncExecutor jdbcExecutor = new CapturingSyncExecutor();
        CapturingReactiveExecutor reactiveExecutor = new CapturingReactiveExecutor();
        RdbDialect dialect = RdbDialect.oracle(OracleVersion.V19C);

        JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.create(jdbcExecutor, dialect);
        ReactiveFormMetadataReader reactive = ReactiveFormMetadataReaders.create(reactiveExecutor, dialect);
        jdbc.readSnapshot("app", "orders");
        reactive.readSnapshot("app", "orders").block();

        assertEquals(jdbcExecutor.sql, reactiveExecutor.sql);
        assertTrue(jdbc.snapshotCoverage().isComplete());
        assertEquals(jdbc.snapshotCoverage(), reactive.snapshotCoverage());
        String sql = String.join("\n", jdbcExecutor.sql).toUpperCase(java.util.Locale.ROOT);
        assertTrue(sql.contains("KEEP_VALUE"));
        assertTrue(sql.contains("SEARCH_CONDITION_VC"));
        assertTrue(sql.contains("C.COLLATION"));
    }

    private static final class CapturingSyncExecutor implements SyncSqlExecutor {

        private final List<String> sql = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            sql.add(request.sql());
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CapturingReactiveExecutor implements ReactiveSqlExecutor {

        private final List<String> sql = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            sql.add(request.sql());
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.error(new UnsupportedOperationException());
        }
    }
}
