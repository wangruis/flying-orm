package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EntityEnumEntryClosureTest {
    @TestFactory
    List<DynamicTest> declaredStorageReachesBothExecutionBoundaries() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean reactive : List.of(false, true)) {
                for (String operation : List.of("SET", "=", "in", "between", "is-null")) {
                    tests.add(DynamicTest.dynamicTest(dialect.name() + "/" + reactive + "/" + operation,
                            () -> verify(dialect, reactive, operation)));
                }
            }
        }
        return tests;
    }

    private static void verify(RdbDialect dialect, boolean reactive, String operation) {
        Capture capture = new Capture();
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, dialect);
        ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
        SyncFormClient sync = SyncFormClient.create(capture, (request, callback) -> {
            throw new AssertionError("unexpected batch");
        }, renderer);
        if (operation.equals("SET")) {
            if (reactive) async.entity(Account.class).update().set(Account::status, Status.ACTIVE)
                    .where(Account::id, 1L).execute().block();
            else sync.entity(Account.class).update().set(Account::status, Status.ACTIVE)
                    .where(Account::id, 1L).execute();
            assertEquals(List.of("A", 1L), capture.request.parameters());
        } else {
            Object value = operation.equals("in") || operation.equals("between")
                    ? new Status[]{Status.ACTIVE, Status.DISABLED}
                    : operation.equals("is-null") ? null : Status.ACTIVE;
            if (reactive) new EntityJoinQueryOperator<>(async, sql, Account.class)
                    .join(Team.class, Account::id, Team::id).select(Account.class, Account::id)
                    .where(Account.class, Account::status, operation, value).executeRows().collectList().block();
            else new SyncEntityJoinQueryOperator<>(sync, sql, Account.class)
                    .join(Team.class, Account::id, Team::id).select(Account.class, Account::id)
                    .where(Account.class, Account::status, operation, value).executeRows();
            assertEquals(operation.equals("is-null") ? List.of() : operation.equals("=")
                    ? List.of("A") : List.of("A", "D"), capture.request.parameters());
        }
    }

    @Test
    void customTermsKeepTheirDeclaredDomainValue() {
        var values = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()).entityValues(Account.class);
        TermRegistry terms = TermRegistry.builder()
                .add(TermHandler.simple("custom-status", ConditionValueShape.SCALAR)).build();
        assertSame(Status.ACTIVE, values.normalizeConditionValue("status", "custom-status", Status.ACTIVE, terms));
        assertNull(values.normalizeWriteValue("status", null));
        assertEquals("A", values.normalizeWriteValue("status", "A"));
    }

    enum Status {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }
    @TableName("enum_accounts")
    record Account(@TableId(type = IdType.INPUT) Long id, Status status) { }
    @TableName("enum_teams")
    record Team(@TableId(type = IdType.INPUT) Long id) { }

    private static final class Capture implements SyncSqlExecutor {
        private SqlRequest request;
        public List<DynamicRow> query(SqlRequest value) { request = value; return List.of(); }
        public long rowsUpdated(SqlRequest value) { request = value; return 1; }
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest value, SqlExecutionOptions options) {
            throw new AssertionError("unexpected generated keys");
        }
        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest value) {
                    return Flux.defer(() -> { request = value; return Flux.empty(); });
                }
                public Mono<Long> rowsUpdated(SqlRequest value) {
                    return Mono.fromSupplier(() -> { request = value; return 1L; });
                }
            };
        }
    }
}
