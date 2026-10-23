package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.support.ListQueryFixture;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GovernedDmlResultVisibilityTest {
    @Test
    void syncRowConsumersSeeOnlyGovernedResultsForDynamicMapperAndDtoTerminals() {
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        com.flying.orm.rdb.dialect.RdbDialect dialect = com.flying.orm.rdb.dialect.RdbDialect.postgresql();
        SyncSqlExecutor executor = syncExecutor(rawRow());
        SyncFormClient forms = SyncFormClient.create(executor, syncBatchExecutor(), FormDataSqlRenderer.create(sql, dialect));
        SyncQueryOperator query = new SyncDmlOperator(forms, executor, sql, DataScope.none(), dialect)
                .query().select("id", "secret", "internal_note").from(form(), policy());
        assertEquals(1L, query.forEach(GovernedDmlResultVisibilityTest::assertPublished));
        assertEquals(1L, query.forEach(row -> { assertPublished(row); return row.get("id"); },
                id -> assertEquals(7L, id)));
        assertEquals(1L, query.forEach(Visible.class,
                row -> assertEquals(new Visible(7L, "**********"), row)));
        SyncQueryOperator raw = new SyncDmlOperator(forms, executor, sql, DataScope.none(), dialect)
                .query().select("id", "secret").from("people");
        assertEquals(1L, raw.forEach(Visible.class,
                row -> assertEquals(new Visible(7L, "classified"), row)));
    }

    record Visible(Long id, String secret) { }

    @Test
    void syncAndReactiveGovernedDmlPublishMaskedAndDropHiddenFields() {
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer forms = FormDataSqlRenderer.create(
                sql, com.flying.orm.rdb.dialect.RdbDialect.postgresql());
        DynamicRow raw = rawRow();
        SyncSqlExecutor syncExecutor = syncExecutor(raw);
        ReactiveSqlExecutor reactiveExecutor = reactiveExecutor(raw);
        SyncFormClient syncForms = SyncFormClient.create(syncExecutor, syncBatchExecutor(), forms);
        ReactiveFormClient reactiveForms = ReactiveFormClient.create(reactiveExecutor, forms);

        DynamicRow sync = new SyncDmlOperator(syncForms, syncExecutor, sql, DataScope.none(), com.flying.orm.rdb.dialect.RdbDialect.postgresql())
                .query().select("id", "secret", "internal_note").from(form(), policy()).fetch().getFirst();
        DynamicRow reactive = new DmlOperator(reactiveForms, reactiveExecutor, sql, DataScope.none(), com.flying.orm.rdb.dialect.RdbDialect.postgresql())
                .query().select("id", "secret", "internal_note").from(form(), policy())
                .fetch().single().block();

        assertPublished(sync);
        assertPublished(reactive);
    }

    private static void assertPublished(DynamicRow row) {
        assertEquals(7L, row.get("id"));
        assertEquals("**********", row.get("secret"));
        assertFalse(row.containsKey("internal_note"));
    }

    private static DynamicForm form() {
        return DynamicForm.builder("people", "people")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .addField(DynamicField.of("internal_note", "VARCHAR"))
                          .masked("secret", MaskedFieldDefinition.builder("full").build())
                          .build();
    }

    private static FieldUsePolicy policy() {
        return FieldUsePolicy.builder()
                             .visibility("id", FieldVisibility.FULL)
                             .visibility("secret", FieldVisibility.MASKED)
                             .allow("internal_note", FieldUse.PROJECT)
                             .build();
    }

    private static DynamicRow rawRow() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 7L);
        values.put("secret", "classified");
        values.put("internal_note", "never-return-this");
        return DynamicRow.copyOf(values);
    }

    private static SyncSqlExecutor syncExecutor(DynamicRow raw) {
        return new ListQueryFixture() {
            @Override public List<DynamicRow> query(SqlRequest request) {
                return List.of(raw);
            }
            @Override public long rowsUpdated(SqlRequest request) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ReactiveSqlExecutor reactiveExecutor(DynamicRow raw) {
        return new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.just(raw);
            }
            @Override public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> { throw new UnsupportedOperationException(method.toString()); });
    }
}
