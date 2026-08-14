package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 用真实 H2 R2DBC 驱动验证 JSON，不只检查生成出来的 SQL 字符串。
 * 这里能及时发现驱动绑定、数据库 JSON 类型和读取返回类型之间的不一致。
 */
class H2R2dbcJsonIntegrationTest {

    @Test
    void roundTripsJsonAcrossSingleInsertAndBatchUpsert() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("form_json_roundtrip")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2()));
        DynamicForm form = jsonForm();

        Map<String, Object> alice = row("name", "Alice", "roles", List.of("admin"));
        Map<String, Object> aliceUpdated = row("name", "Alice", "roles", List.of("admin", "auditor"));
        Map<String, Object> bob = row("name", "Bob", "roles", List.of("user"));

        Mono<List<DynamicRow>> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                        "create table Profiles (id bigint primary key, profile json)",
                        List.of()))
                .then(client.insert(WriteSpec.insert(form, row("id", 1L, "profile", alice))))
                .doOnNext(updated -> assertEquals(1L, updated))
                .then(client.writeBatch(BatchSpec.upsert(
                        form,
                        reactor.core.publisher.Flux.fromIterable(
                                List.of(row("id", 1L, "profile", aliceUpdated),
                                        row("id", 2L, "profile", bob))))))
                .doOnNext(result -> {
                    assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                    assertEquals(2L, result.inputCount());
                    assertEquals(2L, result.affectedRows());
                })
                .thenMany(client.select(QuerySpec.of(form, ConditionGroup.and().build())))
                .sort(Comparator.comparingLong(value -> ((Number) value(value, "id")).longValue()))
                .collectList();

        StepVerifier.create(scenario)
                    .assertNext(rows -> {
                        assertEquals(2, rows.size());
                        assertEquals(aliceUpdated, value(rows.get(0), "profile"));
                        assertEquals(bob, value(rows.get(1), "profile"));
                    })
                    .verifyComplete();
    }

    private static DynamicForm jsonForm() {
        return DynamicForm.builder("profiles", "Profiles")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("profile", "JSON"))
                          .build();
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.length; index += 2) {
            values.put((String) pairs[index], pairs[index + 1]);
        }
        return values;
    }

    private static Object value(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElse(null);
    }
}
