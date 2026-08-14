package com.flying.orm.rdb.codec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 用真实 H2 R2DBC 驱动验证带偏移时间，元数据和数据读写必须认同同一个逻辑类型。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class H2R2dbcOffsetTimeIntegrationTest {

    @Test
    void restoresAndRoundTripsOffsetTimeAcrossSingleAndBatchWrites() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("offset_time_roundtrip")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        ReactiveFormClient client = ReactiveFormClient.create(
                executor, FormDataSqlRenderer.create(conditionRenderer(), RdbDialect.h2()));
        ReactiveFormMetadataReader metadataReader = ReactiveFormMetadataReaders.create(executor, RdbDialect.h2());
        DynamicForm form = DynamicForm.builder("schedules", "Schedules")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("meeting_time", "OFFSET_TIME"))
                                      .build();
        OffsetTime morning = OffsetTime.parse("09:15+08:00");
        OffsetTime afternoon = OffsetTime.parse("14:30+02:00");

        Mono<ScenarioResult> scenario = executor.rowsUpdated(SqlRequest.nativeSql(
                        "create table Schedules (id bigint primary key, meeting_time time with time zone)",
                        List.of()))
                .then(metadataReader.readForm("schedules", "PUBLIC", "SCHEDULES"))
                .flatMap(metadata -> client.insert(WriteSpec.insert(form, row(1L, morning)))
                                           .then(client.writeBatch(BatchSpec.upsert(
                                                   form,
                                                   reactor.core.publisher.Flux.fromIterable(
                                                           List.of(row(1L, morning), row(2L, afternoon))))))
                                           .thenMany(client.select(QuerySpec.of(form, ConditionGroup.and().build())))
                                           .sort(Comparator.comparingLong(value -> ((Number) field(value, "id")).longValue()))
                                           .collectList()
                                           .map(rows -> new ScenarioResult(metadata, rows)));

        StepVerifier.create(scenario)
                    .assertNext(result -> {
                        assertEquals("OFFSET_TIME", result.metadata().field("MEETING_TIME").dataType());
                        assertEquals(morning, field(result.rows().get(0), "meeting_time"));
                        assertEquals(afternoon, field(result.rows().get(1), "meeting_time"));
                    })
                    .verifyComplete();
    }

    private static SqlRenderer conditionRenderer() {
        return SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
    }

    private static Map<String, Object> row(long id, OffsetTime meetingTime) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("meeting_time", meetingTime);
        return values;
    }

    private static Object field(Map<String, Object> row, String name) {
        return row.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                  .map(Map.Entry::getValue)
                  .findFirst()
                  .orElse(null);
    }

    private record ScenarioResult(DynamicForm metadata, List<DynamicRow> rows) {
    }
}
