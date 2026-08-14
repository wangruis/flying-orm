package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorPaginationTest {

    @Test
    void rendersCompositeKeysetAndReturnsNextCursorWithoutCountQuery() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql()));
        DynamicForm form = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("createdAt", "BIGINT").withNullable(false))
                .addField(DynamicField.of("tenantId", "VARCHAR"))
                .build();

        CursorPageResult<DynamicRow> result = client.operations().cursorPages.cursorPage(
                form,
                ConditionGroup.and().where("tenantId", "=", "t1").build(),
                CursorPageQuery.after(2, List.of(100L, 10L),
                                      CursorSort.desc("createdAt"), CursorSort.asc("id")))
                .block();

        assertEquals(2, result.rows().size());
        assertTrue(result.hasMore());
        assertEquals(List.of(80L, 12L), result.nextCursor());
        assertEquals(1, executor.requests.size());
        SqlRequest request = executor.requests.getFirst();
        assertTrue(request.sql().contains("(`createdAt` < ? or (`createdAt` = ? and `id` > ?))"), request.sql());
        assertTrue(request.sql().endsWith("order by `createdAt` desc, `id` asc limit ? offset ?"), request.sql());
        assertEquals(List.of("t1", 100L, 100L, 10L, 3, 0L), request.parameters());
    }

    @Test
    void appendsPrimaryKeyAndUsesTheSameNormalizedSortForNextCursor() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql()));
        DynamicForm form = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("createdAt", "BIGINT").withNullable(false))
                .addField(DynamicField.of("tenantId", "VARCHAR"))
                .build();

        CursorPageResult<DynamicRow> first = client.operations().cursorPages.cursorPage(
                form,
                ConditionGroup.and().where("tenantId", "=", "t1").build(),
                CursorPageQuery.first(2, CursorSort.desc("createdAt")))
                .block();
        CursorPageResult<DynamicRow> second = client.operations().cursorPages.cursorPage(
                form,
                ConditionGroup.and().where("tenantId", "=", "t1").build(),
                CursorPageQuery.after(2, first.nextCursor(), CursorSort.desc("createdAt")))
                .block();

        assertEquals(List.of(80L, 12L), first.nextCursor());
        assertEquals(List.of(80L, 12L), second.nextCursor());
        assertTrue(executor.requests.get(0).sql().contains("order by `createdAt` desc, `id` desc"));
        assertTrue(executor.requests.get(1).sql().contains(
                "(`createdAt` < ? or (`createdAt` = ? and `id` < ?))"));
    }

    /**
     * 验证三列游标在响应式查询链中的参数顺序保持与 keyset 谓词完全一致。
     */
    @Test
    void preservesThreeSortCursorParameterOrderInReactivePath() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql()));
        DynamicForm form = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("createdAt", "BIGINT").withNullable(false))
                .addField(DynamicField.of("sequence", "BIGINT").withNullable(false))
                .addField(DynamicField.of("tenantId", "VARCHAR"))
                .build();

        client.operations().cursorPages.cursorPage(
                form,
                ConditionGroup.and().where("tenantId", "=", "t1").build(),
                CursorPageQuery.after(3, List.of(100L, 10L, 5L),
                                      CursorSort.desc("createdAt"),
                                      CursorSort.asc("sequence"),
                                      CursorSort.desc("id")))
                .block();

        SqlRequest request = executor.requests.getFirst();
        assertTrue(request.sql().contains("(`createdAt` < ? or (`createdAt` = ? and `sequence` > ?)"
                                          + " or (`createdAt` = ? and `sequence` = ? and `id` < ?))"));
        assertEquals(List.of("t1", 100L, 100L, 10L, 100L, 10L, 5L, 4, 0L), request.parameters());
    }

    @Test
    void rejectsNullableSortAndFormWithoutPrimaryKeyBeforeExecutor() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql()));
        DynamicForm nullableSort = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("createdAt", "BIGINT"))
                .build();
        DynamicForm noPrimaryKey = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.of("createdAt", "BIGINT").withNullable(false))
                .build();

        assertThrows(IllegalArgumentException.class, () -> client.operations().cursorPages.cursorPage(
                nullableSort, ConditionGroup.and().build(),
                CursorPageQuery.first(2, CursorSort.asc("createdAt"))).block());
        assertThrows(IllegalArgumentException.class, () -> client.operations().cursorPages.cursorPage(
                noPrimaryKey, ConditionGroup.and().build(),
                CursorPageQuery.first(2, CursorSort.asc("createdAt"))).block());
        assertTrue(executor.requests.isEmpty());
    }

    @Test
    void missingSortFieldFailureDoesNotExposeLookupName() {
        RecordingExecutor executor = new RecordingExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(
                executor,
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql()));
        DynamicForm form = DynamicForm.builder("events", "event_log")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        String secret = "tenant-secret-sort-field";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> client.operations().cursorPages.cursorPage(
                        form,
                        ConditionGroup.and().build(),
                        CursorPageQuery.first(2, CursorSort.asc(secret))).block());

        assertFalse(error.getMessage().contains(secret));
        assertTrue(executor.requests.isEmpty());
    }

    private static final class RecordingExecutor implements ReactiveSqlExecutor {
        private final List<SqlRequest> requests = new ArrayList<>();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            return Flux.just(DynamicRow.copyOf(Map.of("id", 11L, "createdAt", 90L, "sequence", 3L,
                                                     "tenantId", "t1")),
                             DynamicRow.copyOf(Map.of("id", 12L, "createdAt", 80L, "sequence", 2L,
                                                     "tenantId", "t1")),
                             DynamicRow.copyOf(Map.of("id", 13L, "createdAt", 70L, "sequence", 1L,
                                                     "tenantId", "t1")));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(0L);
        }
    }
}
