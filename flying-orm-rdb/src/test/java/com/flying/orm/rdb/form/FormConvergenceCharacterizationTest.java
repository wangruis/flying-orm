package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FormConvergenceCharacterizationTest {

    @Test
    void bothClientsDecodeArrayAndCollectionCarriersIntoNullCapableImmutableLists() {
        ValueCodec custom = new ValueCodec() {
            public boolean supports(Class<?> type) { return type == Integer.class; }
            public Object read(Object value, Class<?> type) { return Integer.parseInt(value.toString()) + 100; }
        };
        FormDataSqlRenderer renderer = renderer(ValueCodecRegistry.standard().withFirst(custom));
        DynamicForm form = DynamicForm.builder("arrays", "arrays")
                .addField(DynamicField.of("dates", "DATE[]"))
                .addField(DynamicField.of("times", "TIME[]"))
                .addField(DynamicField.of("timestamps", "TIMESTAMP[]"))
                .addField(DynamicField.of("numbers", "INTEGER[]"))
                .addField(DynamicField.of("raw", "CUSTOM[]"))
                .build();
        for (boolean collection : List.of(false, true)) {
            for (boolean reactive : List.of(false, true)) {
                LocalDate date = LocalDate.of(2026, 1, 2);
                LocalTime time = LocalTime.of(3, 4, 5);
                LocalDateTime timestamp = LocalDateTime.of(date, time);
                Map<String, Object> source = new LinkedHashMap<>();
                source.put("dates", carrier(collection, java.sql.Date.valueOf(date), null));
                source.put("times", carrier(collection, java.sql.Time.valueOf(time), null));
                source.put("timestamps", carrier(collection, java.sql.Timestamp.valueOf(timestamp), null));
                source.put("numbers", carrier(collection, "1", null));
                source.put("raw", carrier(collection, "opaque", null));
                Fixture fixture = new Fixture(renderer, List.of(DynamicRow.copyOf(source)));
                DynamicRow result = fixture.select(reactive, QuerySpec.of(form, ConditionGroup.and().build()), null)
                        .getFirst();
                assertEquals(Arrays.asList(date, null), result.get("dates"));
                assertEquals(Arrays.asList(time, null), result.get("times"));
                assertEquals(Arrays.asList(timestamp, null), result.get("timestamps"));
                assertEquals(Arrays.asList(101, null), result.get("numbers"));
                assertEquals(Arrays.asList("opaque", null), result.get("raw"));
                for (Object value : result.values()) {
                    assertThrows(UnsupportedOperationException.class, () -> ((List<?>) value).clear());
                }
                if (collection) ((List<?>) source.get("numbers")).clear();
                else ((Object[]) source.get("numbers"))[0] = "changed";
                assertEquals(Arrays.asList(101, null), result.get("numbers"));
                assertEquals(1, fixture.queries);
            }
        }
    }

    @Test
    void bothClientsPreserveNestedArrayRejectionAndTopLevelNull() {
        DynamicForm form = DynamicForm.builder("arrays", "arrays")
                .addField(DynamicField.of("value", "INTEGER[]")).build();
        for (boolean reactive : List.of(false, true)) {
            for (Object nested : List.of(new Object[]{new Object[]{1}}, List.of(List.of(1)))) {
                Fixture fixture = new Fixture(renderer(ValueCodecRegistry.standard()),
                        List.of(DynamicRow.copyOf(Map.of("value", nested))));
                IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> fixture.select(reactive, QuerySpec.of(form, ConditionGroup.and().build()), null));
                assertEquals("nested SQL arrays are not supported yet", failure.getMessage());
            }
            Fixture fixture = new Fixture(renderer(ValueCodecRegistry.standard()),
                    List.of(DynamicRow.copyOf(Collections.singletonMap("value", null))));
            assertNull(fixture.select(reactive, QuerySpec.of(form, ConditionGroup.and().build()), null)
                    .getFirst().get("value"));
        }
    }

    @Test
    void generatedColumnHintKeepsZeroOneAndMultipleCandidateSemantics() {
        DynamicField input = DynamicField.primaryKey("tenant_id", "BIGINT");
        DynamicField first = DynamicField.primaryKey("generated_id", "BIGINT")
                .withGeneration(ValueGeneration.identity());
        DynamicField second = DynamicField.primaryKey("other_id", "BIGINT")
                .withGeneration(ValueGeneration.sequence("other_seq"));
        List<List<DynamicField>> fields = List.of(List.of(input), List.of(first),
                List.of(input, first), List.of(first, second));
        List<Optional<String>> expected = List.of(Optional.empty(), Optional.of("generated_id"),
                Optional.of("generated_id"), Optional.empty());
        for (int index = 0; index < fields.size(); index++) {
            DynamicForm.Builder builder = DynamicForm.builder("keys", "keys");
            fields.get(index).forEach(builder::addField);
            builder.addField(DynamicField.of("label", "VARCHAR"));
            DynamicForm form = builder.build();
            FormOperationPlanner.PlannedWrite plan = new FormOperationPlanner.PlannedWrite(form,
                    new SqlRequest("insert into keys(label) values (?)", List.of("row")),
                    SqlExecutionOptions.safeDefaults(), null, null, DataScope.none());
            assertEquals(expected.get(index), plan.generatedKeyColumn());
            for (boolean reactive : List.of(false, true)) {
                Fixture fixture = new Fixture(renderer(ValueCodecRegistry.standard()), List.of());
                Map<String, Object> values = new LinkedHashMap<>();
                values.put("label", "row");
                if (fields.get(index).contains(input)) values.put("tenant_id", 7L);
                fixture.insertReturningKeys(reactive, WriteSpec.insert(form, values));
                assertEquals(expected.get(index).orElse(null), fixture.generatedColumn);
                assertEquals(1, fixture.writes);
            }
        }
    }

    @Test
    void containsVerificationPrecedesMaskingAndFinalGovernedVisibilityOnBothClients() {
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            DynamicForm form = protectedForm();
            FormDataSqlRenderer renderer = renderer(ValueCodecRegistry.standard()).withProtectedFields(runtime);
            List<DynamicRow> candidates = List.of(encryptedRow(runtime, form, 1L, "alphabet"),
                    encryptedRow(runtime, form, 2L, "goodbye"),
                    encryptedRow(runtime, form, 3L, "alphabet soup"));
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build()).masked();
            FieldUsePolicy policy = FieldUsePolicy.builder()
                    .allow("secret", FieldUse.FILTER)
                    .visibility("id", FieldVisibility.FULL)
                    .visibility("secret", FieldVisibility.MASKED)
                    .visibility("note", FieldVisibility.HIDDEN).build();
            for (boolean reactive : List.of(false, true)) {
                for (boolean governed : List.of(false, true)) {
                    Fixture fixture = new Fixture(renderer, candidates);
                    List<DynamicRow> result = fixture.select(reactive, spec, governed ? policy : null);
                    assertEquals(List.of(1L, 3L), result.stream().map(row -> row.get("id")).toList());
                    assertEquals(List.of("********", "*************"),
                            result.stream().map(row -> row.get("secret")).toList());
                    assertEquals(!governed, result.getFirst().containsKey("note"));
                    assertEquals(1, fixture.queries);
                }
            }
        }
    }

    @Test
    void containsAcceptsMoreThanOneThousandCandidatesOnBothClients() {
        DynamicForm form = protectedForm();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = renderer(ValueCodecRegistry.standard()).withProtectedFields(runtime);
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());
            List<DynamicRow> candidates = Collections.nCopies(1002,
                    encryptedRow(runtime, form, 1L, "alphabet"));
            for (boolean reactive : List.of(false, true)) {
                for (boolean governed : List.of(false, true)) {
                    Fixture fixture = new Fixture(renderer, candidates);
                    FieldUsePolicy policy = governed ? FieldUsePolicy.builder()
                            .allow("secret", FieldUse.FILTER).visibility("id", FieldVisibility.FULL)
                            .visibility("secret", FieldVisibility.FULL).visibility("note", FieldVisibility.HIDDEN)
                            .build() : null;
                    assertEquals(1002, fixture.select(reactive, spec, policy).size());
                    assertFalse(fixture.lastRequest.sql().contains(" limit "), fixture.lastRequest.sql());
                    assertEquals(1, fixture.queries);
                }
            }
        }
    }

    @Test
    void containsPaginationKeepsLargeEmptyPageIndexesWithinTheResult() {
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            DynamicForm form = protectedForm();
            FormDataSqlRenderer renderer = renderer(ValueCodecRegistry.standard()).withProtectedFields(runtime);
            Fixture fixture = new Fixture(renderer, List.of(encryptedRow(runtime, form, 1L, "alphabet")));
            QuerySpec query = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());
            PageQuery page = PageQuery.of(2, Integer.MAX_VALUE);
            SyncFormClient sync = fixture.syncClient();
            ReactiveFormClient reactive = ReactiveFormClient.create(fixture.reactive, renderer);
            try {
                PageResult<DynamicRow> expected = PageResult.of(List.of(), 1, page);
                assertAll(
                        () -> assertEquals(expected, sync.page(query, page)),
                        () -> assertEquals(expected, reactive.page(query, page).block()));
            } finally {
                sync.entityModels().close();
                reactive.entityModels().close();
            }
        }
    }

    @Test
    void containsPagesVerifyAllCandidatesBeforeComputingTheResult() {
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            DynamicForm form = protectedForm();
            FormDataSqlRenderer renderer = renderer(ValueCodecRegistry.standard()).withProtectedFields(runtime);
            Fixture fixture = new Fixture(renderer,
                    Collections.nCopies(1002, encryptedRow(runtime, form, 1L, "alphabet")));
            QuerySpec query = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());
            SyncFormClient sync = fixture.syncClient();
            ReactiveFormClient reactive = ReactiveFormClient.create(fixture.reactive, renderer);
            PageQuery page = PageQuery.of(2, 1000);
            CursorPageQuery cursor = CursorPageQuery.first(1001, CursorSort.asc("id"));
            try {
                for (PageResult<DynamicRow> result : List.of(sync.page(query, page), reactive.page(query, page).block())) {
                    assertEquals(1002, result.total());
                    assertEquals(2, result.rows().size());
                }
                assertFalse(fixture.lastRequest.sql().contains(" limit "), fixture.lastRequest.sql());
                for (var result : List.of(sync.cursorPage(query, cursor), reactive.cursorPage(query, cursor).block())) {
                    assertEquals(1001, result.rows().size());
                    assertTrue(result.hasMore());
                }
                assertFalse(fixture.lastRequest.sql().contains(" limit "), fixture.lastRequest.sql());
            } finally {
                sync.entityModels().close();
                reactive.entityModels().close();
            }
        }
    }

    private static Object carrier(boolean collection, Object... values) {
        return collection ? new ArrayList<>(Arrays.asList(values)) : values;
    }

    private static FormDataSqlRenderer renderer(ValueCodecRegistry codecs) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build()
                .withValueCodecs(codecs), RdbDialect.postgresql());
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("contains", "contains_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build())
                .masked("secret", MaskedFieldDefinition.builder("full").build()).build();
    }

    private static DynamicRow encryptedRow(ProtectedFieldRuntime runtime, DynamicForm form, long id, String value) {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("id", id);
        raw.put("secret", runtime.prepareWrite(form, Map.of("id", id, "secret", value, "note", "private"),
                DataScope.none(), ValueCodecRegistry.standard()).ownedValues().get("secret"));
        raw.put("note", "private");
        return DynamicRow.copyOf(raw);
    }

    private static final class Fixture {
        private final FormDataSqlRenderer renderer;
        private final List<DynamicRow> rows;
        private int queries;
        private SqlRequest lastRequest;
        private int writes;
        private String generatedColumn;
        private Fixture(FormDataSqlRenderer renderer, List<DynamicRow> rows) {
            this.renderer = renderer;
            this.rows = rows;
        }
        private final SyncSqlExecutor sync = new SyncSqlExecutor() {
            public List<DynamicRow> query(SqlRequest request) { queries++; lastRequest = request; return rows; }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                writes++; return new SqlWriteResult(1, List.of());
            }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String column) {
                generatedColumn = column;
                return rowsUpdatedReturningKeys(request, options);
            }
        };
        private final ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> { queries++; lastRequest = request; return Flux.fromIterable(rows); });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("unexpected write")); }
            public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                return Mono.fromSupplier(() -> { writes++; return new SqlWriteResult(1, List.of()); });
            }
            public Mono<SqlWriteResult> rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options, String column) {
                generatedColumn = column;
                return rowsUpdatedReturningKeys(request, options);
            }
        };
        private SyncFormClient syncClient() {
            return SyncFormClient.create(sync, (request, completed) -> {
                throw new AssertionError("unexpected batch");
            }, renderer);
        }
        private List<DynamicRow> select(boolean useReactive, QuerySpec spec, FieldUsePolicy policy) {
            if (useReactive) {
                ReactiveFormClient client = ReactiveFormClient.create(reactive, renderer);
                if (policy != null) client = client.withFieldUsePolicy(policy);
                try { return client.select(spec).collectList().block(); }
                finally { client.entityModels().close(); }
            }
            SyncFormClient client = syncClient();
            if (policy != null) client = client.withFieldUsePolicy(policy);
            try { return client.select(spec); }
            finally { client.entityModels().close(); }
        }
        private void insertReturningKeys(boolean useReactive, WriteSpec spec) {
            if (useReactive) {
                ReactiveFormClient client = ReactiveFormClient.create(reactive, renderer);
                try { client.insertReturningKeys(spec).block(); }
                finally { client.entityModels().close(); }
            } else {
                SyncFormClient client = syncClient();
                try { client.insertReturningKeys(spec); }
                finally { client.entityModels().close(); }
            }
        }
    }
}
