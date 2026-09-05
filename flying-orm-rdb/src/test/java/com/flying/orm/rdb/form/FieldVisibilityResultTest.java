package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldDecision;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.JoinFieldDecision;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldVisibilityResultTest {

    @Test
    void restrictedFieldScopeDoesNotTurnBudgetOnlyApprovalIntoFullDisplay() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build())
                .build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("secret", "classified"));
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build())
                .withScope(DataScope.none().withFields(FieldScope.readable("secret")));
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxProjectionCount(16);
        SyncFormClient sync = SyncFormClient.create(
                syncExecutor(List.of(raw)), syncBatchExecutor(), renderer());
        ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor(raw), renderer());

        assertAll(
                () -> assertEquals("**********", sync.withQueryShapeLimits(limits)
                        .select(spec).getFirst().get("secret")),
                () -> assertEquals("**********", reactive.withQueryShapeLimits(limits)
                        .select(spec).single().block().get("secret")),
                () -> assertEquals("**********", sync.selectGoverned(
                        spec.masked(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                        .getFirst().get("secret")),
                () -> assertEquals("**********", reactive.selectGoverned(
                        spec.masked(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                        .single().block().get("secret")));
    }

    @Test
    void governedAllFullPublicationReturnsTheOriginalRowBeforeMaterializingResultMaps()
            throws Exception {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("id", 7L, "name", "Ada"));
        FieldUseSnapshot snapshot = FieldUseSnapshot.of(List.of(
                new FieldDecision("id", FieldUse.PROJECT, FieldUseOrigin.CALLER,
                                  true, FieldVisibility.FULL),
                new FieldDecision("name", FieldUse.PROJECT, FieldUseOrigin.CALLER,
                                  true, FieldVisibility.FULL)));

        assertSame(raw, FieldVisibilityPublisher.publish(null, form, raw, snapshot));
        JoinQuerySpec.Builder joinBuilder = JoinQuerySpec.builder(form);
        JoinSource root = joinBuilder.root();
        JoinQuerySpec join = joinBuilder.selectAs(root, "id", "id")
                                         .selectAs(root, "name", "name")
                                         .build();
        FieldUseSnapshot joinSnapshot = FieldUseSnapshot.ofJoin(List.of(
                new JoinFieldDecision(new JoinFieldRef(root, "id"), FieldUse.PROJECT,
                                      FieldUseOrigin.CALLER, true, FieldVisibility.FULL),
                new JoinFieldDecision(new JoinFieldRef(root, "name"), FieldUse.PROJECT,
                                      FieldUseOrigin.CALLER, true, FieldVisibility.FULL)));
        assertSame(raw, FieldVisibilityPublisher.publishJoin(null, join, raw, joinSnapshot));

        String source = Files.readString(Path.of(System.getProperty("basedir"), "src", "main", "java",
                                                 "com", "flying", "orm", "rdb", "form",
                                                 "FieldVisibilityPublisher.java"))
                             .replaceAll("\\s+", "");
        String fastReturn = "if(allFull){returnsafeRow;}";
        String mapAllocation = "newLinkedHashMap<>()";
        int firstFastReturn = source.indexOf(fastReturn);
        int firstMapAllocation = source.indexOf(mapAllocation);
        int secondFastReturn = source.indexOf(fastReturn, firstFastReturn + fastReturn.length());
        int secondMapAllocation = source.indexOf(mapAllocation, firstMapAllocation + mapAllocation.length());
        assertTrue(firstFastReturn >= 0 && firstFastReturn < firstMapAllocation
                           && secondFastReturn >= 0 && secondFastReturn < secondMapAllocation,
                   "all-FULL governed rows and joins must return before allocating publication maps");
    }

    @Test
    void queryLimitsDoNotOverrideTheFormsDeclaredSensitiveDisplayMode() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .masked("secret", MaskedFieldDefinition.builder("full").build())
                                      .build();
        SyncFormClient client = SyncFormClient.create(
                        syncExecutor(List.of(DynamicRow.copyOf(Map.of("secret", "classified")))),
                        syncBatchExecutor(), renderer())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16));

        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build());
        DynamicRow row = client.select(spec).getFirst();
        DynamicRow reactiveRow = ReactiveFormClient.create(
                        reactiveExecutor(DynamicRow.copyOf(Map.of("secret", "classified"))), renderer())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16))
                .select(spec)
                .single()
                .block();

        assertEquals("**********", row.get("secret"));
        assertEquals("**********", reactiveRow.get("secret"));
    }

    @Test
    void maskedPolicyWithoutAMaskingDefinitionFailsClosed() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .build();
        SyncFormClient client = SyncFormClient.create(
                        syncExecutor(List.of(DynamicRow.copyOf(Map.of("secret", "classified")))),
                        syncBatchExecutor(), renderer())
                .withFieldUsePolicy(FieldUsePolicy.builder()
                                                  .visibility("secret", FieldVisibility.MASKED)
                                                  .build());

        assertThrows(ScopeAccessException.class,
                     () -> client.select(QuerySpec.of(form, ConditionGroup.and().build())));
    }

    @Test
    void appliesFullMaskedAndHiddenDecisionsToTheReturnedRowLayout() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .addField(DynamicField.of("internal_note", "VARCHAR"))
                                      .masked("secret", MaskedFieldDefinition.builder("full").build())
                                      .build();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", 7L);
        values.put("secret", "classified");
        values.put("internal_note", "never-return-this");
        SyncSqlExecutor executor = syncExecutor(List.of(DynamicRow.copyOf(values)));
        SyncFormClient client = SyncFormClient.create(executor, syncBatchExecutor(), renderer())
                                              .withFieldUsePolicy(FieldUsePolicy.builder()
                                                                              .visibility("id", FieldVisibility.FULL)
                                                                              .visibility("secret", FieldVisibility.MASKED)
                                                                              .visibility("internal_note", FieldVisibility.HIDDEN)
                                                                              .build());

        DynamicRow row = client.select(QuerySpec.of(form, ConditionGroup.and().build())).getFirst();

        assertEquals(7L, row.get("id"));
        assertEquals("**********", row.get("secret"));
        assertFalse(row.containsKey("internal_note"));
        assertEquals(List.of("id", "secret"), row.keySet().stream().toList());
    }

    private static SyncSqlExecutor syncExecutor(List<DynamicRow> rows) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        return rows;
                    }
                    if (method.getName().equals("queryMapped")) {
                        @SuppressWarnings("unchecked")
                        RowMapper<Object> mapper = (RowMapper<Object>) arguments[2];
                        int rowLimit = (int) arguments[3];
                        int size = rowLimit == 0 ? rows.size() : Math.min(rowLimit, rows.size());
                        java.util.ArrayList<Object> mapped = new java.util.ArrayList<>(size);
                        for (int index = 0; index < size; index++) {
                            mapped.add(mapper.map(rows.get(index)));
                        }
                        return List.copyOf(mapped);
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> { throw new UnsupportedOperationException(method.toString()); });
    }

    private static ReactiveSqlExecutor reactiveExecutor(DynamicRow row) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.just(row);
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
