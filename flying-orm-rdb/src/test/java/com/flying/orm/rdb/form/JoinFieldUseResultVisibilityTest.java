package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;

class JoinFieldUseResultVisibilityTest {

    @Test
    void computedColumnCoexistsWithDeclaredAndGovernedMaskedProjections() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build()).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinQuerySpec spec = builder.selectAs(builder.root(), "secret", "visible_secret")
                .select(com.flying.orm.core.expression.ValueExpression.value(3), "calculated").build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("visible_secret", "classified", "calculated", 3));
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .joinVisibility(new JoinFieldRef(builder.root(), "secret"), FieldVisibility.MASKED).build();
        SyncFormClient sync = SyncFormClient.create(syncExecutor(raw), syncBatchExecutor(), renderer());
        ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor(raw), renderer());
        for (DynamicRow row : List.of(sync.selectJoin(spec).getFirst(), reactive.selectJoin(spec).single().block(),
                sync.withFieldUsePolicy(policy).selectJoin(spec).getFirst(),
                reactive.withFieldUsePolicy(policy).selectJoin(spec).single().block())) {
            assertEquals("**********", row.get("visible_secret"));
            assertEquals(3, row.get("calculated"));
        }
    }

    @Test
    void explicitFullVisibilityOverridesJoinDeclaredMasking() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build()).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinQuerySpec spec = builder.selectAs(builder.root(), "secret", "visible_secret").build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("visible_secret", "classified"));
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .joinVisibility(new JoinFieldRef(builder.root(), "secret"), FieldVisibility.FULL).build();

        assertAll(
                () -> assertEquals("classified", SyncFormClient.create(
                        syncExecutor(raw), syncBatchExecutor(), renderer())
                        .withFieldUsePolicy(policy).selectJoin(spec).getFirst().get("visible_secret")),
                () -> assertEquals("classified", ReactiveFormClient.create(reactiveExecutor(raw), renderer())
                        .withFieldUsePolicy(policy).selectJoin(spec).single().block().get("visible_secret")));
    }

    @Test
    void governedJoinAppliesTheMaskingPolicyExactlyOnce() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("length-label").build()).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinQuerySpec spec = builder.selectAs(builder.root(), "secret", "visible_secret")
                .orderBy(builder.root(), "id", com.flying.orm.core.page.PageSort.Direction.ASC).build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("visible_secret", "classified"));
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allowJoin(new JoinFieldRef(builder.root(), "id"), FieldUse.SORT)
                .joinVisibility(new JoinFieldRef(builder.root(), "secret"), FieldVisibility.MASKED).build();
        FormDataSqlRenderer renderer = renderer().withProtectedFields(
                com.flying.orm.rdb.protection.ProtectedFieldRuntime.withoutKeys(
                        com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry.standard(),
                        com.flying.orm.rdb.protection.MaskingPolicyRegistry.standard()
                                .with("length-label", (value, definition) -> "[" + value.length() + "]")));

        assertAll(
                () -> assertEquals("[10]", SyncFormClient.create(syncExecutor(raw), syncBatchExecutor(), renderer)
                        .withFieldUsePolicy(policy).selectJoin(spec).getFirst().get("visible_secret")),
                () -> assertEquals("[10]", ReactiveFormClient.create(reactiveExecutor(raw), renderer)
                        .withFieldUsePolicy(policy).selectJoin(spec).single().block().get("visible_secret")),
                () -> assertEquals("[10]", SyncFormClient.create(syncExecutor(raw), syncBatchExecutor(), renderer)
                        .withFieldUsePolicy(policy).pageJoin(spec, com.flying.orm.core.page.PageQuery.of(1, 10))
                        .rows().getFirst().get("visible_secret")),
                () -> assertEquals(List.of("[10]", "[10]"), ReactiveFormClient.create(reactiveExecutor(raw), renderer)
                        .withFieldUsePolicy(policy).pageJoin(spec, com.flying.orm.core.page.PageQuery.of(1, 10))
                        .repeat(1).map(page -> page.rows().getFirst().get("visible_secret")).collectList().block()));
    }

    @Test
    void queryLimitsDoNotOverrideTheJoinFieldsDeclaredSensitiveDisplayMode() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build())
                .build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "secret", "secret").build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("secret", "classified"));

        DynamicRow row = SyncFormClient.create(syncExecutor(raw), syncBatchExecutor(), renderer())
                .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxProjectionCount(16))
                .selectJoin(spec)
                .getFirst();

        assertEquals("**********", row.get("secret"));
    }

    @Test
    void syncAndReactiveJoinConsumeTheGovernedPublicationSnapshot() {
        DynamicForm form = form();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource root = builder.root();
        JoinQuerySpec spec = builder.selectAs(root, "id", "id")
                                    .selectAs(root, "secret", "secret")
                                    .selectAs(root, "internal_note", "internal_note")
                                    .build();
        DynamicRow raw = rawRow();
        FormDataSqlRenderer renderer = renderer();
        DynamicRow sync = SyncFormClient.create(syncExecutor(raw), syncBatchExecutor(), renderer)
                .withFieldUsePolicy(policy(root)).selectJoin(spec).getFirst();
        DynamicRow reactive = ReactiveFormClient.create(reactiveExecutor(raw), renderer)
                .withFieldUsePolicy(policy(root)).selectJoin(spec).single().block();

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
                          .masked("secret", MaskedFieldDefinition.builder("full")
                                                                 .display(SensitiveDisplayMode.FULL)
                                                                 .build())
                          .build();
    }

    private static FieldUsePolicy policy(JoinSource root) {
        return FieldUsePolicy.builder()
                             .joinVisibility(new JoinFieldRef(root, "id"), FieldVisibility.FULL)
                             .joinVisibility(new JoinFieldRef(root, "secret"), FieldVisibility.MASKED)
                             .allowJoin(new JoinFieldRef(root, "internal_note"), FieldUse.PROJECT)
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
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{com.flying.orm.rdb.support.ListQueryFixture.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        com.flying.orm.core.sql.render.SqlRequest request =
                                (com.flying.orm.core.sql.render.SqlRequest) arguments[0];
                        return List.of(request.sql().startsWith("select count(*)")
                                ? DynamicRow.copyOf(Map.of("total", 1L)) : raw);
                    }
                    if (method.getName().equals("queryMapped")) {
                        @SuppressWarnings("unchecked")
                        RowMapper<Object> mapper = (RowMapper<Object>) arguments[2];
                        return List.of(mapper.map(raw));
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(DynamicRow raw) {
        return new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.just(request.sql().startsWith("select count(*)")
                        ? DynamicRow.copyOf(Map.of("total", 1L)) : raw);
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

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
