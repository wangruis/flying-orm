package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AggregateFieldUseResultVisibilityTest {

    @Test
    void explicitFullVisibilityOverridesDisplayModeForGroupsAndExtrema() {
        DynamicForm form = DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build())
                .build();
        GroupSelection group = GroupSelection.of("secret", "group_secret");
        AggregateExpression<String> maximum = AggregateExpression.max(
                "secret", "selected_secret", LogicalType.TEXT, String.class);
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()).masked())
                .group(group).aggregate(maximum).build();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("secret", FieldUse.GROUP, FieldUse.AGGREGATE)
                .visibility("secret", FieldVisibility.FULL).build();
        Map<String, Object> values = new java.util.LinkedHashMap<>();
        values.put("group_secret", "classified");
        values.put("selected_secret", "classified");
        DynamicRow raw = DynamicRow.copyOf(values);
        FormDataSqlRenderer renderer = renderer();
        AggregateRow sync = SyncFormClient.create(sync(raw), batches(), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).getFirst();
        AggregateRow reactive = ReactiveFormClient.create(reactive(raw), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).single().block();

        assertAll(
                () -> assertEquals("classified", sync.get(group, String.class)),
                () -> assertEquals("classified", sync.get(maximum)),
                () -> assertEquals("classified", reactive.get(group, String.class)),
                () -> assertEquals("classified", reactive.get(maximum)));
    }

    @Test
    void minAndMaxRespectGlobalMaskedDisplayAcrossSyncAndReactive() {
        DynamicForm form = maskedForm();
        for (AggregateExpression<String> expression : List.of(
                AggregateExpression.min("secret", "selected_secret", LogicalType.TEXT, String.class),
                AggregateExpression.max("secret", "selected_secret", LogicalType.TEXT, String.class))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                            form, ConditionGroup.and().build()).masked())
                    .aggregate(expression).build();
            DynamicRow raw = DynamicRow.copyOf(Map.of("selected_secret", "classified"));
            FormDataSqlRenderer renderer = renderer();

            AggregateRow sync = SyncFormClient.create(sync(raw), batches(), renderer)
                    .aggregate(spec).getFirst();
            AggregateRow reactive = ReactiveFormClient.create(reactive(raw), renderer)
                    .aggregate(spec).single().block();

            assertEquals("**********", sync.get(expression));
            assertEquals("**********", reactive.get(expression));
        }
    }

    @Test
    void minAndMaxRespectDeclaredMaskingWithoutFieldUsePolicy() {
        DynamicForm form = DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full")
                        .display(SensitiveDisplayMode.MASKED).build())
                .build();
        for (AggregateExpression<String> expression : List.of(
                AggregateExpression.min("secret", "selected_secret", LogicalType.TEXT, String.class),
                AggregateExpression.max("secret", "selected_secret", LogicalType.TEXT, String.class))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                            form, ConditionGroup.and().build()))
                    .aggregate(expression).build();
            DynamicRow raw = DynamicRow.copyOf(Map.of("selected_secret", "classified"));
            FormDataSqlRenderer renderer = renderer();

            AggregateRow sync = SyncFormClient.create(sync(raw), batches(), renderer)
                    .aggregate(spec).getFirst();
            AggregateRow reactive = ReactiveFormClient.create(reactive(raw), renderer)
                    .aggregate(spec).single().block();

            assertEquals("**********", sync.get(expression));
            assertEquals("**********", reactive.get(expression));
        }
    }

    @Test
    void maskedCountAndCountDistinctFailDuringSharedPlanning() {
        DynamicForm form = maskedForm();
        FieldUsePolicy policy = maskedAggregatePolicy();
        FormDataSqlRenderer renderer = renderer();

        for (AggregateExpression<Long> expression : List.of(
                AggregateExpression.count("secret", "secret_count"),
                AggregateExpression.countDistinct("secret", "distinct_secret_count"))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                            form, ConditionGroup.and().build()))
                    .aggregate(expression)
                    .build();

            assertThrows(IllegalArgumentException.class,
                    () -> SyncFormClient.create(sync(DynamicRow.copyOf(Map.of())), batches(), renderer)
                            .withFieldUsePolicy(policy).aggregate(spec));
            assertThrows(IllegalArgumentException.class,
                    () -> ReactiveFormClient.create(reactive(DynamicRow.copyOf(Map.of())), renderer)
                            .withFieldUsePolicy(policy).aggregate(spec));
        }
    }

    @Test
    void fullCountRemainsLongAcrossSyncAndReactivePlanning() {
        DynamicForm form = maskedForm();
        AggregateExpression<Long> count = AggregateExpression.count("secret", "secret_count");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()))
                .aggregate(count)
                .build();
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("secret", FieldUse.AGGREGATE)
                .visibility("secret", FieldVisibility.FULL)
                .build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("secret_count", 7L));
        FormDataSqlRenderer renderer = renderer();

        AggregateRow sync = SyncFormClient.create(sync(raw), batches(), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).getFirst();
        AggregateRow reactive = ReactiveFormClient.create(reactive(raw), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).single().block();

        assertEquals(7L, sync.get(count));
        assertEquals(7L, reactive.get(count));
    }

    @Test
    void syncAndReactiveAggregateConsumeTheGovernedPublicationSnapshot() {
        DynamicForm form = maskedForm();
        AggregateExpression<String> latest = AggregateExpression.max(
                "secret", "latest_secret", LogicalType.TEXT, String.class);
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()))
                .aggregate(latest)
                .build();
        DynamicRow raw = DynamicRow.copyOf(Map.of("latest_secret", "classified"));
        FieldUsePolicy policy = maskedAggregatePolicy();
        FormDataSqlRenderer renderer = renderer();

        AggregateRow sync = SyncFormClient.create(sync(raw), batches(), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).getFirst();
        AggregateRow reactive = ReactiveFormClient.create(reactive(raw), renderer)
                .withFieldUsePolicy(policy).aggregate(spec).single().block();

        assertEquals("**********", sync.get(latest));
        assertEquals("**********", reactive.get(latest));
    }

    private static DynamicForm maskedForm() {
        return DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full")
                        .display(SensitiveDisplayMode.FULL).build())
                .build();
    }

    private static FieldUsePolicy maskedAggregatePolicy() {
        return FieldUsePolicy.builder()
                .allow("secret", FieldUse.AGGREGATE)
                .visibility("secret", FieldVisibility.MASKED)
                .build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static SyncSqlExecutor sync(DynamicRow row) {
        return new SyncSqlExecutor() {
            @Override public List<DynamicRow> query(
                    com.flying.orm.core.sql.render.SqlRequest request) {
                return List.of(row);
            }
            @Override public long rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                throw new UnsupportedOperationException();
            }
            @Override public SqlWriteResult rowsUpdatedReturningKeys(
                    com.flying.orm.core.sql.render.SqlRequest request,
                    SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static ReactiveSqlExecutor reactive(DynamicRow row) {
        return new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.just(row);
            }
            @Override public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }

    private static SyncBatchExecutor batches() {
        return new SyncBatchExecutor() {
            @Override public BatchWriteResult writeBatch(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }
            @Override public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
