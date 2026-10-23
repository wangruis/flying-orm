package com.flying.orm.rdb.array;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ArrayPredicateContractTest {
    private static final Map<String, String> NEGATED = Map.of(
            "array-not-contains", "@>", "array-not-contained-by", "<@", "array-not-overlaps", "&&");
    private static final DynamicForm FORM = DynamicForm.builder("events", "events")
            .addField(DynamicField.of("tags", "TEXT[]")).build();
    private static final SqlRenderer SQL = SqlRenderer.builder().addDefaultTerms()
            .addTermPackage(ArrayTermHandlers.postgresql()).build();

    @Test
    void negativePredicatesWrapTheOriginalOperatorAndBindOneTypedArray() {
        for (Map.Entry<String, String> entry : NEGATED.entrySet()) {
            SqlFragment fragment = assertDoesNotThrow(() -> SQL.renderWhere(SQL.conditions()
                    .where("tags", entry.getKey(), ArrayConditionValue.of(List.of("x') OR TRUE --"), "TEXT[]")).build()));
            assertEquals("(not (tags " + entry.getValue() + " cast(? as text[])))", fragment.sql());
            assertEquals(1, fragment.parameters().size());
            assertArrayEquals(new String[]{"x') OR TRUE --"}, (String[]) fragment.parameters().getFirst());
        }
    }

    @Test
    void emptyArraysRemainTypedParametersForPositiveAndNegativePredicates() {
        for (String operator : List.of("array-contains", "array-contained-by", "array-overlaps",
                "array-not-contains", "array-not-contained-by", "array-not-overlaps")) {
            ConditionGroup condition = assertDoesNotThrow(() -> ArrayStructuredConditions.postgresql()
                    .compile(FORM, StructuredConditionInput.term("tags", operator, List.of())));
            SqlFragment fragment = SQL.renderWhere(condition);
            assertFalse(fragment.sql().isBlank(), "empty input must not erase a filter");
            assertEquals(1, fragment.parameters().size());
            assertArrayEquals(new String[0], (String[]) fragment.parameters().getFirst());
        }
    }

    @Test
    void structuredNegationUsesMetadataAndFreezesCallerValues() {
        DynamicForm numbers = DynamicForm.builder("numbers", "numbers")
                .addField(DynamicField.of("items", "INTEGER[]")).build();
        for (String operator : NEGATED.keySet()) {
            List<Object> values = new ArrayList<>(Arrays.asList("7", null));
            ConditionGroup condition = assertDoesNotThrow(() -> ArrayStructuredConditions.postgresql()
                    .compile(numbers, StructuredConditionInput.term("items", operator, Map.of("values", values))));
            values.clear();
            SqlFragment fragment = SQL.renderWhere(condition);
            assertArrayEquals(new Integer[]{7, null}, (Integer[]) fragment.parameters().getFirst());
            assertTrue(fragment.sql().contains("cast(? as integer[])"));
            assertFalse(fragment.sql().contains("coalesce"), "SQL unknown must not become true");
        }
    }

    @Test
    void negativeTermsKeepTheExistingFrontendTypeAndBudgetBoundaries() {
        for (String operator : NEGATED.keySet()) {
            assertThrows(IllegalArgumentException.class, () -> ArrayStructuredConditions.postgresql().compile(FORM,
                    StructuredConditionInput.term("tags", operator, Map.of("values", List.of("a"), "type", "integer[]"))));
            for (Object values : List.of(List.of("a", "b"), new String[]{"a", "b"}, Map.of("values", List.of("a", "b")))) {
                StructuredConditionException error = assertThrows(StructuredConditionException.class,
                        () -> ArrayStructuredConditions.postgresql().compile(FORM,
                                StructuredConditionInput.term("tags", operator, values),
                                StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
                assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, error.code());
            }
        }
    }

    @Test
    void syncAndReactiveQueriesUseTheSameGovernedNegativePredicates() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SQL, RdbDialect.postgresql());
        List<SqlRequest> requests = new ArrayList<>();
        SyncSqlExecutor sync = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { requests.add(request); return List.of(); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("unexpected write");
            }
        };
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> { requests.add(request); return Flux.empty(); });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
        };
        FieldUsePolicy allowed = FieldUsePolicy.builder().allow("tags", FieldUse.FILTER, FieldUse.PROJECT).build();
        SyncFormClient jdbc = SyncFormClient.create(sync, (request, completed) -> { throw new AssertionError(); }, renderer)
                .withStructuredConditionResolver(ArrayStructuredConditions.postgresql()).withFieldUsePolicy(allowed);
        ReactiveFormClient r2dbc = ReactiveFormClient.create(reactive, renderer)
                .withStructuredConditionResolver(ArrayStructuredConditions.postgresql()).withFieldUsePolicy(allowed);
        for (String operator : NEGATED.keySet()) {
            QuerySpec query = QuerySpec.structured(FORM, StructuredConditionInput.term("tags", operator, List.of("a")));
            jdbc.select(query);
            SqlRequest expected = requests.getLast();
            int before = requests.size();
            Flux<DynamicRow> publisher = r2dbc.select(query);
            assertEquals(before, requests.size());
            for (int repeat = 0; repeat < 2; repeat++) {
                publisher.collectList().block();
                assertEquals(expected.sql(), requests.getLast().sql());
                assertArrayEquals((String[]) expected.parameters().getFirst(), (String[]) requests.getLast().parameters().getFirst());
            }
            FieldUsePolicy denied = FieldUsePolicy.builder().allow("tags", FieldUse.PROJECT).build();
            assertThrows(ScopeAccessException.class, () -> jdbc.withFieldUsePolicy(denied).select(query));
            assertThrows(ScopeAccessException.class,
                    () -> r2dbc.withFieldUsePolicy(denied).select(query).collectList().block());
        }
    }
}
