package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.array.ArrayStructuredConditions;
import com.flying.orm.rdb.array.ArrayTermHandlers;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.json.JsonStructuredConditions;
import com.flying.orm.rdb.vector.VectorStructuredConditions;
import com.flying.orm.rdb.vector.VectorTermHandlers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionResolverFacadeTest {

    @Test
    void standaloneArrayResolverCompilesItsBuiltInOperatorWithDefaultPolicy() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tags", "TEXT[]"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags", ArrayStructuredConditions.CONTAINS, List.of("a"));

        ConditionGroup condition = assertDoesNotThrow(
                () -> ArrayStructuredConditions.postgresql().compile(form, input));
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                                          .addTermPackage(ArrayTermHandlers.postgresql()).build();
        assertEquals("tags @> cast(? as text[])", renderer.renderWhere(condition).sql());
    }

    @Test
    void standaloneVectorResolverCompilesItsBuiltInOperatorWithDefaultPolicy() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(2))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "embedding", VectorStructuredConditions.L2_LESS_THAN,
                Map.of("vector", List.of(1, 2), "distance", 0.5));

        ConditionGroup condition = assertDoesNotThrow(
                () -> VectorStructuredConditions.postgresql().compile(form, input));
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                                          .addTermPackage(VectorTermHandlers.postgresql()).build();
        assertEquals("(embedding <-> cast(? as vector)) < ?", renderer.renderWhere(condition).sql());
    }

    @Test
    void jsonPathSchemaDoesNotConsumeTheCustomValueCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_PATH_EQUALS, Map.of("path", "status", "value", "active"));

        assertDoesNotThrow(() -> JsonStructuredConditions.standard().compile(
                form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void arrayResolverRetainsTheCustomValueCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tags", "TEXT[]"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags", ArrayStructuredConditions.CONTAINS, List.of("a", "b"));

        assertThrows(StructuredConditionException.class,
                     () -> ArrayStructuredConditions.postgresql().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void jsonContainsRetainsTheRootCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_CONTAINS, List.of("a", "b"));

        assertThrows(StructuredConditionException.class,
                     () -> JsonStructuredConditions.standard().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void jsonContainsRetainsTheNestedCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_CONTAINS, Map.of("tags", List.of("a", "b")));

        assertThrows(StructuredConditionException.class,
                     () -> JsonStructuredConditions.standard().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void guardStopsAnInvalidStructureBeforeInvokingAThirdPartyResolver() {
        AtomicInteger invocations = new AtomicInteger();
        StructuredConditionResolver thirdParty = (form, input, policy) -> {
            invocations.incrementAndGet();
            return ConditionGroup.and().build();
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeGuard guard = new FormScopeGuard(renderer, thirdParty, DataScope.none());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("id", "INTEGER"))
                                      .build();
        StructuredConditionInput deepInput = StructuredConditionInput.and(
                StructuredConditionInput.and(StructuredConditionInput.term("id", "=", 1)));

        assertThrows(StructuredConditionException.class,
                     () -> guard.scopedStructuredRead(
                             form, deepInput, StructuredConditionPolicy.defaults().withMaxDepth(1), DataScope.none()));
        assertEquals(0, invocations.get());
    }
}
