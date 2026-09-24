package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionSpec;
import com.flying.orm.core.scope.TenantScope;
import com.flying.orm.core.scope.TimeScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionScaleConfigurationTest {
    private final SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
    private final DynamicForm form = DynamicForm.builder("samples", "samples")
            .addField(DynamicField.of("id", "INTEGER")).build();

    @Test
    void trustedCollectionsRenderBeyondTheFormerCeiling() {
        List<Integer> values = IntStream.range(0, 1_001).boxed().toList();
        for (Object source : List.of(values, IntStream.range(0, 1_001).toArray())) {
            ConditionGroup direct = ConditionGroup.and().add(TermCondition.of("id", "in", source)).build();
            assertEquals(values, renderer.renderWhere(direct).parameters());
            assertEquals(values, renderer.renderWhere(ConditionGroup.and().where("id", "in", source).build()).parameters());
        }
        String text = "a".repeat(4_097);
        assertEquals(List.of(text), renderer.renderWhere(ConditionGroup.and().where("id", "=", text).build()).parameters());
    }

    @Test
    void trustedScopesDoNotReintroduceTheFormerTextCeiling() {
        String text = "a".repeat(4_097);
        assertEquals(List.of(text), renderer.renderWhere(TenantScope.of("tenant_id", text).toCondition()).parameters());
        assertEquals(List.of(text), renderer.renderWhere(TimeScope.from("time_key", text).toCondition()).parameters());
        assertEquals(List.of(text), renderer.renderWhere(TimeScope.before("time_key", text).toCondition()).parameters());
        assertThrows(ConditionValueException.class, () -> TenantScope.of("tenant_id", " "));
        assertThrows(ConditionValueException.class, () -> TimeScope.from("time_key", " "));
    }

    @Test
    void relationCollectionsRenderBeyondTheFormerCeiling() {
        SqlRenderer relations = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.relationExists(
                "member-of", "membership", "m", "member_id", "group_id")).build();
        List<Integer> values = IntStream.range(0, 1_001).boxed().toList();
        for (Object source : List.of(values, IntStream.range(0, 1_001).toArray())) {
            assertEquals(values, relations.renderWhere(relations.conditions().where("id", "member-of", source).build()).parameters());
        }
    }

    @Test
    void parameterCompilerHonorsLargerConfiguredCollectionAndIntegerMaximum() {
        List<Integer> values = IntStream.range(0, 1_001).boxed().toList();
        for (int limit : new int[]{1_001, Integer.MAX_VALUE}) {
            ParameterConditionCompiler compiler = ParameterConditionCompiler.builder().maxCollectionSize(limit)
                    .addOrGroup(ParameterConditionSpec.of("ids", "id", "in"),
                                ParameterConditionSpec.of("ids", "other_id", "in")).build();
            Iterable<Integer> oneShot = values::iterator;
            assertEquals(2_002, renderer.renderWhere(compiler.compile(Map.of("ids", oneShot))).parameters().size());
        }
        ParameterConditionCompiler limited = ParameterConditionCompiler.builder().maxCollectionSize(1_001)
                .add(ParameterConditionSpec.of("ids", "id", "in")).build();
        assertThrows(ConditionValueException.class, () -> limited.compile(Map.of("ids", Collections.nCopies(1_002, 1))));
    }

    @Test
    void structuredPolicyCanRaiseEveryScaleBudgetWithoutAnotherInternalCeiling() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                .withMaxDepth(128).withMaxNodes(20_002).withMaxCollectionSize(20_001);
        StructuredConditionInput collection = StructuredConditionInput.term("id", "in", Collections.nCopies(20_001, 7));
        StructuredConditionCompiler.validateStructure(collection, policy);
        assertEquals(20_001, renderer.renderWhere(StructuredConditionCompiler.create().compile(form, collection, policy)).parameters().size());
        StructuredConditionInput wide = new StructuredConditionInput(null, null, null, "and",
                Collections.nCopies(10_001, StructuredConditionInput.term("id", "eq", 7)));
        StructuredConditionCompiler.validateStructure(wide, policy);
        assertEquals(10_001, renderer.renderWhere(StructuredConditionCompiler.create().compile(form, wide, policy)).parameters().size());
        StructuredConditionInput input = StructuredConditionInput.term("id", "eq", 7);
        for (int depth = 1; depth < 128; depth++) input = StructuredConditionInput.and(input);
        StructuredConditionCompiler.validateStructure(input, policy);
        assertEquals(List.of(7), renderer.renderWhere(StructuredConditionCompiler.create().compile(form, input, policy)).parameters());
        StructuredConditionInput tooDeep = StructuredConditionInput.and(input);
        assertThrows(StructuredConditionException.class, () -> StructuredConditionCompiler.create().compile(form, tooDeep, policy));
    }

    @Test
    void explicitLimitsAndPositiveConfigurationContractsRemainEnforced() {
        StructuredConditionPolicy defaults = StructuredConditionPolicy.defaults();
        assertEquals(8, defaults.maxDepth());
        assertEquals(100, defaults.maxNodes());
        assertEquals(1_000, defaults.maxCollectionSize());
        assertEquals(4_096, defaults.maxStringLength());
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxDepth(0));
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxNodes(0));
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxCollectionSize(0));
        assertThrows(IllegalArgumentException.class, () -> ParameterConditionCompiler.builder().maxCollectionSize(0));
        StructuredConditionInput input = StructuredConditionInput.term("id", "in", Collections.nCopies(1_001, 1));
        assertThrows(StructuredConditionException.class, () -> StructuredConditionCompiler.create().compile(form, input));
    }

    @Test
    void trustedTreesAcceptMoreThanTenThousandNodesAndSixtyFourLevels() {
        ConditionGroup.Builder wide = ConditionGroup.and();
        for (int index = 0; index < 10_001; index++) wide.where("id", "=", index);
        assertEquals(10_001, renderer.renderWhere(wide.build()).parameters().size());
        ConditionGroup deep = ConditionGroup.and().where("id", "=", 7).build();
        for (int depth = 1; depth < 128; depth++) deep = ConditionGroup.and().add(deep).build();
        assertEquals(List.of(7), renderer.renderWhere(deep).parameters());
        assertEquals(1, deep.executionView().parameterCount());
    }

    @Test
    void snapshotAndRawValidationHaveNoUnconfigurableReferenceOrDepthBudget() {
        Object nested = Collections.nCopies(20_001, 7);
        for (int depth = 0; depth < 128; depth++) nested = List.of(nested);
        StructuredConditionInput input = StructuredConditionInput.term("id", "raw", nested);
        StructuredConditionCompiler.validateStructure(input, StructuredConditionPolicy.defaults()
                .allowOperator("raw").withMaxDepth(256).withMaxNodes(256).withMaxCollectionSize(20_001));
        assertNotSame(nested, input.value());
    }
}
