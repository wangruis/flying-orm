package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionSpec;
import com.flying.orm.core.scope.TenantScope;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionTextValueContractTest {
    @Test
    void ordinaryAndCollectionConditionsPreserveTextValues() {
        ConditionGroup conditions = ConditionGroup.and()
                .where("name", "=", " Alice ")
                .where("name", "in", List.of(" Alice ", "Bob "))
                .whereIfPresent("name", "like", " %Alice% ")
                .where("name", "between", List.of(" z", "a")).build();
        assertEquals(List.of(" Alice ", List.of(" Alice ", "Bob "), " %Alice% ", List.of(" z", "a")),
                conditions.children().stream().map(TermCondition.class::cast).map(TermCondition::value).toList());
    }

    @Test
    void parameterConvertersReceiveTheOriginalText() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                .add(ParameterConditionSpec.builder("name", "name", "=").convert(value -> {
                    assertEquals(" Alice ", value);
                    return value;
                }).build())
                .build();
        assertEquals(" Alice ", firstValue(compiler.compile(Map.of("name", " Alice "))));
    }

    @Test
    void parameterRangesKeepDatabaseEndpointOrder() {
        for (String operator : List.of("between", "not-between")) {
            ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                    .add(ParameterConditionSpec.of("range", "name", operator)).build();
            assertEquals(List.of("a", "Z"), firstValue(compiler.compile(Map.of("range", List.of("a", "Z")))));
        }
    }

    @Test
    void structuredMappingsReceiveAndRetainTheOriginalText() {
        DynamicForm form = DynamicForm.builder("sample", "sample")
                .addField(DynamicField.of("name", "VARCHAR")).build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().mapValues((field, operator, value) -> {
            assertEquals(" Alice ", value);
            return value;
        });
        assertEquals(" Alice ", firstValue(StructuredConditionCompiler.create().compile(form,
                StructuredConditionInput.term("name", "eq", " Alice "), policy)));
    }

    @Test
    void tenantIdentityIsNotTrimmedIntoAnotherTenant() {
        TenantScope tenant = TenantScope.of("tenant", " tenant-A ");
        assertEquals(" tenant-A ", tenant.value());
        assertEquals(" tenant-A ", firstValue(tenant.toCondition()));
    }

    @Test
    void explicitLengthLimitCountsTheActualValue() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                .add(ParameterConditionSpec.of("name", "name", "="))
                .maxStringLength(5).build();
        assertThrows(ConditionValueException.class, () -> compiler.compile(Map.of("name", " Alice ")));
    }

    @Test
    void callersCanExplicitlyNormalizeTheirOwnText() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                .add(ParameterConditionSpec.builder("name", "name", "=")
                        .convert(value -> ((String) value).strip()).build())
                .build();
        assertEquals("Alice", firstValue(compiler.compile(Map.of("name", " Alice "))));
    }

    private static Object firstValue(ConditionGroup group) {
        return ((TermCondition) group.children().getFirst()).value();
    }
}
