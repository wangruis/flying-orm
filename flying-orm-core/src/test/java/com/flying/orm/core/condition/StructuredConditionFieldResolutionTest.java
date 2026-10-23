package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.QueryRelation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class StructuredConditionFieldResolutionTest {
    private final StructuredConditionCompiler compiler = StructuredConditionCompiler.create();

    @Test
    void bindsQualifiedFieldsWithoutSplittingGroupsOrResolvingTheTargetTwice() {
        AtomicInteger resolved = new AtomicInteger();
        DynamicForm department = DynamicForm.builder("department", "department")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DynamicForm user = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("department_id", "BIGINT"))
                .relation("department", QueryRelation.of(() -> {
                    resolved.incrementAndGet();
                    return department;
                }, "department_id", "id")).build();
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(user);
        ConditionGroup condition = compiler.compileFields(path -> {
            var field = query.field(path);
            return field.source().form().field(field.field());
        }, StructuredConditionInput.or(
                StructuredConditionInput.term("id", "eq", "1"),
                StructuredConditionInput.and(
                        StructuredConditionInput.term("department.id", "gt", "2"),
                        StructuredConditionInput.term("department.id", "lt", "8"))),
                StructuredConditionPolicy.defaults());

        assertEquals(LogicalOperator.OR, condition.operator());
        assertEquals(1L, ((TermCondition) condition.children().getFirst()).value());
        ConditionGroup group = (ConditionGroup) condition.children().getLast();
        assertEquals(List.of("department.id", "department.id"), group.children().stream()
                .map(node -> ((TermCondition) node).field()).toList());
        assertEquals(List.of(2L, 8L), group.children().stream()
                .map(node -> ((TermCondition) node).value()).toList());
        assertEquals(1, query.where(condition).buildRelation().joins().size());
        assertEquals(1, resolved.get());
    }

    @Test
    void preservesWholeTreeBudgetAndDoesNotResolveDeniedFields() {
        AtomicInteger lookups = new AtomicInteger();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("department.id", "eq", 1),
                StructuredConditionInput.term("department.id", "eq", 2));
        assertThrows(StructuredConditionException.class, () -> compiler.compileFields(name -> {
            lookups.incrementAndGet();
            return DynamicField.of("id", "BIGINT");
        }, input, StructuredConditionPolicy.defaults().denyFields(List.of("department.id"))));
        assertEquals(0, lookups.get());
        assertThrows(StructuredConditionException.class, () -> compiler.compileFields(
                name -> DynamicField.of("id", "BIGINT"), input,
                StructuredConditionPolicy.defaults().withMaxNodes(2)));
    }

    @Test
    void keepsUnknownFieldErrorAndOrdinaryCanonicalIdentity() {
        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                () -> compiler.compileFields(name -> null,
                        StructuredConditionInput.term("department.missing", "eq", 1),
                        StructuredConditionPolicy.defaults()));
        assertEquals(StructuredConditionErrorCode.FIELD_NOT_ALLOWED, error.code());
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("UserId", "BIGINT")).build();
        ConditionGroup ordinary = compiler.compile(form, StructuredConditionInput.term("USERID", "eq", "2"));
        assertEquals("UserId", ((TermCondition) ordinary.children().getFirst()).field());
    }
}
