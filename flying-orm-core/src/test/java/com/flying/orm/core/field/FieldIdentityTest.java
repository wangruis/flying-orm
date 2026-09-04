package com.flying.orm.core.field;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldIdentityTest {

    @Test
    void preservesSqlNameAndBuildsOneCaseInsensitiveLookupKey() {
        FieldIdentity identity = FieldIdentity.of("  CustomerID  ");

        assertEquals("CustomerID", identity.name());
        assertEquals("customerid", identity.key());
        assertEquals(identity, FieldIdentity.of("customerid"));
        assertEquals(identity.hashCode(), FieldIdentity.of("CUSTOMERID").hashCode());
    }

    @Test
    void rejectsMissingFieldNamesAtTheBoundary() {
        assertThrows(IllegalArgumentException.class, () -> FieldIdentity.of("  "));
        assertThrows(IllegalArgumentException.class, () -> FieldIdentity.of(null));
    }

    @Test
    void reusesOneIdentityFromFormFieldToConditionAst() {
        DynamicField field = DynamicField.of("CustomerID", "BIGINT");

        ConditionGroup where = ConditionGroup.and()
                                             .where(field.identity(), "=", 42L)
                                             .build();

        TermCondition term = (TermCondition) where.children().getFirst();
        assertSame(field.identity(), term.identity());
        assertEquals("CustomerID", term.field());
    }

    @Test
    void structuredConditionsReuseTheCanonicalFormFieldIdentity() {
        DynamicField field = DynamicField.of("CustomerID", "BIGINT");
        DynamicForm form = DynamicForm.builder("customers", "customers")
                                      .addField(field)
                                      .build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                    .allowOnlyFields(Set.of("customerid"))
                                                                    .allowFieldOperators("CUSTOMERID", Set.of("eq"));

        ConditionGroup where = StructuredConditionCompiler.create().compile(
                form, StructuredConditionInput.term("CustomerId", "EQ", 42L), policy);

        TermCondition term = (TermCondition) where.children().getFirst();
        assertSame(field.identity(), term.identity());
    }
}
