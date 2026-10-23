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

    @Test
    void formLookupPreservesCanonicalAliasesUnicodeAndMissingNameSemantics() {
        DynamicField customer = DynamicField.of("CustomerID", "BIGINT");
        DynamicField unicode = DynamicField.of("İsim", "VARCHAR");
        DynamicForm form = DynamicForm.builder("customers", "customers")
                .addField(customer).addField(unicode).build();
        for (String alias : new String[]{"customerid", "CustomerID", "CUSTOMERID", "  CustomerId  "}) {
            assertSame(customer, form.findField(alias).orElseThrow());
            assertSame(customer, form.field(alias));
        }
        assertSame(unicode, form.field("i\u0307sim"));
        assertSame(unicode, form.field("  İSIM  "));
        assertEquals(java.util.Optional.empty(), form.findField("missing"));
        assertEquals(java.util.Optional.empty(), form.findField("  MISSING  "));
        assertThrows(IllegalArgumentException.class, () -> form.field("missing"));
        for (String invalid : new String[]{null, "", " \t "}) {
            assertThrows(IllegalArgumentException.class, () -> form.findField(invalid));
        }
        DynamicForm empty = DynamicForm.builder("empty", "empty").build();
        assertEquals(java.util.Optional.empty(), empty.findField("id"));
        assertThrows(IllegalArgumentException.class, () -> empty.findField(null));
    }

    @Test
    void canonicalLookupDoesNotBypassBlankNameRejection() {
        DynamicForm form = DynamicForm.builder("t", "t")
                .addField(DynamicField.of("\0", "INT"))
                .addField(DynamicField.of("\0\u2003", "INT")).build();
        assertThrows(IllegalArgumentException.class, () -> form.findField(""));
        assertThrows(IllegalArgumentException.class, () -> form.findField("\u2003"));
    }
}
