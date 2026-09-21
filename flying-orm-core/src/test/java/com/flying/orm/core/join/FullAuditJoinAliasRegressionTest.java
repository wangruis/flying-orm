package com.flying.orm.core.join;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditJoinAliasRegressionTest {

    private static final String LONG_FIELD = "abcdefghijklmnopqrstuvwxyzabcd";

    @Test
    void automaticAliasesRemainDistinctWhenLongFieldIsSelectedFirst() {
        assertAutomaticAliases(true);
    }

    @Test
    void automaticAliasesRemainDistinctWhenShortFieldIsSelectedFirst() {
        assertAutomaticAliases(false);
    }

    @Test
    void explicitDuplicateAliasesRemainCaseInsensitiveAndRejected() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form(LONG_FIELD, "f0"));
        builder.selectAs(builder.root(), LONG_FIELD, "ResultValue");

        assertThrows(IllegalArgumentException.class,
                () -> builder.selectAs(builder.root(), "f0", "resultvalue"));
        assertEquals(List.of("ResultValue"), aliases(builder.build()));
    }

    @Test
    void automaticAliasesAvoidExplicitNamesWhileExplicitDuplicatesRemainRejected() {
        DynamicForm form = form("id", LONG_FIELD);
        JoinQuerySpec.Builder explicitFirst = JoinQuerySpec.builder(form);
        explicitFirst.selectAs(explicitFirst.root(), "id", "S0_F1");

        explicitFirst.select(explicitFirst.root(), LONG_FIELD);
        List<String> explicitFirstAliases = aliases(explicitFirst.build());
        assertEquals(2, explicitFirstAliases.size());
        assertEquals("S0_F1", explicitFirstAliases.getFirst());
        assertNotEquals("s0_f1", explicitFirstAliases.getLast().toLowerCase(Locale.ROOT));
        assertTrue(explicitFirstAliases.getLast().length() <= JoinProjection.MAX_PORTABLE_ALIAS_LENGTH);
        assertTrue(explicitFirstAliases.getLast().matches("[A-Za-z_][A-Za-z0-9_]*"));

        JoinQuerySpec.Builder generatedFirst = JoinQuerySpec.builder(form);
        generatedFirst.select(generatedFirst.root(), LONG_FIELD);

        assertThrows(IllegalArgumentException.class,
                () -> generatedFirst.selectAs(generatedFirst.root(), "id", "S0_F1"));
        assertEquals(List.of("s0_f1"), aliases(generatedFirst.build()));
    }

    @Test
    void selectingTheSameFieldAutomaticallyTwiceRemainsRejected() {
        for (String field : List.of(LONG_FIELD, "f0")) {
            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form(LONG_FIELD, "f0"));
            builder.select(builder.root(), field);

            assertThrows(IllegalArgumentException.class,
                    () -> builder.select(builder.root(), field.toUpperCase(Locale.ROOT)));
            assertEquals(List.of(field), fields(builder.build()));
        }
    }

    @Test
    void sameFieldRemainsSelectableWithDistinctExplicitAliases() {
        DynamicForm form = form(LONG_FIELD);
        JoinQuerySpec.Builder explicitFirst = JoinQuerySpec.builder(form);
        explicitFirst.selectAs(explicitFirst.root(), LONG_FIELD, "first_value")
                .selectAs(explicitFirst.root(), LONG_FIELD, "second_value")
                .select(explicitFirst.root(), LONG_FIELD);

        JoinQuerySpec firstQuery = explicitFirst.build();
        assertEquals(List.of(LONG_FIELD, LONG_FIELD, LONG_FIELD), fields(firstQuery));
        assertEquals(3L, aliases(firstQuery).stream().distinct().count());

        JoinQuerySpec.Builder automaticFirst = JoinQuerySpec.builder(form);
        automaticFirst.select(automaticFirst.root(), LONG_FIELD)
                .selectAs(automaticFirst.root(), LONG_FIELD, "explicit_value");

        JoinQuerySpec secondQuery = automaticFirst.build();
        assertEquals(List.of(LONG_FIELD, LONG_FIELD), fields(secondQuery));
        assertEquals(2L, aliases(secondQuery).stream().distinct().count());
    }

    private static void assertAutomaticAliases(boolean longFirst) {
        for (boolean longDeclaredFirst : List.of(true, false)) {
            String shortField = longDeclaredFirst ? "f0" : "f1";
            DynamicForm form = longDeclaredFirst
                    ? form(LONG_FIELD, shortField)
                    : form(shortField, LONG_FIELD);
            JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
            List<String> selectedFields = longFirst
                    ? List.of(LONG_FIELD, shortField)
                    : List.of(shortField, LONG_FIELD);
            for (String field : selectedFields) {
                builder.select(builder.root(), field);
            }

            JoinQuerySpec query = builder.build();
            assertEquals(selectedFields, fields(query));
            List<String> aliases = aliases(query);
            assertEquals(2, aliases.size());
            assertNotEquals(aliases.get(0).toLowerCase(Locale.ROOT),
                    aliases.get(1).toLowerCase(Locale.ROOT));
            for (String alias : aliases) {
                assertTrue(alias.length() <= JoinProjection.MAX_PORTABLE_ALIAS_LENGTH);
                assertTrue(alias.matches("[A-Za-z_][A-Za-z0-9_]*"));
            }
        }
    }

    private static DynamicForm form(String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder("alias_regression", "alias_regression");
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "BIGINT"));
        }
        return builder.build();
    }

    private static List<String> aliases(JoinQuerySpec query) {
        return query.projections().stream().map(JoinProjection::alias).toList();
    }

    private static List<String> fields(JoinQuerySpec query) {
        return query.projections().stream().map(projection -> projection.field().field()).toList();
    }
}
