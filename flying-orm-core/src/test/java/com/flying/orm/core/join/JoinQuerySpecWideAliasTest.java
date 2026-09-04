package com.flying.orm.core.join;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JoinQuerySpecWideAliasTest {

    @Test
    void wideFallbackAliasesFollowFieldOrderRegardlessOfSelectionOrder() {
        int width = 512;
        DynamicForm.Builder formBuilder = DynamicForm.builder("wide", "wide")
                .addField(DynamicField.primaryKey("id", "BIGINT"));
        for (int index = 0; index < width; index++) {
            formBuilder.addField(DynamicField.of(longField(index), "BIGINT"));
        }
        DynamicForm form = formBuilder.build();
        JoinQuerySpec.Builder reverse = JoinQuerySpec.builder(form);
        JoinQuerySpec.Builder forward = JoinQuerySpec.builder(form);
        for (int index = width - 1; index >= 0; index--) {
            reverse.select(reverse.root(), longField(index).toUpperCase(Locale.ROOT));
        }
        for (int index = 0; index < width; index++) {
            forward.select(forward.root(), longField(index));
        }

        List<JoinProjection> reverseProjections = reverse.build().projections();
        List<JoinProjection> forwardProjections = forward.build().projections();
        assertEquals(width, reverseProjections.size());
        assertEquals(width, forwardProjections.size());
        for (int index = 0; index < width; index++) {
            JoinProjection projection = reverseProjections.get(width - 1 - index);
            assertEquals("s0_f" + (index + 1), projection.alias());
            assertEquals(longField(index), projection.field().field());
            assertSame(reverse.root(), projection.field().source());
            assertEquals(projection.alias(), forwardProjections.get(index).alias());
        }
    }

    @Test
    void preservesShortAliasesAtThePortableLimitAndCanonicalFieldNames() {
        String atLimit = "A".repeat(27);
        String beyondLimit = "B".repeat(28);
        DynamicForm form = form("limits", "id", atLimit, beyondLimit);
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);

        JoinQuerySpec query = builder.select(builder.root(), " " + beyondLimit.toLowerCase(Locale.ROOT) + " ")
                .select(builder.root(), " " + atLimit.toLowerCase(Locale.ROOT) + " ")
                .select(builder.root(), "ID")
                .build();

        assertEquals(List.of("s0_f2", "s0_" + atLimit, "s0_id"), aliases(query));
        assertEquals(List.of(beyondLimit, atLimit, "id"),
                query.projections().stream().map(projection -> projection.field().field()).toList());
    }

    @Test
    void usesPhysicalOrdinalsForNamesThatNeedQuotingOrContainNonAsciiCharacters() {
        DynamicForm form = form("quoted_fields", "id", "Order Total", "用户名称");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);

        JoinQuerySpec query = builder.select(builder.root(), "用户名称")
                .select(builder.root(), " order total ")
                .build();

        assertEquals(List.of("s0_f2", "s0_f1"), aliases(query));
        assertEquals(List.of("用户名称", "Order Total"),
                query.projections().stream().map(projection -> projection.field().field()).toList());
    }

    @Test
    void keepsFieldOrdinalsAndSourceIdentitySeparateAcrossJoinedForms() {
        String shared = longField(0);
        DynamicForm rootForm = form("root_table", "id", shared);
        DynamicForm joinedForm = form("joined_table", "id", "padding", shared);
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(rootForm);
        JoinSource root = builder.root();
        JoinSource joined = builder.join(JoinType.INNER, joinedForm, root, "id", "id");

        JoinQuerySpec query = builder.select(joined, shared)
                .select(root, shared)
                .build();

        assertEquals(List.of("s1_f2", "s0_f1"), aliases(query));
        assertSame(joined, query.projections().get(0).field().source());
        assertSame(root, query.projections().get(1).field().source());
    }

    @Test
    void stillRejectsCaseInsensitiveExplicitAliasCollisionsInEitherOrder() {
        String longName = longField(0);
        DynamicForm form = form("collisions", "id", longName);
        JoinQuerySpec.Builder explicitFirst = JoinQuerySpec.builder(form);
        explicitFirst.selectAs(explicitFirst.root(), "id", "S0_F1");
        assertThrows(IllegalArgumentException.class,
                () -> explicitFirst.select(explicitFirst.root(), longName));
        assertEquals(List.of("S0_F1"), aliases(explicitFirst.build()));

        JoinQuerySpec.Builder generatedFirst = JoinQuerySpec.builder(form);
        generatedFirst.select(generatedFirst.root(), longName);
        assertThrows(IllegalArgumentException.class,
                () -> generatedFirst.selectAs(generatedFirst.root(), "id", "S0_F1"));
        assertThrows(IllegalArgumentException.class,
                () -> generatedFirst.select(generatedFirst.root(), longName.toUpperCase(Locale.ROOT)));
        assertEquals(List.of("s0_f1"), aliases(generatedFirst.build()));
    }

    @Test
    void stillRejectsFallbackCollisionsWithNaturalShortAliases() {
        String longName = longField(0);
        DynamicForm form = form("natural_collisions", "id", longName, "f1");
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        builder.select(builder.root(), longName);

        assertThrows(IllegalArgumentException.class, () -> builder.select(builder.root(), "f1"));
        assertEquals(List.of("s0_f1"), aliases(builder.build()));
    }

    @Test
    void preservesEqualSourceAcceptanceAndRejectsForeignSourcesAndUnknownFields() {
        String longName = longField(0);
        DynamicForm form = form("sources", "id", longName, longField(1));
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource equalSource = new JoinSource(builder.root().ordinal(), form);
        JoinSource foreign = new JoinSource(0, form("foreign", "id", longName));

        builder.select(equalSource, longName).select(builder.root(), longField(1));
        assertThrows(IllegalArgumentException.class, () -> builder.select(foreign, longName));
        assertThrows(IllegalArgumentException.class, () -> builder.select(builder.root(), "missing"));

        JoinQuerySpec query = builder.build();
        assertEquals(List.of("s0_f1", "s0_f2"), aliases(query));
        assertSame(equalSource, query.projections().get(0).field().source());
        assertSame(builder.root(), query.projections().get(1).field().source());
    }

    private static List<String> aliases(JoinQuerySpec query) {
        return query.projections().stream().map(JoinProjection::alias).toList();
    }

    private static DynamicForm form(String table, String... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(table, table);
        for (String field : fields) {
            builder.addField(DynamicField.of(field, "BIGINT"));
        }
        return builder.build();
    }

    private static String longField(int index) {
        return "column_" + "x".repeat(23) + (100 + index);
    }
}
