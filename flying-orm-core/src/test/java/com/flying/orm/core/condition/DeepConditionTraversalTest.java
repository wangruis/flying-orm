package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.condition.ConditionExecutionView;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepConditionTraversalTest {

    private static final int DEPTH = 10_000;

    @Test
    void rendersDeepTrustedConditionsWithoutDependingOnTheThreadStack() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();

        SqlFragment fragment = renderer.renderWhere(deepCondition());

        assertEquals("(".repeat(DEPTH) + "id = ?" + ")".repeat(DEPTH), fragment.sql());
        assertEquals(List.of(7), fragment.parameters());
    }

    @Test
    void compilesDeepExecutionViewsWithoutDependingOnTheThreadStack() {
        ConditionGroup group = deepCondition();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();

        ConditionExecutionView view = group.executionView();

        assertEquals(1, view.parameterCount());
        assertTrue(view.cacheable(renderer.standardConditionTermMask()));
        assertEquals(List.of(7), ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard()));
    }

    @Test
    void renderingKeepsEmptySubtreeRollbackAndExtensionPrecedence() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("custom", ConditionValueShape.SCALAR,
                        (term, context) -> new SqlFragment(context.identifier(term.field())
                                + " = ? or enabled = ?", List.of(term.value(), true))))
                .build();
        ConditionGroup empty = ConditionGroup.and().add(ConditionGroup.or().build()).build();
        ConditionGroup group = ConditionGroup.and(renderer.terms())
                .add(empty).where("head", "=", 1).add(empty)
                .or(nested -> nested.add(empty).where("id", "custom", 7)
                        .add(empty).where("tail", "=", 9))
                .add(empty).where("last", "=", 11).add(empty).build();

        SqlFragment fragment = renderer.renderWhere(group);

        assertEquals("head = ? and ((id = ? or enabled = ?) or tail = ?) and last = ?", fragment.sql());
        assertEquals(List.of(1, 7, true, 9, 11), fragment.parameters());
        assertEquals("", renderer.renderWhere(empty).sql());
    }

    @Test
    void executionViewsStillPublishAndReuseSharedSubtreeCaches() throws ReflectiveOperationException {
        ConditionGroup shared = deepCondition();
        ConditionGroup root = ConditionGroup.and().add(shared).add(shared).build();

        ConditionExecutionView rootView = root.executionView();

        Field cache = ConditionGroup.class.getDeclaredField("executionView");
        cache.setAccessible(true);
        assertNotNull(cache.get(shared));
        assertSame(cache.get(shared), shared.executionView());
        assertSame(rootView, root.executionView());
        assertEquals(List.of(7, 7), ConditionExecutionViews.bindParameters(root, ValueCodecRegistry.standard()));
    }

    private static ConditionGroup deepCondition() {
        ConditionGroup group = ConditionGroup.and().where("id", "=", 7).build();
        for (int depth = 0; depth < DEPTH; depth++) {
            group = ConditionGroup.and().add(group).build();
        }
        return group;
    }
}
