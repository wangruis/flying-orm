package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.internal.condition.ConditionExecutionView;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ConditionExecutionViewScaleTest {

    @Test
    void deepGroupsWithParametersAtEveryLevelShareTheirCachedParameterSources() {
        int depth = 12_000;
        ConditionGroup group = ConditionGroup.and().where("id", "=", 0).build();
        for (int level = 1; level < depth; level++) {
            group = ConditionGroup.and().add(group).where("id", "=", level).build();
        }

        ConditionExecutionView view = group.executionView();
        assertEquals(depth, view.parameterCount());
        List<Object> parameters = ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard());
        assertEquals(depth, parameters.size());
        for (int index = 0; index < depth; index++) {
            assertEquals(index, parameters.get(index));
        }
        assertSame(view, group.executionView());
    }

    @Test
    void sharedSegmentsKeepEveryOccurrenceAndMixedTermOrderWithEmptyGroups() {
        ConditionGroup shared = ConditionGroup.and()
                .where("id", "in", List.of(2, 3)).where("optional", "is-null", null).build();
        ConditionGroup group = ConditionGroup.and().where("id", "=", 1)
                .add(shared).add(ConditionGroup.and().build())
                .where("id", "=", 4).add(shared).where("id", "=", 5).build();

        assertEquals(List.of(1, 2, 3, 4, 2, 3, 5),
                ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard()));
        assertEquals(7, group.executionView().parameterCount());
    }

    @Test
    void sharingCachedSourcesDoesNotShareMutableBoundValues() {
        ConditionGroup shared = ConditionGroup.and().where("payload", "=", new byte[]{1}).build();
        ConditionGroup group = ConditionGroup.and().add(shared).add(shared).build();

        List<Object> first = ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard());
        List<Object> second = ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard());
        byte[] modified = (byte[]) first.getFirst();
        assertNotSame(modified, first.get(1));
        assertNotSame(modified, second.getFirst());
        modified[0] = 9;
        assertArrayEquals(new byte[]{1}, (byte[]) first.get(1));
        assertArrayEquals(new byte[]{1}, (byte[]) second.getFirst());
    }

    @Test
    void bindingSharedSegmentsInvokesCodecForEveryOccurrenceOnEveryExecution() {
        ConditionGroup shared = ConditionGroup.and().where("id", "=", 7).build();
        ConditionGroup group = ConditionGroup.and().add(shared).add(shared).build();
        AtomicInteger calls = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Integer.class;
            }

            @Override
            public Object write(Object value) {
                return calls.incrementAndGet();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });

        assertEquals(List.of(1, 2), ConditionExecutionViews.bindParameters(group, codecs));
        assertEquals(List.of(3, 4), ConditionExecutionViews.bindParameters(group, codecs));
    }
}
