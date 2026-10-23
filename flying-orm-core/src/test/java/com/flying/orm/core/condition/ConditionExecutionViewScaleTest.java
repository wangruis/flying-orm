package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.internal.condition.ConditionExecutionView;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import com.flying.orm.core.internal.hash.StableDigest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ConditionExecutionViewScaleTest {

    @Test
    void conditionShapeUsesCompactFramedSegmentsWithoutParameterValues() {
        ConditionGroup first = ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 100L).build();
        ConditionGroup second = ConditionGroup.and().where("id", ">=", 2L).where("id", "<=", 200L).build();
        String expected = StableDigest.sha256(StableDigest.domain("condition-plan-shape/v2"))
                .marker("AND").text("F", "id").integer(">=", 1)
                .text("F", "id").integer("<=", 1).marker("END").finishHex();
        assertEquals(expected, first.executionView().shapeDigest());
        assertEquals(expected, second.executionView().shapeDigest());
        assertEquals(List.of(1L, 100L), ConditionExecutionViews.bindParameters(first, ValueCodecRegistry.standard()));
        assertEquals(List.of(2L, 200L), ConditionExecutionViews.bindParameters(second, ValueCodecRegistry.standard()));
    }

    @Test
    void conditionShapeKeepsGroupOperatorFieldOperatorAndArityDistinct() {
        List<ConditionGroup> shapes = List.of(
                ConditionGroup.and().where("id", "=", 1).build(),
                ConditionGroup.or().where("id", "=", 1).build(),
                ConditionGroup.and().where("id2", "=", 1).build(),
                ConditionGroup.and().where("id", ">", 1).build(),
                ConditionGroup.and().where("id", "in", List.of(1)).build(),
                ConditionGroup.and().where("id", "in", List.of(1, 2)).build(),
                ConditionGroup.and().where("id", "between", List.of(1, 2)).build(),
                ConditionGroup.and().add(ConditionGroup.and().where("id", "=", 1).build()).build(),
                ConditionGroup.and().build(),
                ConditionGroup.or().build());
        for (int left = 0; left < shapes.size(); left++) {
            for (int right = left + 1; right < shapes.size(); right++) {
                assertNotEquals(shapes.get(left).executionView().shapeDigest(),
                        shapes.get(right).executionView().shapeDigest());
            }
        }
    }

    @Test
    void nestedShapeRetainsFramedUnicodeFieldsAndChildDigest() {
        ConditionGroup child = ConditionGroup.and().where("名称", "=", "张三").build();
        String childDigest = StableDigest.sha256(StableDigest.domain("condition-plan-shape/v2"))
                .marker("AND").text("F", "名称").integer("=", 1).marker("END").finishHex();
        String parentDigest = StableDigest.sha256(StableDigest.domain("condition-plan-shape/v2"))
                .marker("OR").text("N", childDigest).marker("END").finishHex();
        assertEquals(childDigest, child.executionView().shapeDigest());
        ConditionGroup parent = ConditionGroup.or().add(child).build();
        assertEquals(parentDigest, parent.executionView().shapeDigest());
        assertEquals(List.of("张三"), ConditionExecutionViews.bindParameters(parent, ValueCodecRegistry.standard()));
    }

    @Test
    void flatAndPrecompiledChildViewsKeepTheSameShapeAndParameterOrder() {
        ConditionGroup flat = ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 100L).build();
        ConditionExecutionView direct = ConditionExecutionViews.compile(
                flat.operator(), flat.children(), TermCondition::value);
        assertEquals(direct.shapeDigest(), flat.executionView().shapeDigest());
        assertEquals(direct.cacheable(0), flat.executionView().cacheable(0));
        assertEquals(direct.cacheable(-1L), flat.executionView().cacheable(-1L));
        ConditionGroup pending = ConditionGroup.or().where("id", "=", 7L).where("id", "=", 8L).build();
        ConditionGroup mixed = ConditionGroup.and().add(flat).add(pending).add(flat).build();
        ConditionExecutionView view = mixed.executionView();
        assertSame(view, mixed.executionView());
        assertEquals(List.of(1L, 100L, 7L, 8L, 1L, 100L),
                ConditionExecutionViews.bindParameters(mixed, ValueCodecRegistry.standard()));
        ConditionGroup prepared = ConditionGroup.and().add(flat).add(pending).add(flat).build();
        assertEquals(view.shapeDigest(), prepared.executionView().shapeDigest());
        assertEquals(view.cacheable(0), prepared.executionView().cacheable(0));
        assertEquals(view.cacheable(-1L), prepared.executionView().cacheable(-1L));
    }

    @Test
    void concurrentFirstCompilationPublishesOneViewForFlatAndNestedGroups() throws Exception {
        ConditionGroup flat = ConditionGroup.and().where("id", "=", 1).build();
        ConditionGroup nested = ConditionGroup.and()
                .add(ConditionGroup.or().where("id", "=", 2).where("id", "=", 3).build()).build();
        try (java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            for (ConditionGroup group : List.of(flat, nested)) {
                java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
                List<java.util.concurrent.Future<ConditionExecutionView>> futures = new java.util.ArrayList<>();
                for (int index = 0; index < 4; index++) {
                    futures.add(executor.submit(() -> {
                        start.await();
                        return group.executionView();
                    }));
                }
                start.countDown();
                ConditionExecutionView expected = futures.getFirst().get(10, java.util.concurrent.TimeUnit.SECONDS);
                for (java.util.concurrent.Future<ConditionExecutionView> future : futures) {
                    assertSame(expected, future.get(10, java.util.concurrent.TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void membershipSegmentsKeepArrayAndListShapesEquivalent() {
        ConditionGroup list = ConditionGroup.and().where("id", "=", 0)
                .where("id", "in", List.of(1, 2)).where("id", "between", List.of(3, 4))
                .where("id", "=", 5).build();
        ConditionGroup array = ConditionGroup.and().where("id", "=", 0)
                .add(new TermCondition("id", "in", new int[]{1, 2}))
                .add(new TermCondition("id", "between", new int[]{3, 4}))
                .where("id", "=", 5).build();
        assertEquals(list.executionView().shapeDigest(), array.executionView().shapeDigest());
        assertEquals(List.of(0, 1, 2, 3, 4, 5),
                ConditionExecutionViews.bindParameters(array, ValueCodecRegistry.standard()));
        assertEquals(6, list.executionView().parameterCount());
    }

    @Test
    void membershipSegmentsKeepMutableValuesIsolatedAcrossBindings() {
        byte[] source = {1};
        ConditionGroup shared = ConditionGroup.and().where("payload", "in", List.of(source)).build();
        ConditionGroup group = ConditionGroup.and().where("id", "=", 7).add(shared).add(shared).build();
        source[0] = 9;
        List<Object> first = ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard());
        ((byte[]) first.get(1))[0] = 8;
        List<Object> second = ConditionExecutionViews.bindParameters(group, ValueCodecRegistry.standard());
        assertEquals(7, first.getFirst());
        assertArrayEquals(new byte[]{1}, (byte[]) first.get(2));
        assertArrayEquals(new byte[]{1}, (byte[]) second.get(1));
        assertArrayEquals(new byte[]{1}, (byte[]) second.get(2));
    }

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
