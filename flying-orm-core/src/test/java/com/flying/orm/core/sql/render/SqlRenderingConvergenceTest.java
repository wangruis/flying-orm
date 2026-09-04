package com.flying.orm.core.sql.render;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlRenderingConvergenceTest {

    private static final int LARGE_BINARY_BYTES = 64 * 1_024;

    private static final long MAX_SINGLE_RENDER_ALLOCATION = 100_000L;

    @Test
    void validatesIdentifiersBeforeInvokingTheConfiguredRenderer() {
        AtomicInteger calls = new AtomicInteger();
        SqlRenderer renderer = SqlRenderer.builder().build().withIdentifierRenderer(name -> {
            calls.incrementAndGet();
            return name;
        });

        assertThrows(IllegalArgumentException.class, () -> renderer.identifier("unsafe;drop"));
        assertThrows(IllegalArgumentException.class, () -> renderer.structureIdentifier("unsafe;drop"));
        assertEquals(0, calls.get());
        assertEquals("safe_name", renderer.identifier(" safe_name "));
        assertEquals(1, calls.get());
    }

    @Test
    void defaultRendererStillRejectsUnsafeIdentifiers() {
        SqlRenderer renderer = SqlRenderer.builder().build();

        assertThrows(IllegalArgumentException.class, () -> renderer.identifier("unsafe;drop"));
    }

    @Test
    void projectionsKeepWildcardSemanticsAndUseTheFinalIdentifierBoundary() {
        SqlRenderer renderer = SqlRenderer.builder().build();

        assertEquals("*", renderer.projection(" * "));
        assertEquals("tenant.*", renderer.projection(" tenant.* "));
        assertThrows(IllegalArgumentException.class, () -> renderer.projection("unsafe;drop.*"));
        assertThrows(IllegalArgumentException.class, () -> renderer.projection("sum(value)"));
    }

    @Test
    void rendersNestedMutableTermsInStableOrderAndPublishesImmutableParameters() {
        List<Integer> states = new ArrayList<>(List.of(2, 3));
        int[] scoreRange = {4, 5};
        ConditionGroup where = ConditionGroup.and()
                                             .where("tenant_id", "=", 1)
                                             .or(or -> or.where("state", "in", states)
                                                         .where("score", "between", scoreRange))
                                             .build();
        states.set(0, 99);
        scoreRange[0] = 98;

        SqlFragment fragment = SqlRenderer.builder().addDefaultTerms().build().renderWhere(where);

        assertEquals("tenant_id = ? and (state in (?, ?) or score between ? and ?)", fragment.sql());
        assertEquals(List.of(1, 2, 3, 4, 5), fragment.parameters());
        assertThrows(UnsupportedOperationException.class, () -> fragment.parameters().add(6));
    }

    @Test
    void publicFragmentConstructionCopiesTheListButKeepsItsOriginalElementSemantics() {
        byte[] mutableParameter = {1, 2};
        List<Object> parameters = new ArrayList<>(List.of(mutableParameter));

        SqlFragment first = new SqlFragment(" value = ? ", parameters);
        parameters.clear();
        SqlFragment second = new SqlFragment(" value = ? ", List.of(mutableParameter));

        assertEquals("value = ?", first.sql());
        assertSame(mutableParameter, first.parameters().getFirst());
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertThrows(UnsupportedOperationException.class, () -> first.parameters().clear());
    }

    @Test
    void internalFragmentPublicationReusesItsExclusiveParameterList() {
        OwnedBindableValues.Buffer buffer = OwnedBindableValues.buffer(3);
        buffer.add(1);
        buffer.add(2);
        buffer.add(3);
        List<Object> parameters = buffer.publish();

        SqlFragment fragment = new SqlFragment("value in (?, ?, ?)", parameters);

        assertSame(parameters, fragment.parameters());
        assertThrows(UnsupportedOperationException.class, () -> fragment.parameters().add(4));
        assertEquals(List.of(1, 2, 3), fragment.parameters());
    }

    @Test
    void relationHandlerKeepsValidatedStringBehaviorWithoutAdditiveTokenApi() {
        SqlTermHandler handler = SqlTermHandler.relationExists(
                "member-of", "membership", "m", "member_id", "group_id");

        Class<?> relationTableType = Arrays.stream(handler.getClass().getRecordComponents())
                                           .filter(component -> component.getName().equals("relationTable"))
                                           .findFirst()
                                           .orElseThrow()
                                           .getType();

        assertEquals(String.class, relationTableType);
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.core.sql.render.ValidatedSqlIdentifier"));
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName("com.flying.orm.core.internal.InternalApi"));
        assertFalse(publicMethod(SqlIdentifiers.class, "validated"));
        assertFalse(publicMethod(SqlRenderer.class, "withValidatedIdentifierRenderer"));
        assertFalse(publicMethod(ConditionValueNormalizer.class, "normalizeOwned"));
        SqlRenderer renderer = SqlRenderer.builder().addTerm(handler).build();
        SqlFragment rendered = renderer.renderWhere(
                ConditionGroup.and(renderer.terms()).where("user_id", "member-of", 7).build());
        assertEquals(
                "exists (select 1 from membership m where m.member_id = user_id and m.group_id = ?)",
                rendered.sql());
        assertEquals(List.of(7), rendered.parameters());
        assertThrows(IllegalArgumentException.class,
                     () -> SqlTermHandler.relationExists(
                             "unsafe", "membership; drop table users", "m", "member_id", "group_id"));
    }

    @Test
    void builtInRenderingIsolatesAstValuesBeforeCallingAMutatingCodec() {
        ValueCodec mutatingCodec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == byte[].class || ByteBuffer.class.isAssignableFrom(targetType);
            }

            @Override
            public Object write(Object value) {
                if (value instanceof byte[] bytes) {
                    int encoded = Byte.toUnsignedInt(bytes[0]);
                    bytes[0]++;
                    return encoded;
                }
                return Byte.toUnsignedInt(((ByteBuffer) value).get());
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new UnsupportedOperationException();
            }
        };
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .valueCodecs(ValueCodecRegistry.standard().withFirst(mutatingCodec))
                                          .build();
        ConditionGroup where = ConditionGroup.and()
                                             .where("scalar_value", "=", new byte[]{1})
                                             .where("set_value", "in", List.of(
                                                     new byte[]{2},
                                                     new byte[]{3}))
                                             .where("range_value", "between", List.of(
                                                     ByteBuffer.wrap(new byte[]{4}),
                                                     ByteBuffer.wrap(new byte[]{5})))
                                             .build();

        SqlFragment first = renderer.renderWhere(where);
        SqlFragment second = renderer.renderWhere(where);

        assertEquals(List.of(1, 2, 3, 4, 5), first.parameters());
        assertEquals(first, second);
    }

    @Test
    void builtInRenderingCopiesAnOwnedBinaryValueOnlyOncePerRender() {
        java.lang.management.ThreadMXBean managementBean = java.lang.management.ManagementFactory.getThreadMXBean();
        assertTrue(managementBean instanceof com.sun.management.ThreadMXBean,
                   "the Java 21 runtime must expose per-thread allocation counters");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ConditionGroup where = ConditionGroup.and()
                                             .where("payload", "=", new byte[LARGE_BINARY_BYTES])
                                             .build();
        for (int iteration = 0; iteration < 100; iteration++) {
            renderer.renderWhere(where);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        SqlFragment fragment = renderer.renderWhere(where);
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

        assertEquals(1, fragment.parameters().size());
        assertTrue(allocated < MAX_SINGLE_RENDER_ALLOCATION,
                   () -> "rendering copied the already-owned binary value more than once: " + allocated + " bytes");
    }

    @Test
    void rendererUsesTheRegisteredHandlerAsTheOnlyTermAuthority() {
        SqlTermHandler handler = new SimpleSqlTermHandler(
                "=",
                ConditionValueShape.SCALAR,
                (term, context) -> new SqlFragment("authority", List.of(41)),
                true);
        SqlRenderer renderer = SqlRenderer.builder().addTerm(handler).build();
        TermCondition term = TermCondition.of("value", "=", 7);

        SqlFragment direct = handler.render(term, renderer);
        SqlFragment throughRenderer = renderer.renderWhere(
                ConditionGroup.and().where("value", "=", 7).build());

        assertEquals(direct, throughRenderer);
    }

    @Test
    void publicFactoryHandlersKeepTheBaselineFourComponentRecordEquality() {
        SqlTermRenderer sharedRenderer = (term, context) -> new SqlFragment("shared", List.of(7));

        SqlTermHandler first = SqlTermHandler.of("shared-term", sharedRenderer);
        SqlTermHandler second = SqlTermHandler.of("shared-term", sharedRenderer);
        SqlTermHandler differentShape = SqlTermHandler.of(
                "shared-term", ConditionValueShape.COLLECTION, sharedRenderer);

        assertNotSame(first, second);
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, differentShape);
    }

    private static boolean publicMethod(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .anyMatch(method -> method.getName().equals(name)
                             && Modifier.isPublic(method.getModifiers()));
    }
}
