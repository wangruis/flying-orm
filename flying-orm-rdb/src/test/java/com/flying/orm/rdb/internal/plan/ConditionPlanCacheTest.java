package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConditionPlanCacheTest {

    @Test
    void disabledCacheCompilesConditionWithoutAnalyzingItFirst() {
        AtomicInteger writes = new AtomicInteger();
        ConditionPlanCache cache = ConditionPlanCache.create(CacheRegionPolicy.disabled());
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .withValueCodecs(ValueCodecRegistry.standard()
                                                                                 .withFirst(new CountingStringCodec(
                                                                                         writes)));
        ConditionGroup where = renderer.conditions().where("name", "=", "flying").build();

        ConditionStructurePlan plan = cache.condition("postgresql", where, renderer);

        assertEquals(List.of("flying"), plan.parameters());
        assertEquals(1, writes.get());
        assertFalse(plan.cacheable());
    }

    @Test
    void isolatesOwnedAstValuesBeforeCallingAMutatingCodec() {
        ConditionPlanCache cache = ConditionPlanCache.create(
                OrmCachePolicy.safeDefaults().conditionPlans());
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addDefaultTerms()
                                          .build()
                                          .withValueCodecs(ValueCodecRegistry.standard()
                                                                                 .withFirst(new MutatingBytesCodec()));
        ConditionGroup where = renderer.conditions().where("payload", "=", new byte[]{1}).build();

        ConditionStructurePlan first = cache.condition("postgresql", where, renderer);
        ConditionStructurePlan second = cache.condition("postgresql", where, renderer);

        assertEquals(2, ((Number) first.parameters().getFirst()).intValue());
        assertEquals(first.parameters(), second.parameters());
    }

    @Test
    void reusesOnlyStructureAndExtractsEachRequestsOwnParameters() {
        ConditionPlanCache cache = ConditionPlanCache.create(
                OrmCachePolicy.safeDefaults().conditionPlans());
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ConditionGroup firstWhere = renderer.conditions().where("age", ">", 18).build();
        ConditionGroup secondWhere = renderer.conditions().where("age", ">", 21).build();

        ConditionStructurePlan first = cache.condition("postgresql", firstWhere, renderer);
        ConditionStructurePlan second = cache.condition("postgresql", secondWhere, renderer);

        assertEquals(first.plan().sql(), second.plan().sql());
        assertEquals(java.util.List.of(18), first.parameters());
        assertEquals(java.util.List.of(21), second.parameters());
        assertEquals(first.shape(), second.shape());
        assertEquals(1L, cache.snapshot().hitCount());
    }

    @Test
    void keepsCustomHandlerParameterSemanticsForCustomOperatorIds() {
        ConditionPlanCache cache = ConditionPlanCache.create(
                OrmCachePolicy.safeDefaults().conditionPlans());
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTerm(SqlTermHandler.of("custom_equals", (term, context) -> new SqlFragment(
                                                  context.identifier(term.field())
                                                          + ("42".equals(term.value()) ? " = ?" : " <> ?"),
                                                  List.of(context.parameter("prefix:" + term.value())))))
                                          .build();

        ConditionStructurePlan first = cache.condition(
                "postgresql", renderer.conditions().where("code", "custom_equals", "42").build(), renderer);
        ConditionStructurePlan second = cache.condition(
                "postgresql", renderer.conditions().where("code", "custom_equals", "43").build(), renderer);

        assertEquals(List.of("prefix:42"), first.parameters());
        assertEquals(List.of("prefix:43"), second.parameters());
        assertEquals("code = ?", first.plan().sql());
        assertEquals("code <> ?", second.plan().sql());
        assertFalse(first.cacheable());
        assertFalse(second.cacheable());
    }

    private static final class MutatingBytesCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == byte[].class;
        }

        @Override
        public Object write(Object value) {
            byte[] bytes = (byte[]) value;
            return ++bytes[0];
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }

    private record CountingStringCodec(AtomicInteger writes) implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            writes.incrementAndGet();
            return value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }
}
