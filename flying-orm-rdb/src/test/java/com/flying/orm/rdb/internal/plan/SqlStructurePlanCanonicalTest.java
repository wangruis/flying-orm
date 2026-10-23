package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStructurePlanCanonicalTest {

    @Test
    void weighsSqlAndTransportTextWithoutRetiredParameterSlots() {
        String sql = "select * from events where id in (" + "?, ".repeat(1999) + "?)";
        SqlPlanSpec spec = new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL,
                "events-v1", RelationIdentity.table("events"), "select", List.of("id"), "in-2000", "", "", "");
        for (CacheRegionPolicy policy : List.of(CacheRegionPolicy.sqlPlanDefaults(),
                CacheRegionPolicy.disabled(), new CacheRegionPolicy(true, 1000, 10,
                        java.time.Duration.ofMinutes(1), true))) {
            StructuralPlanCaches caches = StructuralPlanCaches.create(
                    OrmCachePolicy.builder().sqlPlans(policy).build());
            AtomicInteger compilations = new AtomicInteger();
            java.util.function.Supplier<SqlStructurePlan> compiler = () -> {
                compilations.incrementAndGet();
                return SqlStructurePlan.sequential(sql, "postgresql", SqlBindMarkerStyle.CANONICAL,
                        "select", RelationIdentity.table("events"), 2000);
            };
            SqlStructurePlan first = caches.sqlPlan(spec, compiler);
            SqlStructurePlan second = caches.sqlPlan(spec, compiler);
            assertEquals(2000, second.parameterCount());
            assertTrue(second.statement().transportSql("postgresql").orElseThrow().endsWith("$2000)"));
            if (policy.equals(CacheRegionPolicy.sqlPlanDefaults())) {
                assertSame(first, second);
                assertEquals(1, compilations.get());
                assertEquals(1, caches.sqlSnapshot().estimatedSize());
                assertEquals(0, caches.sqlSnapshot().rejectedOversizedCount());
            } else {
                assertEquals(2, compilations.get());
                assertEquals(0, caches.sqlSnapshot().estimatedSize());
                assertEquals(policy.enabled() ? 2 : 0, caches.sqlSnapshot().rejectedOversizedCount());
            }
        }
    }

    @Test
    void cacheOnlyPlanDetailsStayPackageLocal() throws Exception {
        for (String name : List.of("sql", "operation", "table")) {
            assertFalse(Modifier.isPublic(SqlStructurePlan.class.getDeclaredMethod(name).getModifiers()), name);
        }
    }

    @Test
    void compilesSequentialPlansBeforeTheyEnterTheCache() {
        SqlStructurePlan plan = SqlStructurePlan.sequential(
                "select ?", "postgresql", SqlBindMarkerStyle.CANONICAL,
                "select", RelationIdentity.table("events"), 1);

        assertEquals("select ?", plan.sql());
        assertTrue(plan.statement().prepared());
        assertEquals("select $1", plan.statement().transportSql("postgresql").orElseThrow());
    }
}
