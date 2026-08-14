package com.flying.orm.rdb.internal.plan;

import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuralPlanCachesTest {

    @Test
    void doesNotEchoMalformedRuntimeTableNameFromPlanKey() {
        String table = "tenant.users.extra--must-not-leak";
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new SqlPlanSpec("mysql", SqlBindMarkerStyle.CANONICAL, "form", table,
                                     "select", List.of(), "", "", "", ""));

        assertFalse(error.getMessage().contains(table));
    }

    /** 运行时失效入口的畸形标识符也不能写入异常消息。 */
    @Test
    void doesNotEchoMalformedRuntimeIdentifierFromInvalidation() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        String schema = "tenant.secret.extra--must-not-leak";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> caches.invalidate(schema, "users"));

        assertFalse(error.getMessage().contains(schema));
    }

    @Test
    void reusesShapePlanWithoutCompilingFullSqlOnHitAndInvalidatesExactTableIdentity() {
        OrmCachePolicy policy = OrmCachePolicy.builder()
                .sqlPlans(new CacheRegionPolicy(true, 32, 16, Duration.ofMinutes(1), true))
                .build();
        StructuralPlanCaches caches = StructuralPlanCaches.create(policy);
        AtomicInteger compiles = new AtomicInteger();
        SqlPlanSpec publicUsers = new SqlPlanSpec("mysql",
                                                  SqlBindMarkerStyle.CANONICAL,
                                                  "form-fingerprint",
                                                  "public.users",
                                                  "select",
                                                  List.of("id", "name"),
                                                  "and(id:=:1)",
                                                  "",
                                                  "id:asc",
                                                  "");

        SqlStructurePlan firstPlan = caches.sqlPlan(publicUsers, () -> {
            compiles.incrementAndGet();
            return SqlStructurePlan.sequential("select id, name from public.users where id = ?",
                                               "select",
                                               "public.users",
                                               List.of("id", "name"),
                                               1);
        });
        SqlStructurePlan secondPlan = caches.sqlPlan(publicUsers, () -> {
            throw new AssertionError("cache hit must not compile full SQL");
        });
        assertSame(firstPlan, secondPlan);
        assertEquals(1, compiles.get());
        assertArrayEquals(new int[]{0}, firstPlan.parameterSlots());
        assertEquals(1, caches.sqlSnapshot().estimatedSize());

        SqlPlanSpec tenantUsers = new SqlPlanSpec("mysql",
                                                  SqlBindMarkerStyle.CANONICAL,
                                                  "form-fingerprint",
                                                  "tenant_a.users",
                                                  "select",
                                                  List.of("id", "name"),
                                                  "and(id:=:1)",
                                                  "",
                                                  "id:asc",
                                                  "");
        caches.sqlPlan(tenantUsers, () -> SqlStructurePlan.sequential(
                "select id, name from tenant_a.users where id = ?",
                "select", "tenant_a.users", List.of("id", "name"), 1));

        caches.invalidate("public", "users");
        assertEquals(1, caches.sqlSnapshot().estimatedSize());
        caches.invalidate("users");
        assertEquals(0, caches.sqlSnapshot().estimatedSize());
    }

    @Test
    void reusesConditionStructureWithoutRetainingRequestValues() {
        OrmCachePolicy policy = OrmCachePolicy.builder()
                .conditionPlans(new CacheRegionPolicy(true, 64, 32, Duration.ofMinutes(1), true))
                .build();
        StructuralPlanCaches caches = StructuralPlanCaches.create(policy);
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ConditionGroup first = ConditionGroup.and()
                                             .where("tenant_id", "=", "tenant-a")
                                             .where("id", "in", List.of(1, 2))
                                             .build();
        ConditionGroup second = ConditionGroup.and()
                                              .where("tenant_id", "=", "tenant-b")
                                              .where("id", "in", List.of(8, 9))
                                              .build();

        ConditionStructurePlan firstPlan = caches.condition("mysql", first, renderer);
        ConditionStructurePlan secondPlan = caches.condition("mysql", second, renderer);

        assertSame(firstPlan.plan(), secondPlan.plan());
        assertNotSame(firstPlan.parameters(), secondPlan.parameters());
        assertEquals(List.of("tenant-a", 1, 2), firstPlan.parameters());
        assertEquals(List.of("tenant-b", 8, 9), secondPlan.parameters());
        assertEquals(1L, caches.conditionSnapshot().loadSuccessCount());
    }

    /** 忽略大小写的 LIKE 仍是稳定标量结构，应复用 SQL 计划但不能保留上次请求值。 */
    @Test
    void reusesCaseInsensitiveLikeStructureWithoutRetainingRequestValues() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ConditionGroup first = ConditionGroup.and()
                                             .where("name", "like-ignore-case", "%Alice%")
                                             .build();
        ConditionGroup second = ConditionGroup.and()
                                              .where("name", "like-ignore-case", "%Bob%")
                                              .build();

        ConditionStructurePlan firstPlan = caches.condition("postgresql", first, renderer);
        ConditionStructurePlan secondPlan = caches.condition("postgresql", second, renderer);

        assertSame(firstPlan.plan(), secondPlan.plan());
        assertEquals(List.of("%Alice%"), firstPlan.parameters());
        assertEquals(List.of("%Bob%"), secondPlan.parameters());
        assertEquals(1L, caches.conditionSnapshot().loadSuccessCount());
    }

    @Test
    void isolatesConditionPlansOwnedByDifferentRendererConfigurations() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        SqlRenderer plain = SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build();
        SqlRenderer lowerCase = SqlRenderer.builder()
                .addTerm(SqlTermHandler.of("=", (term, context) -> SqlFragment.of(
                        "lower(" + context.identifier(term.field()) + ") = lower(?)", term.value())))
                .build();

        ConditionStructurePlan plainPlan = caches.condition(
                "h2", ConditionGroup.and(plain.terms()).where("name", "=", "A").build(), plain);
        ConditionStructurePlan lowerPlan = caches.condition(
                "h2", ConditionGroup.and(lowerCase.terms()).where("name", "=", "B").build(), lowerCase);

        assertNotSame(plainPlan.plan(), lowerPlan.plan());
        assertEquals("name = ?", plainPlan.plan().sql());
        assertEquals("lower(name) = lower(?)", lowerPlan.plan().sql());
    }

    @Test
    void keepsQuotedPhysicalTableCaseInThePlanIdentity() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        SqlPlanSpec upper = new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL, "upper",
                                            "Users", "select", List.of("id"), "", "", "", "");
        SqlPlanSpec lower = new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL, "lower",
                                            "users", "select", List.of("id"), "", "", "", "");

        SqlStructurePlan upperPlan = caches.sqlPlan(upper, () -> SqlStructurePlan.sequential(
                "select id from \"Users\"", "select", "Users", List.of("id"), 0));
        SqlStructurePlan lowerPlan = caches.sqlPlan(lower, () -> SqlStructurePlan.sequential(
                "select id from users", "select", "users", List.of("id"), 0));

        assertNotSame(upperPlan, lowerPlan);
        assertEquals("select id from \"Users\"", upperPlan.sql());
        assertEquals("select id from users", lowerPlan.sql());
    }

    @Test
    void consumesOneShotMultiValueIterableOnlyOnce() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        Iterable<Integer> oneShot = new Iterable<>() {
            private final Iterator<Integer> values = List.of(7, 9).iterator();

            @Override
            public Iterator<Integer> iterator() {
                return values;
            }
        };
        ConditionGroup where = ConditionGroup.and()
                                             .add(TermCondition.of("id", "in", oneShot))
                                             .build();

        ConditionStructurePlan plan = caches.condition(
                "postgresql", where, SqlRenderer.builder().addDefaultTerms().build());

        assertEquals("id in (?, ?)", plan.plan().sql());
        assertEquals(List.of(7, 9), plan.parameters());
    }
}
