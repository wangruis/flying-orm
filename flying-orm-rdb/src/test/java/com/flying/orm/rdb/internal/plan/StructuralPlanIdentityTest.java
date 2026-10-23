package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class StructuralPlanIdentityTest {
    @Test
    void literalRelationInvalidationDoesNotFlushUnrelatedPlans() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        RelationIdentity literal = RelationIdentity.table("sales.items");
        RelationIdentity qualified = RelationIdentity.parse("sales.items");
        SqlPlanSpec first = spec(literal, "literal");
        SqlPlanSpec second = spec(qualified, "qualified");
        SqlStructurePlan beforeFirst = load(caches, first);
        SqlStructurePlan beforeSecond = load(caches, second);
        caches.invalidate(literal);
        assertNotSame(beforeFirst, load(caches, first));
        assertSame(beforeSecond, load(caches, second));
    }

    private static SqlPlanSpec spec(RelationIdentity relation, String fingerprint) {
        return new SqlPlanSpec("postgresql", SqlBindMarkerStyle.CANONICAL, fingerprint,
                relation, "select", List.of("id"), "", "", "", "");
    }

    private static SqlStructurePlan load(StructuralPlanCaches caches, SqlPlanSpec spec) {
        return caches.sqlPlan(spec, () -> SqlStructurePlan.sequential(
                "select id from items", "postgresql", SqlBindMarkerStyle.CANONICAL, "select", spec.table(), 0));
    }
}
