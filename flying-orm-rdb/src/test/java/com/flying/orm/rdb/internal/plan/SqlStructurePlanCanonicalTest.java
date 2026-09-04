package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStructurePlanCanonicalTest {

    @Test
    void compilesSequentialPlansBeforeTheyEnterTheCache() {
        SqlStructurePlan plan = SqlStructurePlan.sequential(
                "select ?", "postgresql", SqlBindMarkerStyle.CANONICAL,
                "select", "events", 1);

        assertEquals("select ?", plan.sql());
        assertTrue(plan.statement().prepared());
        assertEquals("select $1", plan.statement().transportSql("postgresql").orElseThrow());
    }
}
