package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStructurePlanCanonicalTest {

    @Test
    void cacheOnlyPlanDetailsStayPackageLocal() throws Exception {
        for (String name : List.of("sql", "operation", "table")) {
            assertFalse(Modifier.isPublic(SqlStructurePlan.class.getDeclaredMethod(name).getModifiers()), name);
        }
    }

    @Test
    void cacheLabelPartsStayPackageLocal() throws Exception {
        for (String name : List.of("schema", "physicalTable")) {
            assertFalse(Modifier.isPublic(SqlPlanSpec.class.getDeclaredMethod(name).getModifiers()), name);
        }
    }

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
