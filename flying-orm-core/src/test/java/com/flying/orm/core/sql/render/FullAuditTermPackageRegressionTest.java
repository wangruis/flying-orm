package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FullAuditTermPackageRegressionTest {

    @Test
    void builtInEqualityHandlerCanBePackagedAndRendered() {
        SqlTermHandler handler = SqlTermHandler.equalsTo();

        SqlTermPackage termPackage = SqlTermPackage.of("equality", handler);

        assertEquals(1, termPackage.handlers().size());
        assertSame(handler, termPackage.handlers().get(0));
        assertEquals(handler.shape(), termPackage.terms().handler("=").shape());
        assertEqualityRendering(termPackage);
    }

    @Test
    void defaultHandlersCanBePackagedThroughIterableFactory() {
        List<SqlTermHandler> handlers = SqlTermHandler.defaults();

        SqlTermPackage termPackage = SqlTermPackage.of("standard", handlers);

        assertEquals(handlers, termPackage.handlers());
        for (SqlTermHandler handler : handlers) {
            assertEquals(handler.shape(), termPackage.terms().handler(handler.id()).shape());
        }
        assertEqualityRendering(termPackage);
    }

    @Test
    void forgedStandardIdRemainsRejectedByVarargsFactory() {
        SqlTermHandler forged = forgedEquality();

        assertThrows(IllegalArgumentException.class,
                () -> SqlTermPackage.of("forged", forged));
    }

    @Test
    void forgedStandardIdRemainsRejectedByIterableFactory() {
        SqlTermHandler forged = forgedEquality();

        assertThrows(IllegalArgumentException.class,
                () -> SqlTermPackage.of("forged", List.of(forged)));
    }

    private static void assertEqualityRendering(SqlTermPackage termPackage) {
        ConditionGroup where = ConditionGroup.and(termPackage.terms())
                .where("status", "=", 7)
                .build();
        SqlRenderer renderer = SqlRenderer.builder().addTermPackage(termPackage).build();

        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("status = ?", fragment.sql());
        assertEquals(List.of(7), fragment.parameters());
    }

    private static SqlTermHandler forgedEquality() {
        return SqlTermHandler.of("=", (term, context) -> new SqlFragment("1 = 1", List.of()));
    }
}
