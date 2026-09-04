package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTermPackageOwnershipTest {

    @Test
    void packageRecordPublishesItsExclusiveFactoryListWithoutTraversingItAgain() {
        SqlTermHandler handler = handler();
        CountingList<SqlTermHandler> handlers = new CountingList<>(List.of(handler));

        SqlTermPackage termPackage = new SimpleSqlTermPackage(
                "filters", handlers, TermRegistry.builder().add(TermHandler.simple("custom-filter")).build());

        assertEquals(0, handlers.reads());
        assertThrows(UnsupportedOperationException.class, () -> termPackage.handlers().clear());
        assertEquals(List.of(handler), termPackage.handlers());
    }

    @Test
    void publicPackageFactoryStillSnapshotsMutableInputOnce() {
        SqlTermHandler handler = handler();
        List<SqlTermHandler> source = new ArrayList<>(List.of(handler));

        SqlTermPackage termPackage = SqlTermPackage.of("filters", source);
        source.clear();

        assertEquals(List.of(handler), termPackage.handlers());
        assertThrows(UnsupportedOperationException.class, () -> termPackage.handlers().clear());
    }

    private static SqlTermHandler handler() {
        return SqlTermHandler.of(
                "custom-filter",
                ConditionValueShape.SCALAR,
                (term, context) -> new SqlFragment("1 = 1", List.of()));
    }

    private static final class CountingList<E> extends AbstractList<E> {

        private final List<E> values;

        private int reads;

        private CountingList(List<E> values) {
            this.values = values;
        }

        @Override
        public E get(int index) {
            reads++;
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }

        private int reads() {
            return reads;
        }
    }
}
