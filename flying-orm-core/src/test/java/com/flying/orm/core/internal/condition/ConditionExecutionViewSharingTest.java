package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionExecutionViewSharingTest {
    @Test
    void compiledMembershipRetainsTheOwnedListWithoutMaterializingItAgain() {
        List<Object> owned = List.of(1, 2, 3);
        ConditionExecutionView view = ConditionExecutionViews.compile(LogicalOperator.AND,
                List.of(new TermCondition("id", "in", owned)), term -> owned);
        assertSame(owned, view.parameterSources());
        assertEquals(3, view.parameterCount());
        assertThrows(UnsupportedOperationException.class, () -> view.parameterSources().add(4));
    }

    @Test
    void concatenationTakesOwnershipOfItsPrivateSegmentDirectory() {
        List<List<Object>> segments = new ArrayList<>(List.of(List.of(1), List.of(2))) {
            @Override
            public Object[] toArray() {
                throw new AssertionError("private segment directory does not need a publication copy");
            }
        };
        List<Object> combined = ConditionExecutionView.concatenate(segments);
        assertEquals(List.of(1, 2), combined);
        assertThrows(UnsupportedOperationException.class, () -> combined.add(3));
    }

    @Test
    void segmentedTraversalUsesLeafBulkTraversalAndPreservesEveryOccurrence() {
        List<Object> leaf = new ArrayList<>(List.of(1, 2)) {
            @Override
            public Iterator<Object> iterator() {
                throw new AssertionError("bulk traversal must not wrap each leaf in an iterator");
            }
        };
        List<Object> nested = ConditionExecutionView.concatenate(List.of(leaf, List.of(3)));
        List<Object> root = ConditionExecutionView.concatenate(List.of(List.of(0), nested, leaf));
        List<Object> actual = new ArrayList<>();
        root.forEach(actual::add);
        assertEquals(List.of(0, 1, 2, 3, 1, 2), actual);
    }
}
