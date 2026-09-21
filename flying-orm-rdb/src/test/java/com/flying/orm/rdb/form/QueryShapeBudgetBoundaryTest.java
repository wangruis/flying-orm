package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryShapeBudgetBoundaryTest {

    @Test
    void acceptsTheExactLimitAndRejectsTheNextItem() {
        QueryShapeBudget budget = new QueryShapeBudget(
                QueryShapeLimits.existingDefaults()
                                .withMaxProjectionCount(2)
                                .withMaxJoinCount(1)
                                .withMaxGroupCount(1)
                                .withMaxAggregateCount(1)
                                .withMaxHavingCount(1)
                                .withMaxSortCount(1)
                                .withMaxBindCount(2)
                                .withMaxSqlLength(16));

        assertDoesNotThrow(() -> {
            budget.addProjections(2);
            budget.addJoins(1);
            budget.addGroups(1);
            budget.addAggregates(1);
            budget.addHavingNodes(1);
            budget.addSorts(1);
            budget.addBinds(2);
            budget.addSqlLength(16);
        });

        assertThrows(IllegalArgumentException.class, () -> budget.addProjections(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addJoins(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addGroups(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addAggregates(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addHavingNodes(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addSorts(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addBinds(1));
        assertThrows(IllegalArgumentException.class, () -> budget.addSqlLength(1));
    }

    @Test
    void defaultsDoNotNarrowLegacyQueries() {
        QueryShapeBudget budget = new QueryShapeBudget(QueryShapeLimits.existingDefaults());

        assertDoesNotThrow(() -> {
            budget.addProjections(100_000);
            budget.addJoins(100_000);
            budget.addGroups(100_000);
            budget.addAggregates(100_000);
            budget.addHavingNodes(100_000);
            budget.addSorts(100_000);
            budget.addBinds(100_000);
            budget.addSqlLength(1_000_000);
        });
    }

    @Test
    void zeroCanExplicitlyProhibitAnOptionalQueryShape() {
        QueryShapeBudget budget = new QueryShapeBudget(
                QueryShapeLimits.existingDefaults().withMaxJoinCount(0));

        assertDoesNotThrow(() -> budget.addJoins(0));
        assertThrows(IllegalArgumentException.class, () -> budget.addJoins(1));
    }
}
