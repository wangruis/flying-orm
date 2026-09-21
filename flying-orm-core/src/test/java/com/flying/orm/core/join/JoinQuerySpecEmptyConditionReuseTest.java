package com.flying.orm.core.join;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinQuerySpecEmptyConditionReuseTest {

    @Test
    void reusesTheImmutableEmptyConditionWithoutChangingDeclaredConditions() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        JoinQuerySpec.Builder emptyBuilder = JoinQuerySpec.builder(form);
        JoinSource root = emptyBuilder.root();
        JoinQuerySpec empty = emptyBuilder.select(root, "id").build();

        ConditionGroup first = empty.where(root);
        assertEquals(LogicalOperator.AND, first.operator());
        assertTrue(first.children().isEmpty());
        assertSame(first, empty.where(root));
        assertThrows(UnsupportedOperationException.class, () -> first.children().add(first));
        assertThrows(NullPointerException.class, () -> empty.where(null));

        ConditionGroup where = ConditionGroup.and().where("id", "=", 7L).build();
        JoinQuerySpec.Builder declaredBuilder = JoinQuerySpec.builder(form);
        JoinSource declaredRoot = declaredBuilder.root();
        JoinQuerySpec declared = declaredBuilder.where(declaredRoot, where).select(declaredRoot, "id").build();
        assertSame(where, declared.where(declaredRoot));
        assertSame(where, declared.where(declaredRoot));
    }
}
