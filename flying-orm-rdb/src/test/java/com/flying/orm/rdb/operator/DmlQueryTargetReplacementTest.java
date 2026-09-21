package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DmlQueryTargetReplacementTest {
    private static final SqlRenderer RENDERER = SqlRenderer.builder().addDefaultTerms().build();
    private static final DynamicForm FORM_A = form("a", "table_a");
    private static final DynamicForm FORM_B = form("b", "table_b");

    @Test
    void settingPhysicalTargetReplacesPreviouslySelectedForm() {
        DmlQueryCommand command = new DmlQueryCommand(RENDERER, DataScope.none());
        command.select("id");
        command.from(FORM_A, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
        command.from("table_b");
        assertFalse(command.governed(), "the explicit physical-table entry is trusted");
        assertTrue(command.toRequest().sql().contains("table_b"));
        assertFalse(command.toRequest().sql().contains("table_a"));
    }

    @Test
    void targetReplacementPreservesDefaultAndRequestScopes() {
        DmlQueryCommand command = new DmlQueryCommand(RENDERER, DataScope.tenant("tenant_id", "tenant-a"));
        command.select("id");
        command.scope(DataScope.self("owner_id", 7));
        command.from(FORM_A, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
        command.from("table_b");
        assertFalse(command.governed());
        var request = command.toRequest();
        assertTrue(request.sql().contains("table_b"));
        assertTrue(request.sql().contains("tenant_id"));
        assertTrue(request.sql().contains("owner_id"));
        assertEquals(List.of("tenant-a", 7), request.parameters());
    }

    @Test
    void selectingFormAfterPhysicalTargetStillEnablesItsPolicy() {
        DmlQueryCommand command = new DmlQueryCommand(RENDERER, DataScope.none());
        command.from("table_a");
        command.from(FORM_B, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
        assertTrue(command.governed());
        assertSame(FORM_B, command.governedQuery(null).spec().form());
    }

    @Test
    void selectingAnotherFormUsesItsOwnMetadata() {
        DmlQueryCommand command = new DmlQueryCommand(RENDERER, DataScope.none());
        command.from(FORM_A, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
        command.from(FORM_B, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
        assertTrue(command.governed());
        assertSame(FORM_B, command.governedQuery(null).spec().form());
    }

    private static DynamicForm form(String id, String table) {
        return DynamicForm.builder(id, table).addField(DynamicField.of("id", "INTEGER")).build();
    }
}
