package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.spec.WriteSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeepDmlWriteConditionTest {

    @Test
    void infersDynamicWriteFieldsFromDeepConditions() {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        ConditionGroup where = ConditionGroup.and().where("id", "=", 7).build();
        for (int level = 1; level < 10_000; level++) where = ConditionGroup.and().add(where).build();
        ConditionGroup conditions = where;
        DmlWriteCommand command = DmlWriteCommand.update(renderer, "deep_rows");
        command.set("label", "updated");
        command.where(ignored -> new WhereDsl(renderer, conditions));

        WriteSpec spec = command.spec();

        assertEquals(List.of("label", "id"), spec.form().fields().stream().map(DynamicField::name).toList());
        assertEquals(List.of(7), renderer.renderWhere(spec.where()).parameters());
    }
}
