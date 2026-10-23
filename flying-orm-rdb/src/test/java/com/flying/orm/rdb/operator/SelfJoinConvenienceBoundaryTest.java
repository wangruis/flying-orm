package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelfJoinConvenienceBoundaryTest {

    @Test
    void formConvenienceRejectsAmbiguousIdentityBeforeMutatingItsSources() {
        DynamicForm form = form("employees");
        PlanningJoin command = new PlanningJoin(form, renderer());
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> command.join(JoinType.LEFT, form, "manager_id", "id"));
        assertSourceBasedGuidance(error);
        JoinQuerySpec spec = command.select(form, "id").spec();
        assertEquals(1, spec.sources().size());
        assertTrue(spec.joins().isEmpty());
        assertSame(spec.root(), spec.projections().getFirst().field().source());
    }

    @Test
    void formConvenienceKeepsDistinctViewsOfTheSamePhysicalTableAddressable() {
        DynamicForm employees = form("employees");
        DynamicForm managers = form("managers");
        JoinQuerySpec spec = new PlanningJoin(employees, renderer())
                .join(JoinType.LEFT, managers, "manager_id", "id")
                .select(employees, "id").select(managers, "id").spec();
        assertEquals(List.of(0, 1), spec.projections().stream().map(value -> value.field().source().ordinal()).toList());
        assertEquals(List.of("s0_id", "s1_id"), spec.projections().stream().map(value -> value.alias()).toList());
    }

    @Test
    void typedConvenienceRejectsRepeatedClassWithAnExecutableSourceBasedAlternative() {
        var command = new PlanningEntityJoin<>(models(), renderer(), Employee.class);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> command.join(JoinType.LEFT, Employee.class, Employee::managerId, Employee::id));
        assertSourceBasedGuidance(error);
        assertEquals(1, command.selectAs(Employee.class, Employee::id, "employee_id").spec().sources().size());
    }

    @Test
    void typedConvenienceAllowsDistinctEntityClassesMappedToTheSameTable() {
        JoinQuerySpec spec = new PlanningEntityJoin<>(models(), renderer(), Employee.class)
                .join(JoinType.LEFT, Manager.class, Employee::managerId, Manager::id)
                .selectAs(Employee.class, Employee::id, "employee_id")
                .selectAs(Manager.class, Manager::id, "manager_id").spec();
        assertEquals(List.of(0, 1), spec.projections().stream().map(value -> value.field().source().ordinal()).toList());
    }

    private static final class PlanningJoin extends JoinQueryCommand<PlanningJoin> {
        PlanningJoin(DynamicForm form, SqlRenderer renderer) { super(form, renderer); }
        @Override protected PlanningJoin self() { return this; }
    }

    private static final class PlanningEntityJoin<R> extends EntityJoinQueryCommand<R, PlanningEntityJoin<R>> {
        PlanningEntityJoin(EntityModelRegistry models, SqlRenderer renderer, Class<R> type) {
            super(models, renderer, type);
        }
        @Override protected PlanningEntityJoin<R> self() { return this; }
    }

    private static void assertSourceBasedGuidance(IllegalArgumentException error) {
        assertTrue(error.getMessage().contains("JoinQuerySpec"), error.getMessage());
        assertTrue(error.getMessage().contains("JoinSource"), error.getMessage());
    }

    private static DynamicForm form(String id) {
        return DynamicForm.builder(id, "employees").addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("manager_id", "BIGINT")).build();
    }
    private static SqlRenderer renderer() { return SqlRenderer.builder().addDefaultTerms().build(); }
    private static EntityModelRegistry models() { return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()); }

    @TableName("employees")
    private record Employee(Long id, Long managerId) { }
    @TableName("employees")
    private record Manager(Long id) { }
}
