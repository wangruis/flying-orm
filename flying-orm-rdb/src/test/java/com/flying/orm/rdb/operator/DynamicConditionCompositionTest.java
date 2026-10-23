package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicConditionCompositionTest {
    private static final SqlRenderer RENDERER = SqlRenderer.builder().addDefaultTerms().build();

    @Test
    void queryCallbacksAppendWithoutLosingEarlierBusinessConditions() {
        PlanningDynamicQuery query = query();
        query.where("id", "=", 7L);
        query.where(query.conditions(where -> where.or(group -> group.is("name", "Ada").is("name", "Bob"))));
        query.where(query.conditions(where -> where.whereIfPresent("unused", " ")));
        query.where("active", "=", true);
        assertEquals("select * from users where id = ? and (name = ? or name = ?) and active = ?",
                query.toRequest().sql());
        assertEquals(List.of(7L, "Ada", "Bob", true), query.toRequest().parameters());
    }

    @Test
    void queryCallbacksKeepExistingRequestsAndRetainedBuildersIndependent() {
        PlanningDynamicQuery query = query();
        query.where("id", "=", 7L);
        SqlRequest original = query.toRequest();
        AtomicReference<WhereDsl> retained = new AtomicReference<>();
        query.where(query.conditions(where -> {
            retained.set(where);
            where.is("active", true);
        }));
        retained.get().is("name", "late");
        assertEquals(List.of(7L), original.parameters());
        assertEquals(List.of(7L, true), query.toRequest().parameters());
    }

    @Test
    void updateAndDeleteCallbacksAppendWithTheSameNestedPrecedence() {
        for (DmlWriteCommand<?> write : writes()) {
            write.where("id", "=", 7L);
            write.where(where -> where.or(group -> group.is("name", "Ada").is("name", "Bob")));
            write.where(where -> where.whereIfPresent("unused", " "));
            write.where("active", "=", true);
            ConditionGroup where = write.spec().where();
            assertEquals("id = ? and (name = ? or name = ?) and active = ?", RENDERER.renderWhere(where).sql());
            assertEquals(List.of(7L, "Ada", "Bob", true), RENDERER.renderWhere(where).parameters());
        }
    }

    @Test
    void writeCallbacksKeepExistingSpecsAndRetainedBuildersIndependent() {
        for (DmlWriteCommand<?> write : writes()) {
            write.where("id", "=", 7L);
            ConditionGroup original = write.spec().where();
            AtomicReference<WhereDsl> retained = new AtomicReference<>();
            write.where(where -> {
                retained.set(where);
                return where.is("active", true);
            });
            retained.get().is("name", "late");
            assertEquals(List.of(7L), RENDERER.renderWhere(original).parameters());
            assertEquals(List.of(7L, true), RENDERER.renderWhere(write.spec().where()).parameters());
        }
    }

    @Test
    void failedCallbacksLeavePreviouslyDeclaredConditionsIntact() {
        IllegalStateException failure = new IllegalStateException("invalid search input");
        PlanningDynamicQuery query = query();
        query.where("id", "=", 7L);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> query.where(query.conditions(where -> {
            where.is("active", true);
            throw failure;
        }))));
        assertEquals(List.of(7L), query.toRequest().parameters());
        for (DmlWriteCommand<?> write : writes()) {
            write.where("id", "=", 7L);
            assertSame(failure, assertThrows(IllegalStateException.class, () -> write.where(where -> {
                where.is("active", true);
                throw failure;
            })));
            assertEquals(List.of(7L), RENDERER.renderWhere(write.spec().where()).parameters());
        }
    }

    @Test
    void returnedOrGroupKeepsItsPrecedenceWhenAppendingToWrites() {
        for (DmlWriteCommand<?> write : writes()) {
            write.where("id", "=", 7L);
            write.where(where -> {
                AtomicReference<WhereDsl> selected = new AtomicReference<>();
                where.or(group -> {
                    selected.set(group);
                    group.is("name", "Ada").is("name", "Bob");
                });
                return selected.get();
            });
            assertEquals("id = ? and (name = ? or name = ?)", RENDERER.renderWhere(write.spec().where()).sql());
            assertEquals(List.of(7L, "Ada", "Bob"), RENDERER.renderWhere(write.spec().where()).parameters());
        }
    }

    @Test
    void appendingAfterOrReplacementDoesNotFlattenTheReplacement() {
        for (DmlWriteCommand<?> write : writes()) {
            write.replaceWhere(where -> {
                AtomicReference<WhereDsl> selected = new AtomicReference<>();
                where.or(group -> {
                    selected.set(group);
                    group.is("name", "Ada").is("name", "Bob");
                });
                return selected.get();
            });
            ConditionGroup original = write.spec().where();
            write.where("id", "=", 7L);
            assertEquals("name = ? or name = ?", RENDERER.renderWhere(original).sql());
            assertEquals("(name = ? or name = ?) and id = ?", RENDERER.renderWhere(write.spec().where()).sql());
            assertEquals(List.of("Ada", "Bob", 7L), RENDERER.renderWhere(write.spec().where()).parameters());
        }
    }

    @Test
    void explicitQueryReplacementRetainsScopeAndLogicalDeletion() {
        PlanningDynamicQuery query = new PlanningDynamicQuery(RENDERER, DataScope.tenant("tenant_id", 10));
        query.from("users");
        query.scope(DataScope.tenant("org_id", 20));
        query.logicDelete("deleted", 0, 1);
        query.where("id", "=", 7L);
        SqlRequest original = query.toRequest();
        AtomicReference<WhereDsl> retained = new AtomicReference<>();
        query.replaceWhere(where -> {
            retained.set(where);
            where.is("id", 8L);
        });
        retained.get().is("name", "late");
        assertEquals(List.of(7L, 10, 20, 0), original.parameters());
        assertEquals(List.of(8L, 10, 20, 0), query.toRequest().parameters());
        query.replaceWhere(where -> { });
        assertEquals(List.of(10, 20, 0), query.toRequest().parameters());
    }

    @Test
    void governedReplacementKeepsFrontendFilterSeparateFromBusinessWhere() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
        PlanningDynamicQuery query = query();
        query.from(form);
        StructuredConditionInput input = StructuredConditionInput.term("name", "=", "Ada");
        query.filter(input, StructuredConditionPolicy.defaults());
        query.scope(DataScope.tenant("tenant_id", 10));
        query.where("id", "=", 7L);
        query.replaceWhere(where -> where.is("id", 8L));
        QuerySpec spec = query.governedQuery(null).spec();
        assertSame(input, spec.structuredInput().orElseThrow());
        assertEquals(List.of(8L), RENDERER.renderWhere(spec.where()).parameters());
        assertEquals(List.of(10), RENDERER.renderWhere(spec.scope().condition().orElseThrow()).parameters());
    }

    @Test
    void explicitWriteReplacementRetainsLockScopeAndLogicalDeletion() {
        for (DmlWriteCommand<?> write : writes()) {
            write.scope(DataScope.tenant("tenant_id", 10));
            write.logicDelete("deleted", 0, 1);
            OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 3L);
            write.optimisticLock(lock);
            write.where("id", "=", 7L);
            WriteSpec original = write.spec();
            AtomicReference<WhereDsl> retained = new AtomicReference<>();
            write.replaceWhere(where -> {
                retained.set(where);
                return where.is("id", 8L);
            });
            retained.get().is("name", "late");
            WriteSpec replaced = write.spec();
            assertEquals(List.of(7L), RENDERER.renderWhere(original.where()).parameters());
            assertEquals(List.of(8L), RENDERER.renderWhere(replaced.where()).parameters());
            assertSame(lock, replaced.lock().orElseThrow());
            assertEquals(List.of(10), RENDERER.renderWhere(replaced.scope().condition().orElseThrow()).parameters());
            assertEquals("deleted", replaced.form().logicDelete().orElseThrow().fieldName());
            write.replaceWhere(where -> where);
            assertTrue(write.spec().where().children().isEmpty());
            assertSame(lock, write.spec().lock().orElseThrow());
        }
    }

    private static PlanningDynamicQuery query() {
        PlanningDynamicQuery query = new PlanningDynamicQuery(RENDERER, DataScope.none());
        query.from("users");
        return query;
    }

    private static List<DmlWriteCommand<?>> writes() {
        PlanningDynamicWrite.Update update = new PlanningDynamicWrite.Update(RENDERER, "users");
        update.set("name", "Updated");
        return List.of(update, new PlanningDynamicWrite.Delete(RENDERER, "users"));
    }
}
