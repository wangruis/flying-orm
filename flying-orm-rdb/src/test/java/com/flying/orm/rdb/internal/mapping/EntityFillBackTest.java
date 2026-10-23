package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.mapping.MappingException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EntityFillBackTest {

    @Test
    void insertDefaultsBackfillDomainValueThroughSetterBeforeEncoding() {
        Bean entity = new Bean();
        EntityValues<Bean> values = plan(Bean.class);
        Map<String, Object> row = values.readForInsert(entity);
        assertEquals("D", row.get("state"));
        assertSame(State.DISABLED, entity.state);
        assertEquals(1, entity.setterCalls);
        assertEquals("D", values.readForInsert(entity).get("state"));
        assertEquals(1, entity.setterCalls, "supplied values do not trigger default filling");
    }

    @Test
    void upsertUsesOriginalValueForConflictPolicyEvenAfterBackfill() {
        Bean entity = new Bean();
        RepositoryUpsertValues row = plan(Bean.class).repositoryUpsertValues(entity);
        assertSame(State.DISABLED, entity.state);
        assertEquals("D", row.insertValues().get("state"));
        assertFalse(row.updateValues().containsKey("state"));
        entity.state = State.ACTIVE;
        row = plan(Bean.class).repositoryUpsertValues(entity);
        assertEquals("A", row.updateValues().get("state"));
        assertEquals(1, entity.setterCalls);
    }

    @Test
    void fieldOnlyAndInheritedMembersAreSupportedWithoutNoArgConstructor() {
        Child entity = new Child(7L);
        assertEquals("D", plan(Child.class).readForInsert(entity).get("state"));
        assertSame(State.DISABLED, entity.state);
    }

    @Test
    void updateDoesNotFillInsertDefaultsAndExcludedFieldsStayUntouched() {
        Bean entity = new Bean();
        assertNull(plan(Bean.class).readForUpdate(entity).get("state"));
        assertNull(entity.state);
        assertEquals(0, entity.setterCalls);
        Excluded excluded = new Excluded();
        EntityValues<Excluded> values = EntityValues.createUncached(Excluded.class,
                EntityMetadataResolver.createUncached(Excluded.class), (a, b, c, d) -> {
                    throw new AssertionError("excluded field must not be filled");
                });
        values.readForInsert(excluded);
        assertNull(excluded.state);
    }

    @Test
    void immutableFillTargetsFailAtPlanCreation() {
        assertThrows(MappingException.class, () -> plan(Immutable.class));
        assertThrows(MappingException.class, () -> plan(FinalBean.class));
    }

    @Test
    void failedSetterIsNotSilentlyIgnored() {
        MappingException error = assertThrows(MappingException.class,
                () -> plan(Failing.class).readForInsert(new Failing()));
        assertTrue(error.getMessage().contains("setter"));
    }

    @Test
    void nullFillResultDoesNotInvokeSetterForUnchangedNull() {
        Bean entity = new Bean();
        EntityValues<Bean> values = EntityValues.createUncached(Bean.class,
                EntityMetadataResolver.createUncached(Bean.class), (a, b, c, d) -> null);
        values.readForInsert(entity);
        assertNull(entity.state);
        assertEquals(0, entity.setterCalls);
    }

    @Test
    void updateFillBackKeepsDomainTypeAndCanExplicitlyClearValue() {
        UpdateBean entity = new UpdateBean();
        assertEquals("D", plan(UpdateBean.class).readForRepositoryUpdate(entity).get("state"));
        assertSame(State.DISABLED, entity.state);
        EntityValues<UpdateBean> clear = EntityValues.createUncached(UpdateBean.class,
                EntityMetadataResolver.createUncached(UpdateBean.class), (a, b, c, d) -> null);
        assertNull(clear.readForUpdate(entity, java.util.Set.of("state")).get("state"));
        assertNull(entity.state);
    }

    @Test
    void cacheWeightIncludesOptedInWriteSlots() {
        assertEquals(3, plan(Bean.class).logicalWeight(), "two read slots and one fill-back write slot");
    }

    private static <T> EntityValues<T> plan(Class<T> type) {
        return EntityValues.createUncached(type, EntityMetadataResolver.createUncached(type),
                (entity, field, operation, current) -> State.DISABLED);
    }

    enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    @TableName("fill_back")
    static class Bean {
        @TableId Long id = 1L;
        @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true) State state;
        transient int setterCalls;
        public State getState() { return state; }
        public void setState(State state) { setterCalls++; this.state = state; }
    }

    static class Parent {
        @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true) State state;
    }

    @TableName("fill_back")
    static class UpdateBean {
        @TableId Long id = 1L;
        @TableField(fill = FieldFill.UPDATE, fillBack = true) State state = State.ACTIVE;
    }

    @TableName("fill_back")
    static class Child extends Parent {
        @TableId Long id;
        Child(Long id) { this.id = id; }
    }

    @TableName("fill_back")
    static class Excluded {
        @TableId Long id = 1L;
        @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true,
                insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER) State state;
    }

    @TableName("fill_back")
    record Immutable(@TableId Long id,
            @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true) State state) { }

    @TableName("fill_back")
    static class FinalBean {
        @TableId Long id = 1L;
        @TableField(fill = FieldFill.INSERT_DEFAULT, fillBack = true) final State state = null;
    }

    @TableName("fill_back")
    static class Failing extends Bean {
        @Override public void setState(State value) { throw new IllegalStateException("setter failed"); }
    }
}
