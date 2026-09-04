package com.flying.orm.rdb.mapping;

import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityMappingEventSnapshotTest {

    @Test
    void snapshotsOnlyTheTopLevelMapAndKeepsNestedValuesByReference() {
        List<Object> nested = new ArrayList<>(List.of("value"));
        byte[] payload = new byte[]{1, 2, 3};
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("nested", nested);
        source.put("payload", payload);

        EntityMappingEvent event = event(source);
        source.put("late", "change");
        nested.add("later");
        payload[0] = 9;

        assertSame(nested, event.values().get("nested"));
        assertSame(payload, event.values().get("payload"));
        assertEquals(List.of("value", "later"), event.values().get("nested"));
        assertEquals(9, ((byte[]) event.values().get("payload"))[0]);
        assertFalse(event.values().containsKey("late"));
        assertThrows(UnsupportedOperationException.class,
                () -> event.values().put("new", "value"));
    }

    @Test
    void acceptsCyclicNestedApplicationValuesWithoutTraversingThem() {
        Map<String, Object> cyclic = new LinkedHashMap<>();
        cyclic.put("self", cyclic);

        EntityMappingEvent event = event(Map.of("value", cyclic));

        assertSame(cyclic, event.values().get("value"));
        assertSame(cyclic, cyclic.get("self"));
    }

    @Test
    void listenerFailureRemainsThePrimaryFailureWithoutMiningItsWrapper() {
        SyntheticVirtualMachineError nestedFatal = new SyntheticVirtualMachineError();
        CompletionException listenerFailure = new CompletionException(nestedFatal);
        EntityValues<TestEntity> values = EntityValues.createUncached(TestEntity.class);

        CompletionException actual = assertThrows(
                CompletionException.class,
                () -> values.read(new TestEntity(1L), new EntityMappingListener() {
                    @Override
                    public void beforeWrite(EntityMappingEvent ignored) {
                        throw listenerFailure;
                    }
                }));

        assertSame(listenerFailure, actual);
    }

    @Test
    void afterReadFailureRemainsThePrimaryFailureWithoutMiningItsWrapper() {
        SyntheticVirtualMachineError nestedFatal = new SyntheticVirtualMachineError();
        CompletionException listenerFailure = new CompletionException(nestedFatal);
        RowMapper<TestEntity> mapper = RowMapper.of(TestEntity.class, new EntityMappingListener() {
            @Override
            public void afterRead(EntityMappingEvent ignored) {
                throw listenerFailure;
            }
        });

        CompletionException actual = assertThrows(
                CompletionException.class,
                () -> mapper.map(Map.of("id", 1L)));

        assertSame(listenerFailure, actual);
    }

    private static EntityMappingEvent event(Map<String, Object> values) {
        TestEntity entity = new TestEntity(1L);
        return new EntityMappingEvent(
                EntityMetadataResolver.createUncached(TestEntity.class), entity, values);
    }

    private record TestEntity(Long id) {
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
    }
}
