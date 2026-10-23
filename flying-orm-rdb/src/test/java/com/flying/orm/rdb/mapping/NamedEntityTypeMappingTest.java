package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NamedEntityTypeMappingTest {
    private static final ValueCodec CODE = codec(false);
    private static final ValueCodec NUMBER = codec(true);

    @Test
    void candidateSelectionRetainsSpecificityStandardPreferenceAndAmbiguity() {
        EntityTypeMappingRegistry.Mapping standard = EntityTypeMappingRegistry.standard().resolve(String.class);
        EntityTypeMappingRegistry.Mapping broad = new EntityTypeMappingRegistry.Mapping(
                "text", CharSequence.class, DatabaseType.of("VARCHAR"), standard.codec());
        EntityTypeMappingRegistry.Mapping unrelated = new EntityTypeMappingRegistry.Mapping(
                "comparable", Comparable.class, DatabaseType.of("VARCHAR"), standard.codec());
        EntityTypeMappingRegistry.Mapping named = new EntityTypeMappingRegistry.Mapping(
                "named", String.class, DatabaseType.of("VARCHAR"), standard.codec());
        List<EntityTypeMappingRegistry.Mapping> defaults = List.of(standard);
        assertNull(EntityTypeMappingResolver.resolveRegistered(
                List.of(broad), Number.class, true, null, defaults));
        assertEquals(StringBuilder.class, EntityTypeMappingResolver.resolveRegistered(
                List.of(broad), StringBuilder.class, true, null, defaults).javaType());
        for (List<EntityTypeMappingRegistry.Mapping> candidates : List.of(
                List.of(broad, unrelated, standard), List.of(standard, unrelated, broad))) {
            assertSame(standard, EntityTypeMappingResolver.resolveRegistered(
                    candidates, String.class, true, null, defaults));
        }
        assertSame(standard, EntityTypeMappingResolver.resolveRegistered(
                List.of(named, standard), String.class, true, null, defaults));
        assertThrows(MappingException.class, () -> EntityTypeMappingResolver.resolveRegistered(
                List.of(named, standard), String.class, false, "VARCHAR", defaults));
        assertThrows(MappingException.class, () -> EntityTypeMappingResolver.resolveRegistered(
                List.of(broad, unrelated), String.class, true, null, defaults));
    }

    @Test
    void explicitNamesChooseDifferentStorageWithoutChangingDefaultConversion() {
        EntityTypeMappingRegistry mappings = namedMappings();
        assertSame(CODE, mappings.resolve("state-code", State.class).codec());
        assertSame(NUMBER, mappings.resolve("state-number", State.class).codec());
        assertEquals("D", mappings.resolve("state-code", State.class).codec().write(State.DISABLED));
        assertEquals(0, mappings.resolve("state-number", State.class).codec().write(State.DISABLED));
        assertEquals("VARCHAR", mappings.resolve(State.class).id());
        assertEquals("DISABLED", mappings.valueCodecs().write(State.DISABLED));
        assertEquals("plain", mappings.valueCodecs().write("plain"));
        assertThrows(MappingException.class, () -> mappings.resolve("state-number", String.class));
    }

    @Test
    void namedMappingsDoNotReplaceAnExplicitDefault() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("state-code", State.class, DatabaseType.of("VARCHAR"), CODE)
                .registerNamed("state-number", State.class, DatabaseType.of("INTEGER"), NUMBER).build();
        assertSame(CODE, mappings.resolve(State.class).codec());
        assertEquals("D", mappings.valueCodecs().write(State.DISABLED));
        assertSame(NUMBER, mappings.resolve("state-number", State.class).codec());
    }

    @Test
    void duplicateNamesForOneTypeAreRejectedInEitherRegistrationOrder() {
        for (boolean namedFirst : List.of(false, true)) {
            EntityTypeMappingRegistry.Builder builder = EntityTypeMappingRegistry.builder();
            if (namedFirst) builder.registerNamed("state", State.class, DatabaseType.of("INTEGER"), NUMBER);
            builder.register("state", State.class, DatabaseType.of("VARCHAR"), CODE);
            if (!namedFirst) builder.registerNamed("state", State.class, DatabaseType.of("INTEGER"), NUMBER);
            assertThrows(IllegalArgumentException.class, builder::build);
        }
        assertThrows(IllegalArgumentException.class, () -> EntityTypeMappingRegistry.builder()
                .registerNamed("state", State.class, DatabaseType.of("VARCHAR"), CODE)
                .registerNamed("state", State.class, DatabaseType.of("INTEGER"), NUMBER).build());
    }

    @Test
    void fingerprintsIncludeNamedMappingRolesAndBuildersPublishIndependentSnapshots() {
        EntityTypeMappingRegistry.Builder builder = EntityTypeMappingRegistry.builder();
        EntityTypeMappingRegistry before = builder.build();
        EntityTypeMappingRegistry after = builder
                .registerNamed("state-code", State.class, DatabaseType.of("VARCHAR"), CODE).build();
        assertEquals(EntityTypeMappingRegistry.standard().fingerprint(), before.fingerprint());
        assertThrows(MappingException.class, () -> before.resolve("state-code", State.class));
        assertNotEquals(before.fingerprint(), after.fingerprint());
        assertNotEquals(after.fingerprint(), EntityTypeMappingRegistry.builder()
                .register("state-code", State.class, DatabaseType.of("VARCHAR"), CODE).build().fingerprint());
        assertEquals(namedMappings().fingerprint(), namedMappings().fingerprint());
    }

    static EntityTypeMappingRegistry namedMappings() {
        return namedBuilder().build();
    }

    static EntityTypeMappingRegistry.Builder namedBuilder() {
        return EntityTypeMappingRegistry.builder()
                .registerNamed("state-code", State.class, DatabaseType.of("VARCHAR"), CODE)
                .registerNamed("state-number", State.class, DatabaseType.of("INTEGER"), NUMBER);
    }

    enum State { ACTIVE, DISABLED }

    private static ValueCodec codec(boolean numeric) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> type) { return type == State.class; }
            @Override
            public Object write(Object value) {
                if (value == null) return null;
                State state = (State) value;
                return numeric ? state == State.ACTIVE ? 1 : 0 : state == State.ACTIVE ? "A" : "D";
            }
            @Override
            public Object read(Object value, Class<?> type) {
                if (value == null) return null;
                if (value.equals(numeric ? 1 : "A")) return State.ACTIVE;
                if (value.equals(numeric ? 0 : "D")) return State.DISABLED;
                throw new IllegalArgumentException("unknown stored state");
            }
        };
    }
}
