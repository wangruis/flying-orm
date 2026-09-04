package com.flying.orm.rdb.internal.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityFieldTypeResolverTest {

    @Test
    void parsesFloatingPointLogicDeleteValuesUsingTheDeclaredFieldType() {
        Object doubleValue = EntityFieldTypeResolver.typedValue("1.5", double.class);
        Object floatValue = EntityFieldTypeResolver.typedValue("2.5", Float.class);

        assertInstanceOf(Double.class, doubleValue);
        assertEquals(1.5D, doubleValue);
        assertInstanceOf(Float.class, floatValue);
        assertEquals(2.5F, floatValue);
    }

    @Test
    void rejectsMisspelledBooleanLogicDeleteValues() {
        assertEquals(true, EntityFieldTypeResolver.typedValue("1", boolean.class));
        assertEquals(false, EntityFieldTypeResolver.typedValue("0", Boolean.class));
        assertEquals(true, EntityFieldTypeResolver.typedValue("TRUE", boolean.class));
        assertEquals(false, EntityFieldTypeResolver.typedValue("False", Boolean.class));
        assertThrows(IllegalArgumentException.class,
                     () -> EntityFieldTypeResolver.typedValue("ture", boolean.class));
    }

    @Test
    void rejectsNonFiniteFloatingPointLogicDeleteValues() {
        assertThrows(IllegalArgumentException.class,
                     () -> EntityFieldTypeResolver.typedValue("NaN", double.class));
        assertThrows(IllegalArgumentException.class,
                     () -> EntityFieldTypeResolver.typedValue("Infinity", float.class));
    }
}
