package com.flying.orm.core.form;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicDeleteDefinitionTest {

    @Test
    void publicReadsStayDefensiveWithoutOwnedAccessors() {
        LogicDeleteDefinition definition = LogicDeleteDefinition.of(
                "deleted", ByteBuffer.wrap(new byte[]{0}), ByteBuffer.wrap(new byte[]{1}));

        ByteBuffer firstNotDeleted = (ByteBuffer) definition.notDeletedValue();
        ByteBuffer secondNotDeleted = (ByteBuffer) definition.notDeletedValue();
        ByteBuffer firstDeleted = (ByteBuffer) definition.deletedValue();
        ByteBuffer secondDeleted = (ByteBuffer) definition.deletedValue();

        assertFalse(publicMethod("ownedNotDeletedValue"));
        assertFalse(publicMethod("ownedDeletedValue"));
        assertNotSame(firstNotDeleted, secondNotDeleted);
        assertNotSame(firstDeleted, secondDeleted);
        assertEquals(0, secondNotDeleted.get(0));
        assertEquals(1, secondDeleted.get(0));
        assertTrue(firstNotDeleted.isReadOnly());
        assertTrue(firstDeleted.isReadOnly());
    }

    private static boolean publicMethod(String name) {
        return Arrays.stream(LogicDeleteDefinition.class.getDeclaredMethods())
                     .anyMatch(method -> method.getName().equals(name)
                             && Modifier.isPublic(method.getModifiers()));
    }
}
