package com.flying.orm.rdb.lock;

import org.junit.jupiter.api.Test;

import com.flying.orm.rdb.schema.SchemaDialect;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimisticLockOptionsTest {

    @Test
    void publicReadsStayDefensiveWithoutOwnedAccessorsOrIdentifierOverload() {
        OptimisticLockOptions options = OptimisticLockOptions.assign(
                "version", ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(new byte[]{2}));

        ByteBuffer firstExpected = (ByteBuffer) options.expectedValue();
        ByteBuffer secondExpected = (ByteBuffer) options.expectedValue();
        ByteBuffer firstNext = (ByteBuffer) options.nextValue();
        ByteBuffer secondNext = (ByteBuffer) options.nextValue();

        assertFalse(publicMethod(OptimisticLockOptions.class, "ownedExpectedValue"));
        assertFalse(publicMethod(OptimisticLockOptions.class, "ownedNextValue"));
        assertFalse(Arrays.stream(SchemaDialect.class.getDeclaredMethods())
                          .filter(method -> method.getName().equals("identifier"))
                          .filter(method -> Modifier.isPublic(method.getModifiers()))
                          .anyMatch(method -> !Arrays.equals(method.getParameterTypes(), new Class<?>[]{String.class})));
        assertNotSame(firstExpected, secondExpected);
        assertNotSame(firstNext, secondNext);
        assertEquals(1, secondExpected.get(0));
        assertEquals(2, secondNext.get(0));
        assertTrue(firstExpected.isReadOnly());
        assertTrue(firstNext.isReadOnly());
    }

    private static boolean publicMethod(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .anyMatch(method -> method.getName().equals(name)
                             && Modifier.isPublic(method.getModifiers()));
    }
}
