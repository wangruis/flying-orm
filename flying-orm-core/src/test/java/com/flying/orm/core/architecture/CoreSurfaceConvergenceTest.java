package com.flying.orm.core.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CoreSurfaceConvergenceTest {

    @Test
    void redundantPackageLocalHelpersAreAbsent() {
        assertAll(
                () -> assertClassAbsent("com.flying.orm.core.page.PageNames"),
                () -> assertClassAbsent("com.flying.orm.core.codec.StandardValueCodecs"));
    }

    @Test
    void standardCodecImplementationsArePrivateToTheirRegistry() {
        assertAll(
                () -> assertClassAbsent("com.flying.orm.core.codec.ArrayIdentityValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.UuidValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.IdentityValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.BooleanValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.EnumValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.TextValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.NumberValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.BinaryValueCodec"),
                () -> assertClassAbsent("com.flying.orm.core.codec.JavaTimeValueCodec"));
    }

    private static void assertClassAbsent(String className) {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(className));
    }
}
