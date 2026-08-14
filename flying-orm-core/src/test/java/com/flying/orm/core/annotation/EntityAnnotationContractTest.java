package com.flying.orm.core.annotation;

import org.junit.jupiter.api.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 只验证注解的公开契约，避免把尚未接入 rdb 的映射实现提前混进 core。
 */
class EntityAnnotationContractTest {

    @Test
    void annotationsAreRuntimeVisibleAndUseNarrowTargets() {
        assertRuntimeTypeAnnotation(TableName.class, ElementType.TYPE);
        assertRuntimeTypeAnnotation(KeySequence.class, ElementType.TYPE);
        assertRuntimeFieldAnnotation(TableId.class);
        assertRuntimeFieldAnnotation(TableField.class);
        assertRuntimeFieldAnnotation(Version.class);
        assertRuntimeFieldAnnotation(TableLogic.class);
        assertRuntimeFieldAnnotation(EnumValue.class);
        assertRuntimeFieldAnnotation(OrderBy.class);
    }

    @Test
    void defaultsAreSafeForAnUnconfiguredEntity() throws Exception {
        assertEquals("", method(TableName.class, "value").getDefaultValue());
        assertEquals("", method(TableName.class, "schema").getDefaultValue());
        assertEquals("", method(TableId.class, "value").getDefaultValue());
        assertEquals(IdType.NONE, method(TableId.class, "type").getDefaultValue());
        assertEquals("", method(TableField.class, "value").getDefaultValue());
        assertTrue((Boolean) method(TableField.class, "exist").getDefaultValue());
        assertTrue((Boolean) method(TableField.class, "select").getDefaultValue());
        assertEquals(FieldFill.DEFAULT, method(TableField.class, "fill").getDefaultValue());
        assertEquals(FieldStrategy.DEFAULT, method(TableField.class, "insertStrategy").getDefaultValue());
        assertEquals(FieldStrategy.DEFAULT, method(TableField.class, "updateStrategy").getDefaultValue());
        assertEquals("0", method(TableLogic.class, "value").getDefaultValue());
        assertEquals("1", method(TableLogic.class, "delval").getDefaultValue());
        assertEquals("", method(KeySequence.class, "value").getDefaultValue());
        assertTrue((Boolean) method(OrderBy.class, "asc").getDefaultValue());
        assertEquals(0, method(OrderBy.class, "sort").getDefaultValue());
    }

    @Test
    void strategyEnumsExposeOnlyTheV2FirstBatchValues() {
        assertArrayEquals(new IdType[]{IdType.NONE, IdType.ASSIGN_ID, IdType.AUTO,
                                       IdType.INPUT, IdType.ASSIGN_UUID}, IdType.values());
        assertArrayEquals(new FieldFill[]{FieldFill.DEFAULT, FieldFill.INSERT,
                                          FieldFill.UPDATE, FieldFill.INSERT_UPDATE}, FieldFill.values());
        assertArrayEquals(new FieldStrategy[]{FieldStrategy.DEFAULT, FieldStrategy.ALWAYS,
                                              FieldStrategy.NOT_NULL, FieldStrategy.NOT_EMPTY,
                                              FieldStrategy.NEVER}, FieldStrategy.values());
    }

    private static Method method(Class<?> annotationType, String name) throws NoSuchMethodException {
        return annotationType.getDeclaredMethod(name);
    }

    private static void assertRuntimeTypeAnnotation(Class<? extends java.lang.annotation.Annotation> annotationType,
                                                    ElementType expectedTarget) {
        assertEquals(RetentionPolicy.RUNTIME, annotationType.getAnnotation(Retention.class).value());
        assertArrayEquals(new ElementType[]{expectedTarget},
                          annotationType.getAnnotation(Target.class).value());
        assertFalse(annotationType.isAnnotationPresent(Deprecated.class));
    }

    private static void assertRuntimeFieldAnnotation(Class<? extends java.lang.annotation.Annotation> annotationType) {
        assertRuntimeTypeAnnotation(annotationType, ElementType.FIELD);
    }

    @TableName
    @KeySequence
    private static final class ExampleEntity {

        @TableId
        @TableField
        @Version
        @TableLogic
        @OrderBy
        private Long id;

        @EnumValue
        private ExampleStatus status;
    }

    private enum ExampleStatus {
        ACTIVE
    }
}
