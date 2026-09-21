package com.flying.orm.rdb.performance;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 默认关闭新能力时的结构护栏。
 *
 * <p>这些类型位于普通 CRUD 和批量热路径。新能力必须使用显式包装或冷路径对象，不能悄悄给每次请求、
 * 每个表单或每个计划增加字段；真实分配和延迟由 JMH/AB 门禁继续量化。</p>
 */
class DefaultPathFeatureOffTest {

    private static final List<String> OPTIONAL_CAPABILITY_MARKERS = List.of(
            "relational", "fielduse", "queryshapelimit", "aggregate", "lockingread",
            "dialectcapabilit", "batchexecutionevidence", "schemadescription");

    @Test
    void optionalCapabilitiesDoNotAddStateToLegacyHotObjects() throws ClassNotFoundException {
        assertNoOptionalCapabilityState(DynamicForm.class);
        assertNoOptionalCapabilityState(QuerySpec.class);
        assertNoOptionalCapabilityState(WriteSpec.class);
        assertNoOptionalCapabilityState(Class.forName("com.flying.orm.rdb.form.FormOperationPlanner"));
    }

    private static void assertNoOptionalCapabilityState(Class<?> type) {
        List<String> violations = Arrays.stream(type.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !field.isSynthetic())
                .filter(DefaultPathFeatureOffTest::isOptionalCapabilityState)
                .map(field -> field.getName() + ":" + field.getGenericType().getTypeName())
                .sorted()
                .toList();
        assertTrue(violations.isEmpty(),
                   () -> type.getName() + " must not carry optional capability state: " + violations);
    }

    private static boolean isOptionalCapabilityState(Field field) {
        String shape = (field.getName() + " " + field.getGenericType().getTypeName())
                .toLowerCase(Locale.ROOT);
        return OPTIONAL_CAPABILITY_MARKERS.stream().anyMatch(shape::contains);
    }
}
