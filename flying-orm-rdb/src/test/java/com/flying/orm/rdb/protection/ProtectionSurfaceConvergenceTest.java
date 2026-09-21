package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.internal.InternalApi;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定保护索引投影只有一个关系模型所有者，不再回流独立的公开内部类型。 */
class ProtectionSurfaceConvergenceTest {

    @Test
    void operationPlansBelongToTheProtectedFieldRuntime() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.protection.ProtectedFieldOperationPlans"));
    }

    @Test
    void indexProjectionBelongsToTheRelationalSchemaProjector() {
        assertAll(
                () -> assertThrows(ClassNotFoundException.class, () -> Class.forName(
                        "com.flying.orm.rdb.protection.ProtectedIndexProjection")),
                () -> {
                    Method method = ProtectedRelationalSchemaProjector.class.getDeclaredMethod(
                            "projectIndexColumns", DynamicForm.class, List.class, boolean.class);
                    assertTrue(Modifier.isPublic(method.getModifiers()));
                    assertTrue(Modifier.isStatic(method.getModifiers()));
                    assertTrue(method.isAnnotationPresent(InternalApi.class));
                });
    }
}
