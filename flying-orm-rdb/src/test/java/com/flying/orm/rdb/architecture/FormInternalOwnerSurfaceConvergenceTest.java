package com.flying.orm.rdb.architecture;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.FormAggregateReadSupport;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.internal.InternalApi;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormInternalOwnerSurfaceConvergenceTest {

    @Test
    void formOwnersDoNotExposeSingleUseTopLevelHelpers() {
        List<String> obsoleteTypes = List.of(
                "com.flying.orm.rdb.form.KeysetPredicateRenderer",
                "com.flying.orm.rdb.form.KeysetCursorVisibilityGuard",
                "com.flying.orm.rdb.form.JoinFieldUseGuard",
                "com.flying.orm.rdb.form.JoinSourceSqlRenderer");

        assertAll(obsoleteTypes.stream()
                .map(typeName -> () -> assertThrows(
                        ClassNotFoundException.class,
                        () -> Class.forName(typeName, false,
                                FormInternalOwnerSurfaceConvergenceTest.class.getClassLoader()))));
    }

    @Test
    void fieldUseGuardKeepsItsImplementationInsideTheFormPackage() throws Exception {
        Class<?> guard = Class.forName("com.flying.orm.rdb.form.FieldUseGuard");
        Method aggregate = guard.getDeclaredMethod(
                "approveAggregate",
                String.class,
                FieldUseRequirements.class,
                FieldScope.class,
                SqlRequest.class,
                FieldUsePolicy.class,
                QueryShapeLimits.class,
                int.class,
                int.class,
                int.class,
                int.class,
                int.class);
        Method term = guard.getDeclaredMethod(
                "approveTermExtension",
                FormDataSqlRenderer.class,
                TermCondition.class,
                FieldUse.class);

        assertAll(
                () -> assertFalse(Modifier.isPublic(guard.getModifiers())),
                () -> assertFalse(Modifier.isPublic(aggregate.getModifiers())),
                () -> assertFalse(Modifier.isPublic(term.getModifiers())));
    }

    @Test
    void aggregateReadSupportOwnsTheEquivalentCrossPackageApprovalBridge() throws Exception {
        Method bridge = FormAggregateReadSupport.class.getMethod(
                "approveAggregate",
                String.class,
                FieldUseRequirements.class,
                FieldScope.class,
                SqlRequest.class,
                FieldUsePolicy.class,
                QueryShapeLimits.class,
                int.class,
                int.class,
                int.class,
                int.class,
                int.class);

        assertAll(
                () -> assertEquals(FieldUseSnapshot.class, bridge.getReturnType()),
                () -> assertTrue(Modifier.isPublic(bridge.getModifiers())),
                () -> assertTrue(bridge.isAnnotationPresent(InternalApi.class)));
    }
}
