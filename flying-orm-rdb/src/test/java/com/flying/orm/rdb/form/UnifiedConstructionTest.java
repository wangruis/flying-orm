package com.flying.orm.rdb.form;

import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.observation.BatchExecutionObservation.BatchWriteRequestView;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UnifiedConstructionTest {
    @Test
    void formCachesAreConfiguredAtCreationWithoutRebuildingTheRenderer() {
        for (Class<?> type : List.of(FormDataSqlRenderer.class, FormSqlRenderSupport.class)) {
            assertFalse(Arrays.stream(type.getDeclaredMethods()).anyMatch(method ->
                    method.getName().equals("withPlanCaches")
                            || method.getName().equals("withResultPlanCachePolicy")));
        }
    }

    @Test
    void sqlSupportRequiresTheCompleteDialectAssembly() {
        assertEquals(1, FormSqlRenderSupport.class.getDeclaredConstructors().length);
        assertEquals(9, FormSqlRenderSupport.class.getDeclaredConstructors()[0].getParameterCount());
    }

    @Test
    void statelessSchemaDiffAndQueryDefaultsHaveOneEntryPoint() {
        assertEquals(0, com.flying.orm.rdb.schema.SchemaDiffer.class.getConstructors().length);
        assertThrows(NoSuchMethodException.class,
                () -> com.flying.orm.core.condition.QueryShapeLimits.class.getMethod("existingDefaults"));
    }

    @Test
    void identifiersDoNotRequireUnusedDiagnosticArguments() {
        assertTrue(Arrays.stream(SqlIdentifiers.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("requireIdentifier")
                        || method.getName().equals("requireProjection"))
                .allMatch(method -> method.getParameterCount() == 1));
    }

    @Test
    void observationsRequireAnExplicitBackend() {
        assertEquals(1, SqlExecutionObservation.class.getConstructors().length);
        assertEquals(1, BatchWriteRequestView.class.getConstructors().length);
        assertEquals(List.of(SqlExecutionBackend.JDBC, SqlExecutionBackend.R2DBC),
                List.of(SqlExecutionBackend.values()));
    }

    @Test
    void formOperationsUseTheSharedConfiguration() {
        assertEquals(1, SyncFormOperations.class.getDeclaredConstructors().length);
        assertEquals(FormConfiguration.class,
                SyncFormOperations.class.getDeclaredConstructors()[0].getParameterTypes()[1]);
    }

    @Test
    void dialectCapabilitiesHaveOneRepresentation() {
        assertFalse(Arrays.stream(RdbDialect.class.getDeclaredFields())
                .anyMatch(field -> field.getName().equals("features")));
    }

    @Test
    void entitySchemaHasOneCompletePlanningPath() {
        assertTrue(Arrays.stream(com.flying.orm.rdb.schema.EntitySchemaSynchronizer.class.getMethods())
                .filter(method -> method.getName().startsWith("synchronize"))
                .allMatch(method -> method.getParameterTypes()[0]
                        == com.flying.orm.rdb.dialect.DatabaseDescriptor.class));
    }

    @Test
    void dialectSupportsCustomIdsThroughTheSameEntryPoint() throws ReflectiveOperationException {
        DialectCapabilityId custom = DialectCapabilityId.of("custom.feature");
        RdbDialect base = RdbDialect.postgresql();
        RdbDialect dialect = RdbDialect.of("custom", base.schema(), base.pagination(),
                base.upsert(), base.json(), "1", DialectCapabilities.of(custom), 0);
        assertEquals(true, RdbDialect.class.getMethod("supports", DialectCapabilityId.class)
                .invoke(dialect, custom));
    }
}
