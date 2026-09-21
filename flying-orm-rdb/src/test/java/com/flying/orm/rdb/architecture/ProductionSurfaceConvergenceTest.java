package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 防止已经收回所有权的内部实现再次以独立生产类型回流。 */
class ProductionSurfaceConvergenceTest {

    private static final List<String> DIALECT_METADATA_DEFINITIONS = List.of(
            "com.flying.orm.rdb.metadata.H2MetadataQueries",
            "com.flying.orm.rdb.metadata.MySqlMetadataQueries",
            "com.flying.orm.rdb.metadata.PostgreSqlMetadataQueries",
            "com.flying.orm.rdb.metadata.OracleMetadataQueries",
            "com.flying.orm.rdb.metadata.SqlServerMetadataQueries");

    private static final List<String> OBSOLETE_DIALECT_READERS = List.of(
            "com.flying.orm.rdb.metadata.H2ReactiveFormMetadataReader",
            "com.flying.orm.rdb.metadata.MySqlReactiveFormMetadataReader",
            "com.flying.orm.rdb.metadata.PostgreSqlReactiveFormMetadataReader",
            "com.flying.orm.rdb.metadata.OracleReactiveFormMetadataReader",
            "com.flying.orm.rdb.metadata.SqlServerReactiveFormMetadataReader");

    private static final List<String> OBSOLETE_METADATA_QUERY_MARKERS = List.of(
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$ColumnQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$IndexQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$ForeignKeyQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$TableQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$PrimaryKeyQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$UniqueConstraintQuery",
            "com.flying.orm.rdb.metadata.InformationSchemaFormMetadataReader$CheckConstraintQuery");

    @Test
    void obsoleteTenantResolverIsAbsent() {
        assertProductionTypeAbsent("com.flying.orm.rdb.internal.mapping.EntityTenantResolver");
    }

    @Test
    void syncFormClientHasNoSingleImplementationRuntimeLayer() {
        assertProductionTypeAbsent("com.flying.orm.rdb.form.SyncFormRuntime");
        assertProductionTypeAbsent("com.flying.orm.rdb.form.JdbcSyncFormRuntime");
    }

    @Test
    void protectedQueryPlanningHasNoForwardingRequestType() {
        assertProductionTypeAbsent("com.flying.orm.rdb.form.FormProtectionQueryRequests");
    }

    @Test
    void dialectMetadataDefinitionsHaveNoReaderInstanceLayer() throws ClassNotFoundException {
        for (String typeName : DIALECT_METADATA_DEFINITIONS) {
            Class<?> type = Class.forName(typeName, false,
                                          ProductionSurfaceConvergenceTest.class.getClassLoader());
            assertTrue(java.util.Arrays.stream(type.getDeclaredFields())
                               .allMatch(field -> Modifier.isStatic(field.getModifiers())), typeName);
            assertTrue(java.util.Arrays.stream(type.getDeclaredMethods())
                               .allMatch(method -> Modifier.isStatic(method.getModifiers())), typeName);
        }
    }

    @Test
    void obsoleteDialectReaderTypeNamesAreAbsent() {
        OBSOLETE_DIALECT_READERS.forEach(ProductionSurfaceConvergenceTest::assertProductionTypeAbsent);
    }

    @Test
    void metadataQueriesUseOneFunctionalContract() {
        OBSOLETE_METADATA_QUERY_MARKERS.forEach(
                ProductionSurfaceConvergenceTest::assertProductionTypeAbsent);
    }

    @Test
    void batchObservationHasNoDetachedFactoryOrClassifier() {
        assertProductionTypeAbsent("com.flying.orm.rdb.observation.BatchExecutionObservationFactory");
        assertProductionTypeAbsent("com.flying.orm.rdb.observation.BatchObservationClassification");
    }

    private static void assertProductionTypeAbsent(String typeName) {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName(typeName, false,
                                         ProductionSurfaceConvergenceTest.class.getClassLoader()));
    }
}
