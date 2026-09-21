package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** 防止只服务 Schema 包内部编排的实现引擎重新成为公开契约。 */
class SchemaEngineVisibilityConvergenceTest {

    private static final List<String> INTERNAL_SCHEMA_ENGINES = List.of(
            "com.flying.orm.rdb.schema.SchemaDependencyGraph",
            "com.flying.orm.rdb.schema.SchemaStronglyConnectedComponents",
            "com.flying.orm.rdb.schema.SchemaRiskClassifier",
            "com.flying.orm.rdb.schema.SchemaSnapshotFingerprint",
            "com.flying.orm.rdb.schema.RelationalSchemaPlanReviewer",
            "com.flying.orm.rdb.schema.SchemaMigrationReviewer",
            "com.flying.orm.rdb.schema.VerifiedSchemaPlanExecutor");

    @Test
    void schemaImplementationEnginesAreNotPublicApi() throws ClassNotFoundException {
        for (String typeName : INTERNAL_SCHEMA_ENGINES) {
            Class<?> type = Class.forName(typeName, false,
                                          SchemaEngineVisibilityConvergenceTest.class.getClassLoader());
            assertFalse(Modifier.isPublic(type.getModifiers()), typeName);
        }
    }
}
