package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.schema.ReviewedSchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定 DDL 草稿的唯一所有者，同时保护 3.3.0 已公开的 fluent 方法描述符。 */
class DdlBuilderSurfaceConvergenceTest {

    @Test
    void oneNeutralDraftOwnsSyncAndReactiveStructureState() {
        assertAll(
                () -> assertDoesNotThrow(() -> Class.forName(
                        "com.flying.orm.rdb.operator.DdlStructureDraft")),
                () -> assertThrows(ClassNotFoundException.class, () -> Class.forName(
                        "com.flying.orm.rdb.operator.SyncDdlStructureState")));
    }

    @Test
    void publicFluentTypesAndMethodDescriptorsRemainStable() {
        assertAll(
                () -> assertFinal(CreateOrAlterTableBuilder.class),
                () -> assertFinal(SyncCreateOrAlterTableBuilder.class),
                () -> assertFinal(ColumnBuilder.class),
                () -> assertFinal(SyncColumnBuilder.class),
                () -> assertFinal(IndexBuilder.class),
                () -> assertFinal(SyncIndexBuilder.class),
                () -> assertFinal(ForeignKeyBuilder.class),
                () -> assertFinal(SyncForeignKeyBuilder.class));

        assertAll(
                () -> assertMethod(CreateOrAlterTableBuilder.class, "addColumn", ColumnBuilder.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "addIndex", IndexBuilder.class, String.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "addForeignKey", ForeignKeyBuilder.class,
                                   String.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "options", CreateOrAlterTableBuilder.class,
                                   com.flying.orm.rdb.schema.SchemaMigrationOptions.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "commit", Mono.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "commitDetailed", Mono.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "plan", Mono.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "review", Mono.class,
                                   SchemaMigrationReviewPolicy.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "executeReviewed", Mono.class,
                                   ReviewedSchemaMigrationPlan.class, SchemaMigrationExecutionOptions.class),
                () -> assertMethod(CreateOrAlterTableBuilder.class, "executeReviewed", Mono.class,
                                   ReviewedSchemaMigrationPlan.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "addColumn", SyncColumnBuilder.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "addIndex", SyncIndexBuilder.class,
                                   String.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "addForeignKey", SyncForeignKeyBuilder.class,
                                   String.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "options",
                                   SyncCreateOrAlterTableBuilder.class,
                                   com.flying.orm.rdb.schema.SchemaMigrationOptions.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "commit", long.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "commitDetailed",
                                   SchemaMigrationResult.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "plan", SchemaMigrationPlan.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "review",
                                   ReviewedSchemaMigrationPlan.class, SchemaMigrationReviewPolicy.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "executeReviewed",
                                   SchemaMigrationResult.class, ReviewedSchemaMigrationPlan.class,
                                   SchemaMigrationExecutionOptions.class),
                () -> assertMethod(SyncCreateOrAlterTableBuilder.class, "executeReviewed",
                                   SchemaMigrationResult.class, ReviewedSchemaMigrationPlan.class));

        assertColumnMethods(ColumnBuilder.class, CreateOrAlterTableBuilder.class);
        assertColumnMethods(SyncColumnBuilder.class, SyncCreateOrAlterTableBuilder.class);
        assertIndexMethods(IndexBuilder.class, CreateOrAlterTableBuilder.class);
        assertIndexMethods(SyncIndexBuilder.class, SyncCreateOrAlterTableBuilder.class);
        assertForeignKeyMethods(ForeignKeyBuilder.class, CreateOrAlterTableBuilder.class);
        assertForeignKeyMethods(SyncForeignKeyBuilder.class, SyncCreateOrAlterTableBuilder.class);
    }

    private static void assertColumnMethods(Class<?> type, Class<?> parent) {
        assertAll(
                () -> assertMethod(type, "name", type, String.class),
                () -> assertMethod(type, "number", type, int.class),
                () -> assertMethod(type, "varchar", type, int.class),
                () -> assertMethod(type, "primaryKey", type),
                () -> assertMethod(type, "comment", type, String.class),
                () -> assertMethod(type, "commit", parent));
    }

    private static void assertIndexMethods(Class<?> type, Class<?> parent) {
        assertAll(
                () -> assertMethod(type, "unique", type),
                () -> assertMethod(type, "column", type, String.class),
                () -> assertMethod(type, "columns", type, String.class, String[].class),
                () -> assertMethod(type, "commit", parent));
    }

    private static void assertForeignKeyMethods(Class<?> type, Class<?> parent) {
        assertAll(
                () -> assertMethod(type, "column", type, String.class),
                () -> assertMethod(type, "columns", type, String.class, String[].class),
                () -> assertMethod(type, "referenceTable", type, String.class),
                () -> assertMethod(type, "referenceColumn", type, String.class),
                () -> assertMethod(type, "referenceColumns", type, String.class, String[].class),
                () -> assertMethod(type, "commit", parent));
    }

    private static void assertFinal(Class<?> type) {
        assertTrue(Modifier.isPublic(type.getModifiers()), type.getName());
        assertTrue(Modifier.isFinal(type.getModifiers()), type.getName());
    }

    private static void assertMethod(Class<?> type,
                                     String name,
                                     Class<?> returnType,
                                     Class<?>... parameterTypes) throws NoSuchMethodException {
        Method method = type.getDeclaredMethod(name, parameterTypes);
        assertTrue(Modifier.isPublic(method.getModifiers()), method.toString());
        assertEquals(returnType, method.getReturnType(), method.toString());
    }
}
