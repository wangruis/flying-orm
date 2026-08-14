package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证数据库和 Schema 元数据的规范化索引行为，保证高频读取路径可稳定复用。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class DatabaseMetadataTest {

    /**
     * 验证 schema 与 table 均可通过规范化名称查找，并且发布后的集合不可变。
     */
    @Test
    void findsSchemaAndTableByNormalizedNameAndExposesCurrentSchema() {
        TableMetadata users = TableMetadata.builder("Users")
                                           .addColumn(ColumnMetadata.primaryKey("ID", "BIGINT"))
                                           .build();
        SchemaMetadata schema = SchemaMetadata.builder("Public")
                                              .addTable(users)
                                              .build();
        DatabaseMetadata database = DatabaseMetadata.builder("main")
                                                    .addSchema(schema)
                                                    .currentSchema(" PUBLIC ")
                                                    .build();

        assertTrue(database.findSchema("public").isPresent());
        assertSame(schema, database.schema(" PUBLIC "));
        assertSame(schema, database.currentSchema());
        assertSame(users, schema.table(" users "));
        assertSame(users, database.table("public", "USERS"));
        assertEquals("Public", database.currentSchema().name());
        assertThrows(UnsupportedOperationException.class,
                     () -> database.schemas().add(SchemaMetadata.builder("other").build()));
        assertThrows(UnsupportedOperationException.class,
                     () -> schema.tables().add(TableMetadata.builder("other").build()));
    }

    /** 验证查找失败不回显调用方提供的 schema 名称。 */
    @Test
    void missingSchemaFailureDoesNotExposeLookupInput() {
        DatabaseMetadata database = DatabaseMetadata.builder("main").build();
        String secretSchema = "SECRET_TENANT_SCHEMA";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> database.schema(secretSchema));

        assertFalse(error.getMessage().contains(secretSchema));
    }

    /** 验证 schema 中表查找失败不回显调用方提供的名称。 */
    @Test
    void missingTableFailureDoesNotExposeLookupInput() {
        SchemaMetadata schema = SchemaMetadata.builder("public").build();
        String secretTable = "SECRET_TENANT_TABLE";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> schema.table(secretTable));

        assertFalse(error.getMessage().contains(secretTable));
    }

    /** 验证未配置当前 Schema 的失败说明不回显可由公开构建器传入的数据库名。 */
    @Test
    void missingCurrentSchemaFailureDoesNotExposeDatabaseName() {
        String secretDatabase = "secret-database-" + "x".repeat(5_000);
        DatabaseMetadata database = DatabaseMetadata.builder(secretDatabase).build();

        IllegalStateException error = assertThrows(IllegalStateException.class, database::currentSchema);

        assertEquals("database has no current schema", error.getMessage());
    }

    /**
     * 已引用的物理标识符可以仅靠大小写区分。精确名称必须优先命中；没有精确命中而折叠名称有歧义时，不能猜测目标。
     */
    @Test
    void preservesCaseDistinctSchemaAndTableNamesWithoutAmbiguousFoldedLookup() {
        TableMetadata users = TableMetadata.builder("Users").build();
        TableMetadata lowerUsers = TableMetadata.builder("users").build();
        SchemaMetadata publicSchema = SchemaMetadata.builder("Public")
                                                      .addTable(users)
                                                      .addTable(lowerUsers)
                                                      .build();
        SchemaMetadata lowerPublicSchema = SchemaMetadata.builder("public").build();
        DatabaseMetadata database = DatabaseMetadata.builder("main")
                                                    .addSchema(publicSchema)
                                                    .addSchema(lowerPublicSchema)
                                                    .build();

        assertSame(publicSchema, database.schema("Public"));
        assertSame(lowerPublicSchema, database.schema("public"));
        assertFalse(database.findSchema("PUBLIC").isPresent());
        assertSame(users, publicSchema.table("Users"));
        assertSame(lowerUsers, publicSchema.table("users"));
        assertFalse(publicSchema.findTable("USERS").isPresent());
    }

    /** 重复物理标识符的构建失败只给稳定分类，不能把配置输入回显到公开异常。 */
    @Test
    void duplicatePhysicalMetadataNamesDoNotExposeInput() {
        String secretTable = "SECRET_TABLE_NAME";
        String secretSchema = "SECRET_SCHEMA_NAME";

        IllegalArgumentException tableError = assertThrows(IllegalArgumentException.class,
                () -> SchemaMetadata.builder("public")
                                    .addTable(TableMetadata.builder(secretTable).build())
                                    .addTable(TableMetadata.builder(secretTable).build())
                                    .build());
        IllegalArgumentException schemaError = assertThrows(IllegalArgumentException.class,
                () -> DatabaseMetadata.builder("main")
                                      .addSchema(SchemaMetadata.builder(secretSchema).build())
                                      .addSchema(SchemaMetadata.builder(secretSchema).build())
                                      .build());

        assertEquals("duplicate table name", tableError.getMessage());
        assertEquals("duplicate schema name", schemaError.getMessage());
    }
}
