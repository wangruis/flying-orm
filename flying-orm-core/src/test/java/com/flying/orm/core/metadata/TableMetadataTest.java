package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证表元数据的高频读取行为，重点覆盖规范化名称查找、主键顺序和只读发布约束。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class TableMetadataTest {

    /**
     * 验证列名查找不受大小写和首尾空白影响，并且构建后的列集合不能被外部修改。
     */
    @Test
    void findsColumnByNormalizedNameAndKeepsPublishedMetadataReadOnly() {
        TableMetadata table = TableMetadata.builder("Users")
                                           .addColumn(ColumnMetadata.primaryKey("ID", "BIGINT"))
                                           .addColumn(ColumnMetadata.of("UserName", "VARCHAR"))
                                           .build();

        Optional<ColumnMetadata> column = table.findColumn(" username ");

        assertTrue(column.isPresent());
        assertEquals("UserName", column.get().name());
        assertSame(column.get(), table.column("USERNAME"));
        assertEquals(List.of("ID"), table.primaryKeyColumns().stream().map(ColumnMetadata::name).toList());
        assertThrows(UnsupportedOperationException.class,
                     () -> table.columns().add(ColumnMetadata.of("Age", "INTEGER")));
    }

    /**
     * 验证索引元数据按规范化名称查找，并保留声明时的索引列顺序。
     */
    @Test
    void findsIndexByNormalizedNameAndKeepsIndexColumnsOrdered() {
        IndexMetadata userNameIndex = IndexMetadata.builder("idx_user_name")
                                                   .unique()
                                                   .addColumn("tenant_id")
                                                   .addColumn("UserName")
                                                   .build();
        TableMetadata table = TableMetadata.builder("Users")
                                           .addColumn(ColumnMetadata.primaryKey("ID", "BIGINT"))
                                           .addColumn(ColumnMetadata.of("tenant_id", "BIGINT"))
                                           .addColumn(ColumnMetadata.of("UserName", "VARCHAR"))
                                           .addIndex(userNameIndex)
                                           .build();

        assertSame(userNameIndex, table.index(" IDX_USER_NAME "));
        assertEquals(List.of("tenant_id", "UserName"), table.index("idx_user_name").columns());
        assertThrows(UnsupportedOperationException.class,
                     () -> table.indexes().add(IndexMetadata.builder("other").addColumn("id").build()));
    }

    /** 验证发布索引元数据前必须声明至少一个索引列，避免把无效结构推迟到 DDL 渲染阶段。 */
    @Test
    void rejectsIndexWithoutColumnsWithoutExposingItsName() {
        String secretName = "idx_SECRET_TENANT_TOKEN";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> IndexMetadata.builder(secretName).build());

        assertFalse(error.getMessage().contains(secretName));
    }

    @Test
    void findsForeignKeyByNormalizedNameAndKeepsColumnsOrdered() {
        ForeignKeyMetadata orgForeignKey = ForeignKeyMetadata.builder("fk_user_org")
                                                             .addColumn("tenant_id")
                                                             .addColumn("org_id")
                                                             .referenceTable("organizations")
                                                             .addReferenceColumn("tenant_id")
                                                             .addReferenceColumn("id")
                                                             .build();
        TableMetadata table = TableMetadata.builder("Users")
                                           .addColumn(ColumnMetadata.primaryKey("ID", "BIGINT"))
                                           .addColumn(ColumnMetadata.of("tenant_id", "BIGINT"))
                                           .addColumn(ColumnMetadata.of("org_id", "BIGINT"))
                                           .addForeignKey(orgForeignKey)
                                           .build();

        assertSame(orgForeignKey, table.foreignKey(" FK_USER_ORG "));
        assertEquals(List.of("tenant_id", "org_id"), table.foreignKey("fk_user_org").columns());
        assertEquals("organizations", table.foreignKey("fk_user_org").referenceTable());
        assertEquals(List.of("tenant_id", "id"), table.foreignKey("fk_user_org").referenceColumns());
        assertThrows(UnsupportedOperationException.class,
                     () -> table.foreignKeys().add(orgForeignKey));
    }

    @Test
    void canonicalForeignKeyConstructorRebuildsDerivedNamesAndValidatesColumns() {
        ForeignKeyMetadata foreignKey = new ForeignKeyMetadata(
                " FK_User_Org ", "wrong-name", List.of(" tenant_id "),
                " Organizations ", "wrong-table", List.of(" id "));
        TableMetadata table = TableMetadata.builder("Users")
                                           .addForeignKey(foreignKey)
                                           .build();

        assertSame(foreignKey, table.foreignKey("fk_user_org"));
        assertEquals("organizations", foreignKey.normalizedReferenceTable());
        assertEquals(List.of("tenant_id"), foreignKey.columns());
        assertEquals(List.of("id"), foreignKey.referenceColumns());
        assertThrows(IllegalArgumentException.class,
                     () -> new ForeignKeyMetadata("fk", "ignored", List.of(" "),
                                                  "parents", "ignored", List.of("id")));
    }

    /** 验证外键列校验失败不回显调用方提供的外键名称。 */
    @Test
    void foreignKeyValidationFailureDoesNotExposeName() {
        String secretName = "fk_SECRET_TENANT_TOKEN";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new ForeignKeyMetadata(secretName, List.of(), "parents", List.of("id")));

        assertFalse(error.getMessage().contains(secretName));
    }

    /** 验证按名称获取元数据失败时不回显调用方提供的任意名称。 */
    @Test
    void missingMetadataFailuresDoNotExposeLookupNames() {
        TableMetadata table = TableMetadata.builder("Users").build();
        String secretColumn = "SECRET_COLUMN_TOKEN";
        String secretIndex = "SECRET_INDEX_TOKEN";
        String secretForeignKey = "SECRET_FOREIGN_KEY_TOKEN";

        IllegalArgumentException columnError = assertThrows(IllegalArgumentException.class,
                () -> table.column(secretColumn));
        IllegalArgumentException indexError = assertThrows(IllegalArgumentException.class,
                () -> table.index(secretIndex));
        IllegalArgumentException foreignKeyError = assertThrows(IllegalArgumentException.class,
                () -> table.foreignKey(secretForeignKey));

        assertFalse(columnError.getMessage().contains(secretColumn));
        assertFalse(indexError.getMessage().contains(secretIndex));
        assertFalse(foreignKeyError.getMessage().contains(secretForeignKey));
    }

    /** 验证重复列定义的构建失败不回显配置中的物理列名。 */
    @Test
    void duplicateColumnDefinitionDoesNotExposeName() {
        String secretColumn = "SECRET_COLUMN_TOKEN";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TableMetadata.builder("users")
                                   .addColumn(ColumnMetadata.of(secretColumn, "VARCHAR"))
                                   .addColumn(ColumnMetadata.of(secretColumn, "VARCHAR"))
                                   .build());

        assertEquals("duplicate column name", error.getMessage());
    }

    /** 验证重复索引定义的构建失败不回显配置中的物理索引名。 */
    @Test
    void duplicateIndexDefinitionDoesNotExposeName() {
        String secretIndex = "SECRET_INDEX_TOKEN";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TableMetadata.builder("users")
                                   .addIndex(IndexMetadata.builder(secretIndex).addColumn("id").build())
                                   .addIndex(IndexMetadata.builder(secretIndex).addColumn("id").build())
                                   .build());

        assertEquals("duplicate index name", error.getMessage());
    }

    /** 验证重复外键定义的构建失败不回显配置中的物理外键名。 */
    @Test
    void duplicateForeignKeyDefinitionDoesNotExposeName() {
        String secretForeignKey = "SECRET_FOREIGN_KEY_TOKEN";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> TableMetadata.builder("users")
                                   .addForeignKey(ForeignKeyMetadata.builder(secretForeignKey)
                                                                   .addColumn("parent_id")
                                                                   .referenceTable("parents")
                                                                   .addReferenceColumn("id")
                                                                   .build())
                                   .addForeignKey(ForeignKeyMetadata.builder(secretForeignKey)
                                                                   .addColumn("parent_id")
                                                                   .referenceTable("parents")
                                                                   .addReferenceColumn("id")
                                                                   .build())
                                   .build());

        assertEquals("duplicate foreign key name", error.getMessage());
    }
}
