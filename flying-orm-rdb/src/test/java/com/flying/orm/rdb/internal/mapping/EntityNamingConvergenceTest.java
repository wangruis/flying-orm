package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityNamingConvergenceTest {

    @Test
    void fixedNamingDoesNotNeedAStrategyInterfaceOrImplementation() {
        for (String name : new String[]{"EntityNamingStrategy", "SnakeCaseEntityNamingStrategy"}) {
            assertThrows(ClassNotFoundException.class,
                    () -> Class.forName("com.flying.orm.rdb.internal.mapping." + name));
        }
    }

    @Test
    void tableResolverDoesNotKeepUnusedStringForwarders() {
        assertFalse(Arrays.stream(EntityTableNameResolver.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().equals("resolve")
                        || method.getName().equals("resolveTable") && method.getParameterCount() != 1));
    }

    @Test
    void defaultNamesStayIdenticalForCrudAndSchema() {
        EntityMetadata<URLRecord> crud = EntityMetadataResolver.createUncached(URLRecord.class);
        EntityMetadata<URLRecord> schema = EntitySchemaDescriptor.builder(URLRecord.class).build().metadata();
        for (EntityMetadata<URLRecord> metadata : java.util.List.of(crud, schema)) {
            assertEquals("u_r_l_record", metadata.formId());
            assertEquals("u_r_l_record", metadata.table());
            assertEquals("user_i_d", metadata.field("userID").columnName());
            assertEquals("already_snake", metadata.field("already_snake").columnName());
        }
    }

    @Test
    void explicitAnnotationsKeepTheirPrecedenceAndNamespace() {
        EntityMetadata<ExplicitRecord> crud = EntityMetadataResolver.createUncached(ExplicitRecord.class);
        EntityMetadata<ExplicitRecord> schema = EntitySchemaDescriptor.builder(ExplicitRecord.class).build().metadata();
        for (EntityMetadata<ExplicitRecord> metadata : java.util.List.of(crud, schema)) {
            assertEquals("explicit_record", metadata.formId());
            assertEquals("AuditLog", metadata.table());
            assertEquals("app", metadata.toDynamicForm().relationIdentity().schema().orElseThrow());
            assertEquals("PrimaryID", metadata.field("id").columnName());
            assertEquals("DisplayName", metadata.field("displayName").columnName());
        }
        assertEquals(crud.structureFingerprint(), schema.structureFingerprint());
    }

    static class URLRecord {
        @TableId
        long userID;
        String already_snake;
    }

    @TableCatalog("archive")
    @TableName(value = "AuditLog", schema = "app")
    static class ExplicitRecord {
        @TableId("PrimaryID")
        @TableField("IgnoredID")
        long id;
        @TableField("DisplayName")
        String displayName;
    }
}
