package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtectedPhysicalNamingTest {
    @Test
    void qualifiedEntityAnnotationPreservesItsPhysicalNamingSource() {
        DynamicForm form = EntityMetadataResolver.createUncached(QualifiedUser.class).toDynamicForm();
        assertEquals("app.users", form.namingSource());
        assertEquals(ProtectedColumnNames.containsTable(form.id(), "app.users"),
                ProtectedContainsLayout.resolve(form).orElseThrow().table().relationIdentity().table());
    }

    @TableName("app.users")
    static class QualifiedUser {
        @TableId
        Long id;
        @EncryptedField(search = EncryptedSearchMode.CONTAINS)
        String name;
    }

    @Test
    void segmentedIdentityDoesNotRenamePersistedSideTableAndIndexes() {
        verify(DynamicForm.relationalBuilder("f", RelationIdentity.of(null, "app", "users")), "users");
    }

    @Test
    void qualifiedTextDoesNotRenamePersistedSideTableAndIndexes() {
        verify(DynamicForm.builder("f", "app.users"), "app.users");
    }

    private void verify(DynamicForm.Builder builder, String namingSource) {
        DynamicForm form = builder.addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(128))
                .encrypted("name", EncryptedFieldDefinition.builder().searchModes(EncryptedSearchMode.CONTAINS).build())
                .build();
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        assertEquals(ProtectedColumnNames.containsTable("f", namingSource), layout.table().relationIdentity().table());
        assertEquals("app", layout.table().relationIdentity().schema().orElseThrow());
        assertEquals(ProtectedColumnNames.containsQueryIndex("f", namingSource), layout.indexes().get(0).name());
        assertEquals(ProtectedColumnNames.containsUniqueIndex("f", namingSource), layout.indexes().get(1).name());
        assertEquals(ProtectedColumnNames.containsForeignKey("f", namingSource), layout.foreignKeys().getFirst().name());
        assertEquals(namingSource, ProtectedFormLayout.physical(form).namingSource());
    }
}
