package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UniqueNullPolicyProjectionTest {

    @Test
    void protectedUniqueRetainsDistinctNullPolicyOnItsStableHashColumn() {
        var descriptor = EntitySchemaDescriptor.builder(Account.class)
                .unique("uq_mobile", distinct()).build();
        var unique = descriptor.table().uniqueConstraints().getFirst();
        assertEquals(UniqueNullPolicy.DISTINCT, unique.nullPolicy());
        assertEquals("uq_mobile", unique.name());
        assertNotEquals(List.of("mobile"), unique.columns());
        assertTrue(ProtectedFormLayout.isHashType(descriptor.table()
                .column(unique.columns().getFirst()).databaseType()));
        assertTrue(descriptor.table().column(unique.columns().getFirst()).nullable());
        assertTrue(descriptor.form().field("mobile").unique());
    }

    @Test
    void entityAnnotationPublishesDistinctThroughTheSameProtectedProjection() {
        var annotated = EntitySchemaDescriptor.builder(AnnotatedAccount.class).build();
        var programmatic = EntitySchemaDescriptor.builder(AnnotatedAccount.class)
                .unique("uq_mobile", distinct()).build();

        assertEquals(programmatic.table().uniqueConstraints(), annotated.table().uniqueConstraints());
        assertEquals(programmatic.relationalFingerprint(), annotated.relationalFingerprint());
        assertEquals(UniqueNullPolicy.DISTINCT, annotated.table().uniqueConstraints().getFirst().nullPolicy());
    }

    @Test
    void distinctPolicyIsNotEqualToDefaultInSqlServerSchemaComparisons() {
        UniqueConstraintDefinition distinct = distinct();
        assertFalse(SchemaDefinitionEquality.sameUnique(
                UniqueConstraintDefinition.of("uq_mobile", "mobile"), distinct, RdbDialect.sqlServer().schema()));
        assertTrue(SchemaDefinitionEquality.sameUnique(distinct, distinct, RdbDialect.sqlServer().schema()));
    }

    private static UniqueConstraintDefinition distinct() {
        return new UniqueConstraintDefinition("uq_mobile", List.of("mobile"), UniqueNullPolicy.DISTINCT);
    }

    @TableName("protected_nullable_accounts")
    private static class Account {
        @TableId
        private Long id;

        @EncryptedField(search = EncryptedSearchMode.EXACT)
        @TableColumn(databaseTypeId = "VARCHAR", length = 64)
        private String mobile;
    }

    @TableName("protected_nullable_accounts")
    @TableUnique(id = "uq_mobile", properties = "mobile", nullPolicy = UniqueNullPolicy.DISTINCT)
    private static final class AnnotatedAccount extends Account {
    }
}
