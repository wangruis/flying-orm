package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableCheck;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.MappingException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedRelationalSchemaRejectionTest {

    @Test
    void rejectsProtectedRelationalSemanticsThatCiphertextCannotPreserve() {
        for (Class<?> entityType : List.of(
                ProtectedPrimaryKey.class,
                ProtectedDefault.class,
                ProtectedGeneration.class,
                UniqueWithoutExact.class,
                IndexWithoutExact.class,
                CompositeUnique.class,
                CompositeIndex.class,
                ProtectedForeignKey.class,
                ReferencesProtectedTarget.class,
                ProtectedCheck.class)) {
            assertThrows(MappingException.class,
                    () -> EntitySchemaDescriptor.builder(entityType).build(), entityType.getName());
        }
    }

    @TableName("protected_pk")
    private static final class ProtectedPrimaryKey {
        @TableId
        @EncryptedField
        private String id;
    }

    @TableName("protected_default")
    private static final class ProtectedDefault {
        @TableId
        private Long id;
        @EncryptedField
        @TableColumn(defaultId = "CURRENT_TIMESTAMP")
        private String secret;
    }

    @TableName("protected_generation")
    private static final class ProtectedGeneration {
        @TableId
        private Long id;
        @EncryptedField
        @TableColumn(generation = TableColumn.Generation.IDENTITY)
        private String secret;
    }

    @TableName("unique_without_exact")
    @TableUnique(id = "secret", name = "uq_secret", properties = "secret")
    private static final class UniqueWithoutExact {
        @TableId
        private Long id;
        @EncryptedField(search = {})
        private String secret;
    }

    @TableName("index_without_exact")
    @TableIndex(id = "secret", name = "idx_secret",
            columns = @TableIndexColumn(property = "secret"))
    private static final class IndexWithoutExact {
        @TableId
        private Long id;
        @EncryptedField(search = {})
        private String secret;
    }

    @TableName("composite_unique")
    @TableUnique(id = "secret", name = "uq_secret_scope", properties = {"scope", "secret"})
    private static final class CompositeUnique {
        @TableId
        private Long id;
        private String scope;
        @EncryptedField
        private String secret;
    }

    @TableName("composite_index")
    @TableIndex(id = "secret", name = "idx_secret_scope",
            columns = {@TableIndexColumn(property = "scope"),
                    @TableIndexColumn(property = "secret")})
    private static final class CompositeIndex {
        @TableId
        private Long id;
        private String scope;
        @EncryptedField
        private String secret;
    }

    @TableName("protected_fk")
    @TableForeignKey(id = "secret", name = "fk_secret",
            localProperties = "secret", targetEntity = PlainTarget.class,
            targetProperties = "id")
    private static final class ProtectedForeignKey {
        @TableId
        private Long id;
        @EncryptedField
        private String secret;
    }

    @TableName("protected_check")
    @TableCheck(id = "secret", name = "ck_secret", property = "secret",
            operator = TableCheck.Operator.IS_NOT_NULL)
    private static final class ProtectedCheck {
        @TableId
        private Long id;
        @EncryptedField
        private String secret;
    }

    @TableName("references_protected_target")
    @TableForeignKey(id = "secret", name = "fk_target_secret",
            localProperties = "secret", targetEntity = ProtectedTarget.class,
            targetProperties = "secret")
    private static final class ReferencesProtectedTarget {
        @TableId
        private Long id;
        private String secret;
    }

    @TableName("protected_target")
    @TableUnique(id = "secret", properties = "secret")
    private static final class ProtectedTarget {
        @TableId
        private Long id;
        @EncryptedField
        private String secret;
    }

    @TableName("plain_target")
    @TablePrimaryKey(name = "pk_plain_target", properties = "id")
    private static final class PlainTarget {
        @TableId
        private String id;
    }
}
