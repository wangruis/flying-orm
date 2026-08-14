package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 CONTAINS 侧索引的物理布局、主键约束和写入令牌快照。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedContainsLayoutTest {

    @Test
    void createsOneBoundedTokenTablePerProtectedBusinessTable() {
        DynamicForm form = protectedForm();

        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();

        assertTrue(layout.table().table().length() <= 30);
        assertEquals(3, layout.table().fields().size());
        assertFalse(layout.table().field("id").primaryKey());
        assertEquals("VARCHAR", layout.table().field("field_tag").dataType());
        assertEquals("PROTECTED_HASH", layout.table().field("token_hash").dataType());
        assertEquals(2, layout.indexes().size());
        assertFalse(layout.indexes().getFirst().unique());
        assertTrue(layout.indexes().getLast().unique());
        assertEquals(1, layout.foreignKeys().size());
        assertEquals("customer", layout.foreignKeys().getFirst().referenceTable());
        assertEquals(java.util.List.of("id"), layout.foreignKeys().getFirst().referenceColumns());
    }

    @Test
    void keepsContainsTableInTheBusinessTableSchema() {
        DynamicForm form = DynamicForm.builder("customer", "secure.customer")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("contact", "VARCHAR"))
                                      .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                     .searchModes(
                                                                                             EncryptedSearchMode.CONTAINS)
                                                                                     .build())
                                      .build();

        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();

        assertTrue(layout.table().table().startsWith("secure.__fop_c_"));
        assertTrue(layout.table().table().substring("secure.".length()).length() <= 30);
        assertEquals("secure.customer", layout.foreignKeys().getFirst().referenceTable());
    }

    @Test
    void snapshotsContainsTokensWithThePreparedWrite() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(7))) {
            ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys);
            ProtectedFieldRuntime.PreparedWrite write = runtime.prepareWrite(
                    protectedForm(),
                    Map.of("id", 9L, "contact", "ABABA"),
                    DataScope.none(),
                    ValueCodecRegistry.standard());

            ProtectedFieldRuntime.ContainsFieldTokens field = runtime.prepareContainsTokens(
                    protectedForm(), Map.of("id", 9L, "contact", "ABABA"), DataScope.none(),
                    ValueCodecRegistry.standard()).getFirst();
            byte[] expected = field.tokens().getFirst();
            field.tokens().getFirst()[0] ^= 0x7f;

            assertEquals("contact", field.fieldName());
            assertTrue(field.fieldTag().length() <= 30);
            assertEquals(2, field.tokens().size());
            assertArrayEquals(expected, runtime.prepareContainsTokens(
                    protectedForm(), Map.of("id", 9L, "contact", "ABABA"), DataScope.none(),
                    ValueCodecRegistry.standard()).getFirst().tokens().getFirst());
        }
    }

    @Test
    void rejectsContainsWithoutAStablePrimaryKey() {
        DynamicForm form = DynamicForm.builder("note", "note")
                                      .addField(DynamicField.of("body", "VARCHAR"))
                                      .encrypted("body", EncryptedFieldDefinition.builder()
                                                                                   .searchModes(
                                                                                           EncryptedSearchMode.CONTAINS)
                                                                                   .build())
                                      .build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> ProtectedContainsLayout.resolve(form));

        assertEquals("protected contains search requires a primary key", error.getMessage());
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                                 EncryptedSearchMode.CONTAINS)
                                                                         .normalizer("case-fold")
                                                                         .build())
                          .build();
    }

    private static byte[] key(int seed) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) seed);
        return key;
    }
}
