package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 CONTAINS 条件提取不会改变其余业务条件，并拒绝无法保持集合语义的 OR 组合。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedContainsQueryTest {

    @Test
    void extractsOneContainsTermAndKeepsTheRemainingAndConditions() {
        ConditionGroup where = ConditionGroup.and()
                                             .add(ProtectedConditions.contains("contact", "ABA"))
                                             .add(TermCondition.of("status", "eq", "ACTIVE"))
                                             .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                                                               .current("v2", key(2))
                                                               .readable("v1", key(1))
                                                               .build()) {
            ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys);

            ProtectedFieldRuntime.PreparedContainsQuery query = runtime.prepareContainsQuery(
                    protectedForm(), where, DataScope.none(), ValueCodecRegistry.standard()).orElseThrow();

            assertEquals("contact", query.fieldName());
            assertTrue(query.fieldTag().length() <= 30);
            assertEquals("aba", query.normalizedValue());
            assertEquals(2, query.tokenGroups().size());
            assertEquals(1, query.distinctTokenCount());
            assertEquals(1, query.remainingWhere().children().size());
            assertEquals("status", ((TermCondition) query.remainingWhere().children().getFirst()).field());
        }
    }

    @Test
    void rejectsContainsInsideOrBeforeAnySqlIsCreated() {
        ConditionGroup where = ConditionGroup.or()
                                             .add(ProtectedConditions.contains("contact", "abc"))
                                             .add(TermCondition.of("status", "eq", "ACTIVE"))
                                             .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys);

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> runtime.prepareContainsQuery(
                            protectedForm(), where, DataScope.none(), ValueCodecRegistry.standard()));

            assertEquals("protected contains search requires a top-level AND condition", error.getMessage());
            assertFalse(error.getMessage().contains("contact"));
        }
    }

    @Test
    void preservesNullChecksOnEncryptedColumnsWithoutCreatingSearchTokens() {
        ConditionGroup where = ConditionGroup.and()
                                             .whereNull("contact")
                                             .whereNotNull("status")
                                             .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys);

            ProtectedFieldRuntime.PreparedQuery query = runtime.prepareQuery(
                    protectedForm(), protectedForm(), where, DataScope.none(), ValueCodecRegistry.standard());

            TermCondition encryptedNull = (TermCondition) query.where().children().getFirst();
            assertEquals("contact", encryptedNull.field());
            assertEquals("is-null", encryptedNull.operator());
            assertNull(encryptedNull.value());
            TermCondition ordinaryNotNull = (TermCondition) query.where().children().getLast();
            assertEquals("is-not-null", ordinaryNotNull.operator());
        }
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("status", "VARCHAR"))
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
