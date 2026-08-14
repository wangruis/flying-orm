package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证四种正式数据库的 CONTAINS 候选 SQL 保持版本分组、参数绑定和候选上限。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedContainsSqlPlannerTest {

    @Test
    void rendersOneBoundedCandidateRequestPerReadableKeyVersion() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                                                               .current("v2", key(2))
                                                               .readable("v1", key(1))
                                                               .build()) {
            for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                                              RdbDialect.oracle(), RdbDialect.sqlServer())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect)
                                                               .withProtectedFields(
                                                                       ProtectedFieldRuntime.create(keys));
                ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                        .prepareContainsQuery(
                                protectedForm(),
                                ConditionGroup.and().add(
                                        ProtectedConditions.contains("contact", "ABCDE")).build(),
                                DataScope.none())
                        .orElseThrow();

                List<SqlRequest> requests = renderer.protection().containsCandidates(query, 1000);

                assertEquals(2, requests.size());
                requests.forEach(request -> {
                    assertTrue(request.sql().contains("count(distinct"));
                    assertTrue(request.sql().contains("order by"));
                    assertEquals(query.fieldTag(), request.parameters().getFirst());
                    assertEquals(query.distinctTokenCount(), request.parameters().get(4));
                    assertEquals(1001, request.parameters().getLast());
                });
            }
        }
    }

    /**
     * 多个可读密钥版本必须先各自完成令牌交集，再用 UNION 去重并一次性读取有界业务行。
     */
    @Test
    void rendersOneBoundedVerificationQueryAcrossReadableKeyVersions() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                                                               .current("v2", key(2))
                                                               .readable("v1", key(1))
                                                               .build()) {
            for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                                              RdbDialect.oracle(), RdbDialect.sqlServer())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect)
                                                               .withProtectedFields(
                                                                       ProtectedFieldRuntime.create(keys));
                ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                        .prepareContainsQuery(
                                protectedForm(),
                                ConditionGroup.and().add(
                                        ProtectedConditions.contains("contact", "ABCDE")).build(),
                                DataScope.none())
                        .orElseThrow();

                SqlRequest request = renderer.protection().containsRows(
                        query, List.of(PageSort.desc("id")), 1000);

                assertTrue(request.sql().contains(" union "));
                assertTrue(request.sql().contains(" join ("));
                assertTrue(request.sql().contains("count(distinct"));
                assertTrue(request.sql().contains("order by"));
                assertEquals(1001, request.parameters().getLast());
                assertFalse(request.parameters().contains("ABCDE"));
            }
        }
    }

    /**
     * 业务字段允许与候选子查询的内部列名相同；剩余条件必须限定到业务表别名，避免四库都产生歧义列。
     */
    @Test
    void qualifiesBusinessConditionsWhenAFieldMatchesAnInternalCandidateColumn() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                                              RdbDialect.oracle(), RdbDialect.sqlServer())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect)
                        .withProtectedFields(ProtectedFieldRuntime.create(keys));
                DynamicForm form = protectedFormWithCandidateNameCollision();
                ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                        .prepareContainsQuery(
                                form,
                                ConditionGroup.and()
                                              .add(ProtectedConditions.contains("contact", "ABCDE"))
                                              .where("__fop_c0", "=", "active")
                                              .build(),
                                DataScope.none())
                        .orElseThrow();

                SqlRequest request = renderer.protection().containsRows(
                        query, List.of(PageSort.asc("id")), 1000);

                assertTrue(request.sql().contains("where fop_business."));
                assertEquals("active", request.parameters().get(request.parameters().size() - 2));
            }
        }
    }

    /** 物理表单会移除保护声明，排序校验必须保留逻辑表单的全部加密字段集合。 */
    @Test
    void rejectsOrderingContainsCandidatesByAnotherEncryptedField() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                    .prepareContainsQuery(
                            protectedForm(),
                            ConditionGroup.and().add(
                                    ProtectedConditions.contains("contact", "ABCDE")).build(),
                            DataScope.none())
                    .orElseThrow();

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> renderer.protection().containsRows(
                            query, List.of(PageSort.asc("secret_rank")), 100));

            assertEquals("encrypted field cannot be used for protected contains ordering", error.getMessage());
        }
    }

    /** 加密字段排序限制必须遵循表单的大小写无关字段查找，不能被物理字段原始大小写绕过。 */
    @Test
    void rejectsOrderingByMixedCaseEncryptedField() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            DynamicForm form = protectedFormWithMixedCaseEncryptedField();
            ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                    .prepareContainsQuery(
                            form,
                            ConditionGroup.and().add(
                                    ProtectedConditions.contains("Contact", "ABCDE")).build(),
                            DataScope.none())
                    .orElseThrow();

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> renderer.protection().containsRows(
                            query, List.of(PageSort.asc("SecretRank")), 100));

            assertEquals("encrypted field cannot be used for protected contains ordering", error.getMessage());
        }
    }

    @Test
    void rejectsContainsTokenGroupsThatExceedTheOracleInListLimit() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.oracle())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            ProtectedFieldRuntime.PreparedContainsQuery query = preparedQuery(
                    List.of(new ProtectedFieldRuntime.ContainsTokenGroup("v1", tokens(1001))), 1001);

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> renderer.protection().containsRows(query, List.of(PageSort.asc("id")), 1000));

            assertEquals("protected contains query exceeds the portable SQL parameter limit", error.getMessage());
        }
    }

    @Test
    void rejectsContainsUnionThatExceedsTheSqlServerParameterLimit() {
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.sqlServer())
                    .withProtectedFields(ProtectedFieldRuntime.create(keys));
            List<ProtectedFieldRuntime.ContainsTokenGroup> groups = List.of(
                    new ProtectedFieldRuntime.ContainsTokenGroup("v1", tokens(700)),
                    new ProtectedFieldRuntime.ContainsTokenGroup("v2", tokens(700)),
                    new ProtectedFieldRuntime.ContainsTokenGroup("v3", tokens(700)));
            ProtectedFieldRuntime.PreparedContainsQuery query = preparedQuery(groups, 700);

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> renderer.protection().containsRows(query, List.of(PageSort.asc("id")), 1000));

            assertEquals("protected contains query exceeds the portable SQL parameter limit", error.getMessage());
        }
    }

    private static ProtectedFieldRuntime.PreparedContainsQuery preparedQuery(
            List<ProtectedFieldRuntime.ContainsTokenGroup> groups,
            int distinctTokenCount) {
        return new ProtectedFieldRuntime.PreparedContainsQuery(
                protectedForm(),
                ConditionGroup.and().build(),
                List.of("id"),
                Set.of("contact", "secret_rank"),
                "contact",
                "field-tag",
                "normalized",
                groups,
                distinctTokenCount,
                List.of("id"),
                "__fop_c_test");
    }

    private static List<byte[]> tokens(int count) {
        List<byte[]> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            byte[] value = new byte[32];
            value[0] = (byte) (index >>> 24);
            value[1] = (byte) (index >>> 16);
            value[2] = (byte) (index >>> 8);
            value[3] = (byte) index;
            values.add(value);
        }
        return values;
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("secret_rank", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                         EncryptedSearchMode.CONTAINS)
                                                                         .build())
                          .encrypted("secret_rank", EncryptedFieldDefinition.builder().build())
                          .build();
    }

    private static DynamicForm protectedFormWithCandidateNameCollision() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("contact", "VARCHAR"))
                          .addField(DynamicField.of("__fop_c0", "VARCHAR"))
                          .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                         .searchModes(
                                                                         EncryptedSearchMode.CONTAINS)
                                                                         .build())
                          .build();
    }

    private static DynamicForm protectedFormWithMixedCaseEncryptedField() {
        return DynamicForm.builder("customer", "customer")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("Contact", "VARCHAR"))
                          .addField(DynamicField.of("SecretRank", "VARCHAR"))
                          .encrypted("Contact", EncryptedFieldDefinition.builder()
                                                                          .searchModes(
                                                                                  EncryptedSearchMode.CONTAINS)
                                                                          .build())
                          .encrypted("SecretRank", EncryptedFieldDefinition.builder().build())
                          .build();
    }

    private static byte[] key(int seed) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) seed);
        return key;
    }
}
