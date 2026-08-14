package com.flying.orm.core.sql.render;

import com.flying.orm.core.condition.ConditionGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证命名 SQL term 包可以批量注册业务条件算子，减少应用侧重复配置。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class SqlTermPackageTest {

    /**
     * 验证用户机构关系条件包可以一次注册 in 和 not-in 两个业务算子。
     */
    @Test
    void registersUserOrganizationRelationTermPackage() {
        SqlTermPackage termPackage = userOrganizationTerms();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(termPackage)
                                          .build();

        SqlFragment inOrg = renderer.renderWhere(ConditionGroup.and(termPackage.terms())
                                                               .where("userId",
                                                                      "user-in-org",
                                                                      List.of("org-1", "org-2"))
                                                               .build());
        SqlFragment notInOrg = renderer.renderWhere(ConditionGroup.and(termPackage.terms())
                                                                  .where("userId",
                                                                         "user-not-in-org",
                                                                         "org-3")
                                                                  .build());

        assertEquals("exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?))",
                     inOrg.sql());
        assertEquals(List.of("org-1", "org-2"), inOrg.parameters());
        assertEquals("not exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     notInOrg.sql());
        assertEquals(List.of("org-3"), notInOrg.parameters());
    }

    /**
     * 验证命名条件包的 handler 列表不可变，避免运行期并发注册产生漂移。
     */
    @Test
    void exposesImmutableHandlersFromTermPackage() {
        SqlTermPackage termPackage = userOrganizationTerms();

        assertEquals("user-organization", termPackage.name());
        assertEquals(2, termPackage.handlers().size());
        assertThrows(UnsupportedOperationException.class,
                     () -> termPackage.handlers().add(SqlTermHandler.equalsTo()));
    }

    @Test
    void missingSqlTermFailureDoesNotExposeLookupId() {
        String secret = "tenant-secret-sql-term";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTermRegistry.builder().build().handler(secret));

        assertFalse(error.getMessage().contains(secret));
    }

    /** 重复 SQL term 的公开配置不能把任意长 id 写回异常消息。 */
    @Test
    void duplicateSqlTermFailureDoesNotExposeConfiguredId() {
        String secret = "tenant-secret-" + "x".repeat(5_000);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> SqlTermRegistry.builder()
                                     .add(SqlTermHandler.of(secret, (term, context) -> SqlFragment.of("")))
                                     .add(SqlTermHandler.of(secret, (term, context) -> SqlFragment.of("")))
                                     .build());

        assertEquals("duplicate sql term id", error.getMessage());
    }

    /** 关系表允许 schema 限定，但自行拼接的别名和列名必须是单段标识符。 */
    @Test
    void rejectsQualifiedRelationAliasesAndColumnsAtRegistration() {
        assertThrows(IllegalArgumentException.class,
                     () -> RelationTermPackage.of("relations", "schema.org_user", "ou.extra",
                                                   "user_id", "org_id", "in-org", "not-in-org"));
        assertThrows(IllegalArgumentException.class,
                     () -> RelationTermPackage.of("relations", "schema.org_user", "ou",
                                                   "tenant.user_id", "org_id", "in-org", "not-in-org"));

        RelationTermPackage.of("relations", "schema.org_user", "ou",
                               "user_id", "org_id", "in-org", "not-in-org");
    }

    private static SqlTermPackage userOrganizationTerms() {
        return RelationTermPackage.of("user-organization",
                                      "org_user",
                                      "ou",
                                      "user_id",
                                      "org_id",
                                      "user-in-org",
                                      "user-not-in-org");
    }
}
