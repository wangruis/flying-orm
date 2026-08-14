package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.sql.render.RelationTermPackage;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证参数条件命名包可以把请求参数映射为业务 term，并与 SQL term 包组合工作。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
class ParameterConditionPackageTest {

    /**
     * 验证用户机构参数条件包可以把 orgIds 和 excludeOrgIds 编译为用户机构业务条件。
     */
    @Test
    void compilesUserOrganizationParametersAndRendersWithRelationTermPackage() {
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .addPackage(userOrganizationParameters())
                                                                        .build();
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(userOrganizationTerms())
                                          .build();

        ConditionGroup condition = compiler.compile(Map.of("orgIds",
                                                           List.of("org-1", "org-2"),
                                                           "excludeOrgIds",
                                                           "org-3"));
        SqlFragment where = renderer.renderWhere(condition);

        assertEquals("exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id in (?, ?)) "
                             + "and not exists (select 1 from org_user ou where ou.user_id = userId and ou.org_id = ?)",
                     where.sql());
        assertEquals(List.of("org-1", "org-2", "org-3"), where.parameters());
    }

    /**
     * 验证参数条件包发布后不可变，避免并发运行期改写参数映射规则。
     */
    @Test
    void exposesImmutableSpecsFromParameterConditionPackage() {
        ParameterConditionPackage conditionPackage = userOrganizationParameters();

        assertEquals("user-organization", conditionPackage.name());
        assertEquals(2, conditionPackage.specs().size());
        assertEquals(ConditionValueShape.SCALAR_OR_COLLECTION,
                     conditionPackage.terms().handler("user-in-org").shape());
        assertThrows(UnsupportedOperationException.class,
                     () -> conditionPackage.specs().add(ParameterConditionSpec.of("status", "status", "=")));
    }

    private static ParameterConditionPackage userOrganizationParameters() {
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.simple("user-in-org",
                                                                 ConditionValueShape.SCALAR_OR_COLLECTION))
                                         .add(TermHandler.simple("user-not-in-org",
                                                                 ConditionValueShape.SCALAR_OR_COLLECTION))
                                         .build();
        return ParameterConditionPackage.of(
                "user-organization",
                terms,
                ParameterConditionSpec.of("orgIds", "userId", "user-in-org"),
                ParameterConditionSpec.of("excludeOrgIds", "userId", "user-not-in-org"));
    }

    private static com.flying.orm.core.sql.render.SqlTermPackage userOrganizationTerms() {
        return RelationTermPackage.of("user-organization",
                                      "org_user",
                                      "ou",
                                      "user_id",
                                      "org_id",
                                      "user-in-org",
                                      "user-not-in-org");
    }
}
