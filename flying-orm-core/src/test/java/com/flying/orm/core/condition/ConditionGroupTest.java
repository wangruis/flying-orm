package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证参数驱动动态条件会保留 Java 侧结构化语义，不提前泄漏为 SQL 字符串。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class ConditionGroupTest {

    /**
     * 验证业务自定义 term 可以作为普通条件进入条件树，例如 user-in-org 机构范围条件。
     */
    @Test
    void keepsCustomBusinessTermStructuredInConditionTree() {
        ConditionGroup group = ConditionGroup.and()
                                             .where("status", "=", "enabled")
                                             .where("userId", "user-in-org", "org-1")
                                             .or(or -> or.where("ownerId", "=", "u-1")
                                                        .where("creatorId", "=", "u-1"))
                                             .build();

        assertEquals(LogicalOperator.AND, group.operator());
        assertEquals(3, group.children().size());

        TermCondition customTerm = assertInstanceOf(TermCondition.class, group.children().get(1));
        assertEquals("userId", customTerm.field());
        assertEquals("user-in-org", customTerm.operator());
        assertEquals("org-1", customTerm.value());

        ConditionGroup nestedOr = assertInstanceOf(ConditionGroup.class, group.children().get(2));
        assertEquals(LogicalOperator.OR, nestedOr.operator());
        assertEquals(List.of("ownerId", "creatorId"),
                     nestedOr.children()
                             .stream()
                             .map(TermCondition.class::cast)
                             .map(TermCondition::field)
                             .toList());
        assertThrows(UnsupportedOperationException.class,
                     () -> group.children().add(TermCondition.of("deleted", "=", false)));
    }

    @Test
    void normalizesStrictOptionalNullAndExplicitMixedShapeTerms() {
        TermRegistry terms = TermRegistry.builder()
                                         .add(TermHandler.simple("user-in-org",
                                                                 ConditionValueShape.SCALAR_OR_COLLECTION))
                                         .build();
        ConditionGroup group = ConditionGroup.and(terms)
                                             .where("name", "like", "  张三  ")
                                             .whereIfPresent("status", "=", "   ")
                                             .whereNull("deleted_at")
                                             .where("user_id",
                                                    "user-in-org",
                                                    List.of(" org-1 ", "org-2"))
                                             .build();

        assertEquals(3, group.children().size());
        assertEquals("张三", ((TermCondition) group.children().get(0)).value());
        assertNull(((TermCondition) group.children().get(1)).value());
        assertEquals(List.of("org-1", "org-2"), ((TermCondition) group.children().get(2)).value());
        assertThrows(ConditionValueException.class,
                     () -> ConditionGroup.and().where("name", "=", " "));
        assertThrows(ConditionValueException.class,
                     () -> ConditionGroup.and().where("name", "=", null));
        assertThrows(ConditionValueException.class,
                     () -> ConditionGroup.and()
                                         .where("user_id", "user-in-org", List.of("org-1", "org-2")));

        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                       .add(TermHandler.simple("=",
                                                               ConditionValueShape.SCALAR_OR_COLLECTION))
                                       .build());
    }

    /** 直接 AST 同样必须有硬深度边界，不能把递归风险推迟到 SQL 渲染或计划缓存。 */
    @Test
    void rejectsDirectAstBeyondTheHardDepthLimit() {
        ConditionGroup nested = ConditionGroup.and().where("id", "=", "u-1").build();
        for (int depth = 0; depth < 62; depth++) {
            nested = ConditionGroup.and().add(nested).build();
        }
        ConditionGroup maximumDepth = nested;

        assertThrows(IllegalArgumentException.class,
                     () -> ConditionGroup.and().add(maximumDepth).build());
    }

    /** 直接构造 term 也必须限制集合大小，防止防御性快照消费无界输入。 */
    @Test
    void rejectsOversizedDirectTermCollection() {
        List<Integer> oversized = java.util.stream.IntStream.rangeClosed(0, 1_000).boxed().toList();
        Object[] oversizedArray = new Object[1_001];

        ConditionValueException error = assertThrows(ConditionValueException.class,
                () -> TermCondition.of("id", "in", oversized));
        ConditionValueException arrayError = assertThrows(ConditionValueException.class,
                () -> TermCondition.of("id", "in", oversizedArray));

        assertEquals(ConditionValueException.Error.COLLECTION_TOO_LARGE, error.error());
        assertEquals(ConditionValueException.Error.COLLECTION_TOO_LARGE, arrayError.error());
    }
}
