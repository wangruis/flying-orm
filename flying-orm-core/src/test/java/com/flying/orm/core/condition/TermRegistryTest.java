package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证通用条件 term 注册表的扩展能力，确保业务 term 和内置 term 使用同一种查找路径。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class TermRegistryTest {

    /**
     * 验证自定义 term id 可规范化查找，重复注册会失败。
     */
    @Test
    void registersCustomTermIdsAndRejectsDuplicates() {
        TermHandler equals = TermHandler.simple("=");
        TermHandler userInOrg = TermHandler.simple("user-in-org");

        TermRegistry registry = TermRegistry.builder()
                                            .add(equals)
                                            .add(userInOrg)
                                            .build();

        assertTrue(registry.find(" USER-IN-ORG ").isPresent());
        assertSame(userInOrg, registry.handler("user-in-org"));
        assertSame(equals, registry.handler("="));
        assertThrows(UnsupportedOperationException.class, () -> registry.handlers().add(userInOrg));
        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                      .add(userInOrg)
                                      .add(TermHandler.simple(" USER-IN-ORG "))
                                      .build());
    }

    @Test
    void standardAndCustomTermsExposeTheirValueShapes() {
        assertEquals(ConditionValueShape.COLLECTION, TermRegistry.standard().handler("in").shape());
        assertEquals(ConditionValueShape.RANGE, TermRegistry.standard().handler("between").shape());
        assertEquals(ConditionValueShape.NONE, TermRegistry.standard().handler("is-null").shape());
        assertEquals(ConditionValueShape.SCALAR, TermHandler.simple("user-in-org").shape());
        assertEquals(ConditionValueShape.COLLECTION,
                     TermHandler.simple("user-in-orgs", ConditionValueShape.COLLECTION).shape());
    }

    @Test
    void rejectsCustomShapeThatConflictsWithStandardTerm() {
        assertThrows(IllegalArgumentException.class,
                     () -> TermRegistry.builder()
                                       .add(TermHandler.simple("in", ConditionValueShape.SCALAR))
                                       .build());
    }

    /** 标准 term 的值形状冲突不能回显自定义处理器保留的原始 id。 */
    @Test
    void standardShapeConflictDoesNotExposeCustomHandlerId() {
        String rawId = " ".repeat(5_000) + "in" + " ".repeat(5_000);
        TermHandler handler = new TermHandler() {
            @Override
            public String id() {
                return rawId;
            }

            @Override
            public ConditionValueShape shape() {
                return ConditionValueShape.SCALAR;
            }
        };

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TermRegistry.builder().add(handler).build());

        assertEquals("standard term must use its declared value shape", error.getMessage());
        assertFalse(error.getMessage().contains(rawId));
    }

    @Test
    void missingTermFailureDoesNotExposeLookupId() {
        String secret = "tenant-secret-term";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TermRegistry.empty().handler(secret));

        assertFalse(error.getMessage().contains(secret));
    }

    /** 重复自定义 term 的公开配置不能把任意长 id 写回异常消息。 */
    @Test
    void duplicateTermFailureDoesNotExposeConfiguredId() {
        String secret = "tenant-secret-" + "x".repeat(5_000);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> TermRegistry.builder()
                                  .add(TermHandler.simple(secret))
                                  .add(TermHandler.simple(secret))
                                  .build());

        assertEquals("duplicate term id", error.getMessage());
    }
}
