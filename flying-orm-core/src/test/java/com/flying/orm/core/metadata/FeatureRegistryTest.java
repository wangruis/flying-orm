package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 feature 注册表的确定性查找行为，避免运行期扩展点出现重复或不可预期覆盖。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class FeatureRegistryTest {

    /**
     * 验证 feature 可按规范化 id 和具体类型查找，并且注册表发布后不可变。
     */
    @Test
    void findsFeatureByIdAndTypeAndRejectsDuplicateRegistration() {
        QueryFeature queryFeature = new QueryFeature("query");
        FeatureRegistry registry = FeatureRegistry.builder()
                                                  .add(queryFeature)
                                                  .build();

        assertTrue(registry.find(" QUERY ").isPresent());
        assertSame(queryFeature, registry.feature("query"));
        assertSame(queryFeature, registry.feature(QueryFeature.class));
        assertThrows(UnsupportedOperationException.class, () -> registry.features().add(queryFeature));
        assertThrows(IllegalArgumentException.class,
                     () -> FeatureRegistry.builder()
                                          .add(queryFeature)
                                          .add(new QueryFeature("other-query"))
                                          .build());
    }

    @Test
    void missingFeatureFailureDoesNotExposeLookupId() {
        String secret = "tenant-secret-feature";

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> FeatureRegistry.empty().feature(secret));

        assertFalse(error.getMessage().contains(secret));
    }

    /** 重复 feature 的公开配置不能把任意长 id 写回异常消息。 */
    @Test
    void duplicateFeatureFailureDoesNotExposeConfiguredId() {
        String secret = "tenant-secret-" + "x".repeat(5_000);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> FeatureRegistry.builder()
                                     .add(new QueryFeature(secret))
                                     .add(new WriteFeature(secret))
                                     .build());

        assertEquals("duplicate feature id", error.getMessage());
    }

    private record QueryFeature(String id) implements Feature {
    }

    private record WriteFeature(String id) implements Feature {
    }
}
