package com.flying.orm.rdb.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 对已生成迁移计划做上线前审核的策略。
 *
 * @param onlineDdlMode 在线 DDL 要求
 * @param columnRenames 明确的旧列到新列映射，供回滚计划生成反向 rename
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationReviewPolicy(OnlineDdlMode onlineDdlMode,
                                          Map<String, String> columnRenames) {

    public SchemaMigrationReviewPolicy {
        onlineDdlMode = Objects.requireNonNull(onlineDdlMode, "online DDL mode must not be null");
        columnRenames = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(
                columnRenames, "column renames must not be null")));
    }

    public static SchemaMigrationReviewPolicy allowBlocking() {
        return new SchemaMigrationReviewPolicy(OnlineDdlMode.ALLOW_BLOCKING, Map.of());
    }

    public static SchemaMigrationReviewPolicy preferOnline() {
        return new SchemaMigrationReviewPolicy(OnlineDdlMode.PREFER_ONLINE, Map.of());
    }

    public static SchemaMigrationReviewPolicy requireOnline() {
        return new SchemaMigrationReviewPolicy(OnlineDdlMode.REQUIRE_ONLINE, Map.of());
    }

    public SchemaMigrationReviewPolicy withColumnRenames(Map<String, String> renames) {
        return new SchemaMigrationReviewPolicy(onlineDdlMode, renames);
    }
}
