package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 审核计划执行前必须仍然成立的一项指纹条件。
 *
 * <p>这里只保存稳定指纹，不保存连接、reader 或数据库对象。执行器用同一元数据读取边界重新取值后
 * 做相等比较；不相等就以零 SQL 结束。</p>
 *
 * @author wangr
 * @version v3.2
 */
public record SchemaPlanPrecondition(Kind kind, String expectedFingerprint) {

    public enum Kind {
        DATABASE_DESCRIPTOR,
        CAPABILITIES,
        ACTUAL_SCHEMA,
        SNAPSHOT_COVERAGE
    }

    public SchemaPlanPrecondition {
        kind = Objects.requireNonNull(kind, "schema plan precondition kind must not be null");
        expectedFingerprint = requireText(
                expectedFingerprint, "schema plan expected fingerprint");
    }

    public static SchemaPlanPrecondition database(String fingerprint) {
        return new SchemaPlanPrecondition(Kind.DATABASE_DESCRIPTOR, fingerprint);
    }

    public static SchemaPlanPrecondition capabilities(String fingerprint) {
        return new SchemaPlanPrecondition(Kind.CAPABILITIES, fingerprint);
    }

    public static SchemaPlanPrecondition actualSnapshot(String fingerprint) {
        return new SchemaPlanPrecondition(Kind.ACTUAL_SCHEMA, fingerprint);
    }

    public static SchemaPlanPrecondition snapshotCoverage(String fingerprint) {
        return new SchemaPlanPrecondition(Kind.SNAPSHOT_COVERAGE, fingerprint);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
