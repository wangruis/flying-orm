package com.flying.orm.rdb.dialect;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 方言能力的稳定字符串身份。
 *
 * <p>它不是封闭枚举：flying-orm 可以继续增加能力，受控扩展也可以声明自己的能力 ID，
 * 而已保存的指纹仍然只依赖规范化后的文本。ID 统一使用小写字母、数字以及点、横线、下划线，
 * 避免同一个能力因为大小写或首尾空白产生两份身份。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class DialectCapabilityId implements Comparable<DialectCapabilityId> {

    private static final Pattern FORMAT = Pattern.compile("[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*");

    public static final DialectCapabilityId OFFSET_FETCH_PAGINATION = of("offset-fetch-pagination");
    public static final DialectCapabilityId MERGE_UPSERT = of("merge-upsert");
    public static final DialectCapabilityId IDENTITY_COLUMNS = of("identity-columns");
    public static final DialectCapabilityId SEQUENCES = of("sequences");
    public static final DialectCapabilityId JSON_FUNCTIONS = of("json-functions");
    public static final DialectCapabilityId NATIVE_JSON = of("native-json");
    public static final DialectCapabilityId NATIVE_BOOLEAN = of("native-boolean");
    public static final DialectCapabilityId LARGE_OBJECTS = of("large-objects");
    public static final DialectCapabilityId MYSQL_RELATIONAL_METADATA = of("mysql-relational-metadata");
    public static final DialectCapabilityId POSTGRESQL_VECTOR = of("postgresql-vector");

    private final String value;

    private DialectCapabilityId(String value) {
        this.value = value;
    }

    /**
     * 创建可持久化、可参与指纹的能力 ID。
     *
     * @param value 稳定文本；会去掉首尾空白并规范成小写
     * @return 规范化后的 ID
     */
    public static DialectCapabilityId of(String value) {
        String normalized = Objects.requireNonNull(value, "dialect capability id must not be null")
                                   .trim()
                                   .toLowerCase(Locale.ROOT);
        if (!FORMAT.matcher(normalized).matches()) {
            throw new IllegalArgumentException("invalid dialect capability id: " + normalized);
        }
        return new DialectCapabilityId(normalized);
    }

    static DialectCapabilityId from(DialectFeature feature) {
        return switch (Objects.requireNonNull(feature, "dialect feature must not be null")) {
            case OFFSET_FETCH_PAGINATION -> OFFSET_FETCH_PAGINATION;
            case MERGE_UPSERT -> MERGE_UPSERT;
            case IDENTITY_COLUMNS -> IDENTITY_COLUMNS;
            case SEQUENCES -> SEQUENCES;
            case JSON_FUNCTIONS -> JSON_FUNCTIONS;
            case NATIVE_JSON -> NATIVE_JSON;
            case NATIVE_BOOLEAN -> NATIVE_BOOLEAN;
            case LARGE_OBJECTS -> LARGE_OBJECTS;
            case MYSQL_RELATIONAL_METADATA -> MYSQL_RELATIONAL_METADATA;
            case POSTGRESQL_VECTOR -> POSTGRESQL_VECTOR;
        };
    }

    /** @return 规范化后的稳定字符串 */
    public String value() {
        return value;
    }

    @Override
    public int compareTo(DialectCapabilityId other) {
        return value.compareTo(Objects.requireNonNull(other, "other capability id must not be null").value);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof DialectCapabilityId that && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
