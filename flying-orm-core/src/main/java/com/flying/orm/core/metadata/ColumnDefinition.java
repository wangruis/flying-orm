package com.flying.orm.core.metadata;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;

import java.util.Objects;

/**
 * 完整、不可变的关系列定义。
 *
 * <p>字段的物理声明只在这里保存。构建时完成规范化与组合校验，发布后的访问器不再解析类型或复制数据。
 * 值相等比较完整声明；数据库回读与目标的方言匹配由 Schema 规划器负责。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class ColumnDefinition {

    public static final String STANDARD_CODEC = "standard";

    private final FieldIdentity identity;
    private final DatabaseType databaseType;
    private final String codecId;
    private final boolean nullable;
    private final Integer length;
    private final Integer precision;
    private final Integer scale;
    private final Integer temporalPrecision;
    private final ColumnDefault defaultValue;
    private final String defaultConstraintName;
    private final String comment;
    private final ValueGeneration generation;
    private final String charset;
    private final String collation;

    private ColumnDefinition(Builder builder) {
        identity = builder.identity;
        databaseType = builder.databaseType;
        codecId = builder.codecId;
        nullable = builder.nullable;
        length = positive(builder.length, "column length");
        precision = positive(builder.precision, "column precision");
        scale = nonNegative(builder.scale, "column scale");
        temporalPrecision = nonNegative(builder.temporalPrecision, "column temporal precision");
        defaultValue = builder.defaultValue;
        defaultConstraintName = optionalDefaultConstraintName(builder.defaultConstraintName);
        comment = optionalText(builder.comment, "column comment");
        generation = builder.generation;
        charset = optionalText(builder.charset, "column charset");
        collation = optionalText(builder.collation, "column collation");

        if (scale != null && precision == null) {
            throw new IllegalArgumentException("column precision must be set when scale is set");
        }
        if (scale != null && scale > precision) {
            throw new IllegalArgumentException("column scale must not be greater than precision");
        }
        if (temporalPrecision != null && !databaseType.logicalType().temporal()) {
            throw new IllegalArgumentException("temporal precision requires a temporal database type");
        }
        if ((precision != null || scale != null) && databaseType.logicalType().temporal()) {
            throw new IllegalArgumentException("temporal database type must use temporal precision");
        }
    }

    public static Builder builder(String name, String databaseType) {
        return new Builder(FieldIdentity.of(name), DatabaseType.of(databaseType));
    }

    public static Builder builder(String name, DatabaseType databaseType) {
        return new Builder(FieldIdentity.of(name), databaseType);
    }

    public static Builder builder(FieldIdentity identity, DatabaseType databaseType) {
        return new Builder(identity, databaseType);
    }

    public FieldIdentity identity() {
        return identity;
    }

    public String name() {
        return identity.name();
    }

    public DatabaseType databaseType() {
        return databaseType;
    }

    public String codecId() {
        return codecId;
    }

    public boolean nullable() {
        return nullable;
    }

    public Integer length() {
        return length;
    }

    public Integer precision() {
        return precision;
    }

    public Integer scale() {
        return scale;
    }

    public Integer temporalPrecision() {
        return temporalPrecision;
    }

    public ColumnDefault defaultValue() {
        return defaultValue;
    }

    /** 数据库回读或调用方显式指定的默认约束名；未指定时为空，不要求实体猜测数据库生成的名称。 */
    public String defaultConstraintName() {
        return defaultConstraintName;
    }

    public String comment() {
        return comment;
    }

    public ValueGeneration generation() {
        return generation;
    }

    public String charset() {
        return charset;
    }

    public String collation() {
        return collation;
    }

    /** 创建独立的编辑草稿，保留包括数据库回读信息在内的完整列声明。 */
    public Builder toBuilder() {
        return toBuilder(identity);
    }

    /** 改名时仍复用完整列属性，不由各调用者重新拼装。 */
    public Builder toBuilder(FieldIdentity identity) {
        return new Builder(identity, databaseType).codecId(codecId).nullable(nullable)
                .length(length).precision(precision).scale(scale).temporalPrecision(temporalPrecision)
                .defaultValue(defaultValue).defaultConstraintName(defaultConstraintName).comment(comment)
                .generation(generation).charset(charset).collation(collation);
    }

    @Override
    public boolean equals(Object candidate) {
        return this == candidate || candidate instanceof ColumnDefinition other
                && nullable == other.nullable
                && identity.equals(other.identity)
                && databaseType.equals(other.databaseType)
                && codecId.equals(other.codecId)
                && Objects.equals(length, other.length)
                && Objects.equals(precision, other.precision)
                && Objects.equals(scale, other.scale)
                && Objects.equals(temporalPrecision, other.temporalPrecision)
                && defaultValue.equals(other.defaultValue)
                && Objects.equals(defaultConstraintName, other.defaultConstraintName)
                && Objects.equals(comment, other.comment)
                && generation.equals(other.generation)
                && Objects.equals(charset, other.charset)
                && Objects.equals(collation, other.collation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identity, databaseType, codecId, nullable, length, precision, scale,
                temporalPrecision, defaultValue, defaultConstraintName, comment, generation, charset, collation);
    }

    /** 构建器只在配置线程内使用。 */
    public static final class Builder {

        private final FieldIdentity identity;
        private final DatabaseType databaseType;
        private String codecId = STANDARD_CODEC;
        private boolean nullable = true;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private Integer temporalPrecision;
        private ColumnDefault defaultValue = ColumnDefault.none();
        private String defaultConstraintName;
        private String comment;
        private ValueGeneration generation = ValueGeneration.none();
        private String charset;
        private String collation;

        private Builder(FieldIdentity identity, DatabaseType databaseType) {
            this.identity = Objects.requireNonNull(identity, "column identity must not be null");
            this.databaseType = Objects.requireNonNull(databaseType, "column database type must not be null");
        }

        public Builder codecId(String codecId) {
            this.codecId = Names.requireText(codecId, "column codec id");
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder length(Integer length) {
            this.length = length;
            return this;
        }

        public Builder precision(Integer precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(Integer scale) {
            this.scale = scale;
            return this;
        }

        public Builder temporalPrecision(Integer temporalPrecision) {
            this.temporalPrecision = temporalPrecision;
            return this;
        }

        public Builder defaultValue(ColumnDefault defaultValue) {
            this.defaultValue = Objects.requireNonNull(defaultValue, "column default must not be null");
            return this;
        }

        public Builder defaultConstraintName(String defaultConstraintName) {
            this.defaultConstraintName = defaultConstraintName;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder generation(ValueGeneration generation) {
            this.generation = Objects.requireNonNull(generation, "column generation must not be null");
            return this;
        }

        public Builder charset(String charset) {
            this.charset = charset;
            return this;
        }

        public Builder collation(String collation) {
            this.collation = collation;
            return this;
        }

        public ColumnDefinition build() {
            return new ColumnDefinition(this);
        }
    }

    private static Integer positive(Integer value, String name) {
        if (value != null && value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static Integer nonNegative(Integer value, String name) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static String optionalText(String value, String name) {
        return value == null ? null : Names.requireText(value, name);
    }

    private static String optionalDefaultConstraintName(String value) {
        if (value == null) {
            return null;
        }
        String name = SqlIdentifiers.requireIdentifier(value);
        if (name.indexOf('.') >= 0) {
            throw new IllegalArgumentException("column default constraint name must not be qualified");
        }
        return name;
    }
}
