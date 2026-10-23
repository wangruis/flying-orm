package com.flying.orm.core.form;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 动态字段共享唯一物理列声明，并携带插入时按需求值的应用默认值。
 *
 * @param column 完整列声明；字段编辑不丢失默认值、字符集或生成策略
 * @param primaryKey 是否主键；主键列始终不可空
 * @param unique 是否生成单列唯一索引
 * @param runtimeDefault 缺值插入时的 Java 值提供者；不参与数据库 DDL
 * @param features 字段显式扩展；表单构建时绑定本次表级继承来源
 * @author wangr
 * @version v1.0
 */
public record DynamicField(ColumnDefinition column, boolean primaryKey, boolean unique,
                           Supplier<?> runtimeDefault, FeatureRegistry features) {

    /** 从物理列声明创建无应用默认值的字段。 */
    public DynamicField(ColumnDefinition column, boolean primaryKey, boolean unique) {
        this(column, primaryKey, unique, null, FeatureRegistry.empty());
    }

    public DynamicField {
        column = Objects.requireNonNull(column, "dynamic field column must not be null");
        features = Objects.requireNonNull(features, "field features");
        if (primaryKey && column.nullable()) {
            column = column.toBuilder().nullable(false).build();
        }
    }

    public FieldIdentity identity() { return column.identity(); }
    public String name() { return column.name(); }
    public String normalizedName() { return column.identity().key(); }
    public DatabaseType databaseType() { return column.databaseType(); }
    public String dataType() { return column.databaseType().declaration(); }
    public boolean nullable() { return column.nullable(); }
    public Integer length() { return column.length(); }

    /** 时间类型返回小数秒精度，其他类型返回数值精度。 */
    public Integer precision() {
        return databaseType().logicalType().temporal() ? column.temporalPrecision() : column.precision();
    }

    public Integer scale() { return column.scale(); }
    public String comment() { return column.comment(); }
    public ValueGeneration generation() { return column.generation(); }

    public static DynamicField of(String name, String dataType) {
        return of(name, DatabaseType.of(dataType));
    }

    public static DynamicField of(String name, DatabaseType databaseType) {
        return new DynamicField(ColumnDefinition.builder(name, databaseType).build(), false, false);
    }

    public static DynamicField primaryKey(String name, String dataType) {
        return primaryKey(name, DatabaseType.of(dataType));
    }

    public static DynamicField primaryKey(String name, DatabaseType databaseType) {
        return new DynamicField(ColumnDefinition.builder(name, databaseType).nullable(false).build(), true, false);
    }

    public DynamicField withNullable(boolean nullable) {
        return withColumn(column.toBuilder().nullable(!primaryKey && nullable));
    }

    public DynamicField withUnique(boolean unique) {
        return new DynamicField(column, primaryKey, unique, runtimeDefault, features);
    }

    /** 仅在插入值缺失或为 null 时调用；返回值仍经过字段 codec，不进入冲突更新。 */
    public DynamicField withRuntimeDefault(Supplier<?> value) {
        return new DynamicField(column, primaryKey, unique, Objects.requireNonNull(value, "runtime default"), features);
    }

    /** 配置字段扩展，不改变物理列、默认值或约束。 */
    public DynamicField withFeatures(FeatureRegistry features) {
        return this.features == features ? this : new DynamicField(column, primaryKey, unique, runtimeDefault, features);
    }

    public DynamicField withLength(Integer length) {
        return withColumn(column.toBuilder().length(length));
    }

    public DynamicField withPrecision(Integer precision, Integer scale) {
        ColumnDefinition.Builder draft = column.toBuilder().scale(scale);
        if (databaseType().logicalType().temporal()) {
            draft.temporalPrecision(precision);
        } else {
            draft.precision(precision);
        }
        return withColumn(draft);
    }

    public DynamicField withComment(String comment) {
        return withColumn(column.toBuilder().comment(optionalComment(comment)));
    }

    public DynamicField generatedByIdentity() {
        return withGeneration(ValueGeneration.identity());
    }

    public DynamicField generatedByIdentity(long startWith, long incrementBy, int cacheSize) {
        return withGeneration(ValueGeneration.identity(startWith, incrementBy, cacheSize));
    }

    public DynamicField generatedBySequence(String sequenceName) {
        return withGeneration(ValueGeneration.sequence(sequenceName));
    }

    public DynamicField generatedBySequence(String sequenceName, long startWith, long incrementBy, int cacheSize) {
        return withGeneration(ValueGeneration.sequence(sequenceName, startWith, incrementBy, cacheSize));
    }

    public DynamicField withGeneration(ValueGeneration generation) {
        return withColumn(column.toBuilder().generation(generation));
    }

    private DynamicField withColumn(ColumnDefinition.Builder draft) {
        return new DynamicField(draft.build(), primaryKey, unique, runtimeDefault, features);
    }

    private static String optionalComment(String comment) {
        return comment == null || comment.isBlank() ? null : comment.trim();
    }
}
