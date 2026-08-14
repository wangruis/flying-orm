package com.flying.orm.core.form;

import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ValueGeneration;

import java.util.Objects;

/**
 * DynamicField 描述动态表单中的单个字段，并能转换为表列元数据。
 *
 * @param name       字段名
 * @param dataType   逻辑数据类型
 * @param primaryKey 是否主键字段
 * @param nullable   数据库列是否允许空值；主键始终为 false
 * @param unique     是否生成确定性单列唯一索引
 * @param length     字符或二进制字段长度；未指定时为空
 * @param precision  数值字段总有效位数；未指定时为空
 * @param scale      数值字段小数位数；未指定时为空且不能脱离 precision
 * @param comment    可选字段注释；空白会规范化为空
 * @param generation 数据库值生成策略；不会为空
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record DynamicField(String name,
                           String dataType,
                           boolean primaryKey,
                           boolean nullable,
                           boolean unique,
                           Integer length,
                           Integer precision,
                           Integer scale,
                           String comment,
                           ValueGeneration generation) {

    /**
     * 创建动态字段并完成基础校验。
     *
     * @param name       字段名
     * @param dataType   逻辑数据类型
     * @param primaryKey 是否主键字段
     * @param nullable   是否允许数据库空值
     * @param unique     是否声明单列唯一约束
     * @param length     可选长度
     * @param precision  可选精度
     * @param scale      可选小数位
     * @param comment    可选字段注释
     * @param generation 数据库值生成策略
     */
    public DynamicField {
        name = FormNames.requireText(name, "dynamic field name");
        dataType = FormNames.requireText(dataType, "dynamic field data type");
        nullable = !primaryKey && nullable;
        length = requirePositive(length, "dynamic field length");
        precision = requirePositive(precision, "dynamic field precision");
        scale = requireNonNegative(scale, "dynamic field scale");
        if (scale != null && precision == null) {
            throw new IllegalArgumentException("dynamic field precision must be set when scale is set");
        }
        if (precision != null && scale != null && scale > precision) {
            throw new IllegalArgumentException("dynamic field scale must not be greater than precision");
        }
        comment = normalizeOptionalText(comment);
        generation = Objects.requireNonNull(generation, "dynamic field value generation must not be null");
    }

    /**
     * 创建普通动态字段。
     *
     * @param name     字段名
     * @param dataType 逻辑数据类型
     * @return 动态字段
     */
    public static DynamicField of(String name, String dataType) {
        return new DynamicField(name, dataType, false, true, false,
                                null, null, null, null, ValueGeneration.none());
    }

    /**
     * 创建主键动态字段。
     *
     * @param name     字段名
     * @param dataType 逻辑数据类型
     * @return 主键动态字段
     */
    public static DynamicField primaryKey(String name, String dataType) {
        return new DynamicField(name, dataType, true, false, false,
                                null, null, null, null, ValueGeneration.none());
    }

    /**
     * 返回仅替换可空性的新字段。主键字段会继续保持不可空，避免生成自相矛盾的目标结构。
     *
     * @param nullable 是否允许数据库空值
     * @return 新字段定义
     */
    public DynamicField withNullable(boolean nullable) {
        return new DynamicField(name, dataType, primaryKey, nullable, unique,
                                length, precision, scale, comment, generation);
    }

    /**
     * 返回仅替换单列唯一约束声明的新字段。
     *
     * @param unique 是否生成唯一索引
     * @return 新字段定义
     */
    public DynamicField withUnique(boolean unique) {
        return new DynamicField(name, dataType, primaryKey, nullable, unique,
                                length, precision, scale, comment, generation);
    }

    /** @return 仅替换长度的新字段定义 */
    public DynamicField withLength(Integer length) {
        return new DynamicField(name, dataType, primaryKey, nullable, unique,
                                length, precision, scale, comment, generation);
    }

    /** @return 仅替换精度和小数位的新字段定义 */
    public DynamicField withPrecision(Integer precision, Integer scale) {
        return new DynamicField(name, dataType, primaryKey, nullable, unique,
                                length, precision, scale, comment, generation);
    }

    /** @return 仅替换字段注释的新字段定义 */
    public DynamicField withComment(String comment) {
        return new DynamicField(name, dataType, primaryKey, nullable, unique,
                                length, precision, scale, comment, generation);
    }

    /**
     * 声明由数据库标识列生成值。业务插入时可以省略该字段，DDL 由当前数据库方言决定具体语法。
     */
    public DynamicField generatedByIdentity() {
        return withGeneration(ValueGeneration.identity());
    }

    /**
     * 声明带起点、步长和缓存大小的标识列。
     */
    public DynamicField generatedByIdentity(long startWith, long incrementBy, int cacheSize) {
        return withGeneration(ValueGeneration.identity(startWith, incrementBy, cacheSize));
    }

    /**
     * 声明由命名序列生成值。序列名字随后还会经过方言标识符白名单校验。
     */
    public DynamicField generatedBySequence(String sequenceName) {
        return withGeneration(ValueGeneration.sequence(sequenceName));
    }

    /**
     * 声明带起点、步长和缓存大小的命名序列。
     */
    public DynamicField generatedBySequence(String sequenceName,
                                            long startWith,
                                            long incrementBy,
                                            int cacheSize) {
        return withGeneration(ValueGeneration.sequence(sequenceName, startWith, incrementBy, cacheSize));
    }

    /** @return 只替换生成策略的新字段，原字段保持不变 */
    public DynamicField withGeneration(ValueGeneration generation) {
        return new DynamicField(name,
                                dataType,
                                primaryKey,
                                nullable,
                                unique,
                                length,
                                precision,
                                scale,
                                comment,
                                Objects.requireNonNull(generation, "dynamic field value generation must not be null"));
    }

    /**
     * 返回规范化字段名，用于高频查找和版本 diff。
     *
     * @return 规范化字段名
     */
    public String normalizedName() {
        return FormNames.normalize(name, "dynamic field name");
    }

    /**
     * 转换为列元数据。
     *
     * @return 列元数据
     */
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata column = primaryKey ? ColumnMetadata.primaryKey(name, dataType) : ColumnMetadata.of(name, dataType);
        return column.withLength(length)
                     .withPrecision(precision, scale)
                     .withComment(comment)
                     .withNullable(nullable)
                     .withGeneration(generation);
    }

    private static Integer requirePositive(Integer value, String fieldName) {
        if (value != null && value < 1) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }

    private static Integer requireNonNegative(Integer value, String fieldName) {
        if (value != null && value < 0) {
            throw new IllegalArgumentException(fieldName + " must not be negative");
        }
        return value;
    }

    private static String normalizeOptionalText(String value) {
        if (value == null) {
            return null;
        }
        String text = value.trim();
        return text.isEmpty() ? null : text;
    }
}
