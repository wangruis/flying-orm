package com.flying.orm.core.metadata;

/**
 * 列元数据描述单个数据库列的稳定属性，构建后可在线程之间安全共享。
 *
 * @param name       原始列名，保留调用方传入的可读形式
 * @param dataType   逻辑数据类型名称，后续由方言映射为具体数据库类型
 * @param primaryKey 是否属于主键列
 * @param nullable   数据库列是否允许空值；主键始终为 false
 * @param length     字符或二进制列长度；未指定时为空
 * @param precision  数值列总有效位数；未指定时为空
 * @param scale      数值列小数位数；未指定时为空且不能脱离 precision
 * @param comment    可选列注释；空白会规范化为空
 * @param generation 数据库值生成策略；不会为空
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record ColumnMetadata(String name,
                             String dataType,
                             boolean primaryKey,
                             boolean nullable,
                             Integer length,
                             Integer precision,
                             Integer scale,
                             String comment,
                             ValueGeneration generation) {

    /**
     * 创建列元数据并完成基础空值校验。
     *
     * @param name       原始列名
     * @param dataType   逻辑数据类型名称
     * @param primaryKey 是否主键
     * @param nullable   是否允许数据库空值
     * @param length     可选长度
     * @param precision  可选精度
     * @param scale      可选小数位
     * @param comment    可选列注释
     * @param generation 数据库值生成策略
     */
    public ColumnMetadata {
        // 精度和小数位在元数据入口一次校验，避免把明显无效的定义传到不同方言后得到不同错误。
        name = MetadataNames.requireText(name, "column name");
        dataType = MetadataNames.requireText(dataType, "column data type");
        nullable = !primaryKey && nullable;
        length = requirePositive(length, "column length");
        precision = requirePositive(precision, "column precision");
        scale = requireNonNegative(scale, "column scale");
        if (scale != null && precision == null) {
            throw new IllegalArgumentException("column precision must be set when scale is set");
        }
        if (precision != null && scale != null && scale > precision) {
            throw new IllegalArgumentException("column scale must not be greater than precision");
        }
        comment = normalizeOptionalText(comment);
        generation = java.util.Objects.requireNonNull(generation, "column value generation must not be null");
    }

    /**
     * 创建普通列元数据。
     *
     * @param name     原始列名
     * @param dataType 逻辑数据类型名称
     * @return 普通列元数据
     */
    public static ColumnMetadata of(String name, String dataType) {
        return new ColumnMetadata(name, dataType, false, true,
                                  null, null, null, null, ValueGeneration.none());
    }

    /**
     * 创建主键列元数据。
     *
     * @param name     原始列名
     * @param dataType 逻辑数据类型名称
     * @return 主键列元数据
     */
    public static ColumnMetadata primaryKey(String name, String dataType) {
        return new ColumnMetadata(name, dataType, true, false,
                                  null, null, null, null, ValueGeneration.none());
    }

    /**
     * 返回仅替换可空性的新列。主键列始终保持不可空。
     *
     * @param nullable 是否允许数据库空值
     * @return 新列定义
     */
    public ColumnMetadata withNullable(boolean nullable) {
        return new ColumnMetadata(name, dataType, primaryKey, nullable,
                                  length, precision, scale, comment, generation);
    }

    /**
     * 返回仅替换长度的新列定义，原对象保持不变，适合链式构建和缓存快照复用。
     *
     * @param length 字符或二进制类型长度，必须大于零
     * @return 新列定义
     */
    public ColumnMetadata withLength(Integer length) {
        return new ColumnMetadata(name, dataType, primaryKey, nullable,
                                  length, precision, scale, comment, generation);
    }

    /**
     * 返回仅替换数值精度的新列定义。scale 不能脱离 precision 单独存在，也不能大于 precision。
     *
     * @param precision 总有效位数
     * @param scale 小数位数
     * @return 新列定义
     */
    public ColumnMetadata withPrecision(Integer precision, Integer scale) {
        return new ColumnMetadata(name, dataType, primaryKey, nullable,
                                  length, precision, scale, comment, generation);
    }

    /**
     * 返回仅替换注释的新列定义；空白注释会被统一收口成 null。
     *
     * @param comment 列注释
     * @return 新列定义
     */
    public ColumnMetadata withComment(String comment) {
        return new ColumnMetadata(name, dataType, primaryKey, nullable,
                                  length, precision, scale, comment, generation);
    }

    /** @return 只替换数据库生成方式的新列定义 */
    public ColumnMetadata withGeneration(ValueGeneration generation) {
        return new ColumnMetadata(name,
                                  dataType,
                                  primaryKey,
                                  nullable,
                                  length,
                                  precision,
                                  scale,
                                  comment,
                                  java.util.Objects.requireNonNull(generation,
                                                                   "column value generation must not be null"));
    }

    /**
     * 返回规范化列名，用于构建高频查找索引。
     *
     * @return 小写且去除首尾空白后的列名
     */
    public String normalizedName() {
        return MetadataNames.normalize(name, "column name");
    }

    /**
     * 返回主键版本的当前列，便于构建器以声明式方式调整列角色。
     *
     * @return 主键列元数据
     */
    public ColumnMetadata asPrimaryKey() {
        if (primaryKey) {
            return this;
        }
        return new ColumnMetadata(name, dataType, true, false,
                                  length, precision, scale, comment, generation);
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
