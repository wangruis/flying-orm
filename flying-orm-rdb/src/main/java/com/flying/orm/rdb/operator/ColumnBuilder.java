package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;

/**
 * 一次新增列操作的可变构建器。
 *
 * <p>先用 {@link #name(String)} 和类型方法补齐列定义，再调用 {@link #commit()} 把列交回表级构建器。
 * 本对象只保存当前这一列的临时状态，不要跨请求或跨线程复用。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class ColumnBuilder {

    private final CreateOrAlterTableBuilder table;

    private String name;

    private String dataType;

    private boolean primaryKey;

    private Integer length;

    private Integer precision;

    private Integer scale;

    private String comment;

    ColumnBuilder(CreateOrAlterTableBuilder table) {
        this.table = table;
    }

    /**
     * 设置列名。这里只接受普通 SQL 标识符，不接受带引号、函数或 SQL 片段的文本。
     *
     * @param name 物理列名
     * @return 当前列构建器
     */
    public ColumnBuilder name(String name) {
        this.name = CreateOrAlterTableBuilder.requireText(name, "column name");
        return this;
    }

    /**
     * 按十进制精度选择常用数值类型：较小精度使用 INTEGER 或 BIGINT，更大精度使用 DECIMAL。
     *
     * @param precision 十进制总位数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder number(int precision) {
        if (precision < 1) {
            throw new IllegalArgumentException("number precision must be positive");
        }
        this.dataType = precision <= 10 ? "INTEGER" : precision <= 19 ? "BIGINT" : "DECIMAL";
        if ("DECIMAL".equals(this.dataType)) {
            this.precision = precision;
            this.scale = 0;
        }
        return this;
    }

    /**
     * 声明 VARCHAR 列。
     *
     * @param length 最大字符数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder varchar(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("varchar length must be positive");
        }
        this.dataType = "VARCHAR";
        this.length = length;
        return this;
    }

    /**
     * 把当前列标记为主键列。复合主键可以在同一张表里对多列调用本方法。
     *
     * @return 当前列构建器
     */
    public ColumnBuilder primaryKey() {
        this.primaryKey = true;
        return this;
    }

    /**
     * 设置数据库列注释。注释作为普通元数据保存，最终由方言决定使用行内还是独立 COMMENT SQL。
     *
     * @param comment 非空列注释
     * @return 当前列构建器
     */
    public ColumnBuilder comment(String comment) {
        this.comment = CreateOrAlterTableBuilder.requireText(comment, "column comment");
        return this;
    }

    /**
     * 校验列名和类型，并把不可变字段定义加入表级迁移计划。调用后应继续使用返回的表构建器。
     *
     * @return 所属表的构建器
     */
    public CreateOrAlterTableBuilder commit() {
        String safeName = CreateOrAlterTableBuilder.requireText(name, "column name");
        String safeType = CreateOrAlterTableBuilder.requireText(dataType, "column data type");
        DynamicField field = primaryKey ? DynamicField.primaryKey(safeName, safeType) : DynamicField.of(safeName, safeType);
        table.addField(field.withLength(length)
                            .withPrecision(precision, scale)
                            .withComment(comment));
        return table;
    }
}
