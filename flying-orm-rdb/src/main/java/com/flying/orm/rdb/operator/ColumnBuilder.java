package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ValueGeneration;

import java.util.Objects;

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
public final class ColumnBuilder<T> {

    private final T table;
    private final DdlStructureDraft draft;
    private String name;
    private String dataType;
    private boolean primaryKey;
    private boolean nullable = true;
    private boolean unique;
    private Integer length;
    private Integer precision;
    private Integer scale;
    private String comment;
    private ValueGeneration generation = ValueGeneration.none();

    ColumnBuilder(T table, DdlStructureDraft draft) {
        this.table = table;
        this.draft = draft;
    }

    /**
     * 设置列名。这里只接受普通 SQL 标识符，不接受带引号、函数或 SQL 片段的文本。
     *
     * @param name 物理列名
     * @return 当前列构建器
     */
    public ColumnBuilder<T> name(String name) {
        this.name = DdlStructureDraft.requireText(name, "column name");
        return this;
    }

    /**
     * 按十进制精度选择常用数值类型：较小精度使用 INTEGER 或 BIGINT，更大精度使用 DECIMAL。
     *
     * @param precision 十进制总位数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder<T> number(int precision) {
        if (precision < 1) {
            throw new IllegalArgumentException("number precision must be positive");
        }
        type(precision <= 10 ? "INTEGER" : precision <= 19 ? "BIGINT" : "DECIMAL");
        this.precision = "DECIMAL".equals(dataType) ? precision : null;
        scale = this.precision == null ? null : 0;
        return this;
    }

    /** 声明 DECIMAL 的总位数和小数位数，字段规则在 commit 时统一校验。 */
    public ColumnBuilder<T> number(int precision, int scale) {
        type("DECIMAL");
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    /** 设置受控类型声明，如 TIMESTAMP(6)、BOOLEAN；替换类型时清除之前的长度和精度。 */
    public ColumnBuilder<T> type(String type) {
        dataType = DdlStructureDraft.requireText(type, "column data type");
        length = null;
        precision = null;
        scale = null;
        return this;
    }

    /** 设置可空性；主键始终不可空。 */
    public ColumnBuilder<T> nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    /** 设置是否生成单列唯一索引。 */
    public ColumnBuilder<T> unique(boolean unique) {
        this.unique = unique;
        return this;
    }

    /** 使用现有值生成模型设置标识列或序列，具体语法由方言决定。 */
    public ColumnBuilder<T> generation(ValueGeneration generation) {
        this.generation = Objects.requireNonNull(generation, "value generation must not be null");
        return this;
    }

    /**
     * 声明 VARCHAR 列。
     *
     * @param length 最大字符数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder<T> varchar(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("varchar length must be positive");
        }
        type("VARCHAR");
        this.length = length;
        return this;
    }

    /**
     * 把当前列标记为主键列。复合主键可以在同一张表里对多列调用本方法。
     *
     * @return 当前列构建器
     */
    public ColumnBuilder<T> primaryKey() {
        primaryKey = true;
        return this;
    }

    /**
     * 设置数据库列注释。注释作为普通元数据保存，最终由方言决定使用行内还是独立 COMMENT SQL。
     *
     * @param comment 非空列注释
     * @return 当前列构建器
     */
    public ColumnBuilder<T> comment(String comment) {
        this.comment = DdlStructureDraft.requireText(comment, "column comment");
        return this;
    }

    /**
     * 校验列名和类型，并把不可变字段定义加入表级迁移计划。调用后应继续使用返回的表构建器。
     *
     * @return 所属表的构建器
     */
    public T commit() {
        ColumnDefinition column = ColumnDefinition.builder(
                        DdlStructureDraft.requireText(name, "column name"),
                        DdlStructureDraft.requireText(dataType, "column data type"))
                .nullable(!primaryKey && nullable).length(length).precision(precision).scale(scale)
                .comment(comment).generation(generation).build();
        draft.addColumn(new DynamicField(column, primaryKey, unique));
        return table;
    }
}
