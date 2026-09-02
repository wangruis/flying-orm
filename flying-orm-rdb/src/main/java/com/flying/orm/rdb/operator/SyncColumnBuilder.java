package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;

import java.util.Objects;

/**
 * 同步列定义 builder。
 *
 * <p>构建器只保存当前列的临时描述，调用 commit 后转换成不可变的 {@link DynamicField} 并交回表构建器。
 * 每次 {@code addColumn()} 都创建新实例，因此不要跨线程或跨请求复用。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncColumnBuilder {

    private final SyncCreateOrAlterTableBuilder table;
    private final SyncDdlStructureState jdbcState;
    private String name;
    private String dataType;
    private boolean primaryKey;
    private Integer length;
    private Integer precision;
    private Integer scale;
    private String comment;

    SyncColumnBuilder(SyncDdlStructureState jdbcState, SyncCreateOrAlterTableBuilder table) {
        this.table = Objects.requireNonNull(table, "table builder must not be null");
        this.jdbcState = Objects.requireNonNull(jdbcState, "sync DDL state must not be null");
    }

    /** 设置安全的物理列名。 */
    public SyncColumnBuilder name(String name) {
        this.name = CreateOrAlterTableBuilder.requireText(name, "column name");
        return this;
    }

    /** 根据十进制精度选择 INTEGER、BIGINT 或 DECIMAL。 */
    public SyncColumnBuilder number(int precision) {
        if (precision < 1) {
            throw new IllegalArgumentException("number precision must be positive");
        }
        dataType = precision <= 10 ? "INTEGER" : precision <= 19 ? "BIGINT" : "DECIMAL";
        if ("DECIMAL".equals(dataType)) {
            this.precision = precision;
            scale = 0;
        }
        return this;
    }

    /** 声明 VARCHAR 列，长度必须大于零。 */
    public SyncColumnBuilder varchar(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("varchar length must be positive");
        }
        dataType = "VARCHAR";
        this.length = length;
        return this;
    }

    /** 把当前列标记为主键列。 */
    public SyncColumnBuilder primaryKey() {
        primaryKey = true;
        return this;
    }

    /** 设置列注释，由方言决定最终使用行内定义还是独立 COMMENT SQL。 */
    public SyncColumnBuilder comment(String comment) {
        this.comment = CreateOrAlterTableBuilder.requireText(comment, "column comment");
        return this;
    }

    /** 校验列定义后加入当前表草稿，并返回表级 builder 继续描述。 */
    public SyncCreateOrAlterTableBuilder commit() {
        String safeName = CreateOrAlterTableBuilder.requireText(name, "column name");
        String safeType = CreateOrAlterTableBuilder.requireText(dataType, "column data type");
        DynamicField field = primaryKey
                ? DynamicField.primaryKey(safeName, safeType)
                : DynamicField.of(safeName, safeType);
        jdbcState.addField(field.withLength(length).withPrecision(precision, scale).withComment(comment));
        return table;
    }
}
