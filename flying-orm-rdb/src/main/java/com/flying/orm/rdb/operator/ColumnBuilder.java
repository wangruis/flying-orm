package com.flying.orm.rdb.operator;

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
    private final DdlStructureDraft.ColumnDraft column;

    ColumnBuilder(CreateOrAlterTableBuilder table) {
        this.table = table;
        this.column = table.draft().column();
    }

    /**
     * 设置列名。这里只接受普通 SQL 标识符，不接受带引号、函数或 SQL 片段的文本。
     *
     * @param name 物理列名
     * @return 当前列构建器
     */
    public ColumnBuilder name(String name) {
        column.name(name);
        return this;
    }

    /**
     * 按十进制精度选择常用数值类型：较小精度使用 INTEGER 或 BIGINT，更大精度使用 DECIMAL。
     *
     * @param precision 十进制总位数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder number(int precision) {
        column.number(precision);
        return this;
    }

    /**
     * 声明 VARCHAR 列。
     *
     * @param length 最大字符数，必须大于零
     * @return 当前列构建器
     */
    public ColumnBuilder varchar(int length) {
        column.varchar(length);
        return this;
    }

    /**
     * 把当前列标记为主键列。复合主键可以在同一张表里对多列调用本方法。
     *
     * @return 当前列构建器
     */
    public ColumnBuilder primaryKey() {
        column.primaryKey();
        return this;
    }

    /**
     * 设置数据库列注释。注释作为普通元数据保存，最终由方言决定使用行内还是独立 COMMENT SQL。
     *
     * @param comment 非空列注释
     * @return 当前列构建器
     */
    public ColumnBuilder comment(String comment) {
        column.comment(comment);
        return this;
    }

    /**
     * 校验列名和类型，并把不可变字段定义加入表级迁移计划。调用后应继续使用返回的表构建器。
     *
     * @return 所属表的构建器
     */
    public CreateOrAlterTableBuilder commit() {
        column.commit();
        return table;
    }
}
