package com.flying.orm.rdb.operator;

/**
 * 同步列定义 builder。
 *
 * <p>构建器只保存当前列的临时描述，调用 commit 后转换成不可变字段并交回表构建器。
 * 每次 {@code addColumn()} 都创建新实例，因此不要跨线程或跨请求复用。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncColumnBuilder {

    private final SyncCreateOrAlterTableBuilder table;
    private final DdlStructureDraft.ColumnDraft column;

    SyncColumnBuilder(SyncCreateOrAlterTableBuilder table) {
        this.table = table;
        this.column = table.draft().column();
    }

    /** 设置安全的物理列名。 */
    public SyncColumnBuilder name(String name) {
        column.name(name);
        return this;
    }

    /** 根据十进制精度选择 INTEGER、BIGINT 或 DECIMAL。 */
    public SyncColumnBuilder number(int precision) {
        column.number(precision);
        return this;
    }

    /** 声明 VARCHAR 列，长度必须大于零。 */
    public SyncColumnBuilder varchar(int length) {
        column.varchar(length);
        return this;
    }

    /** 把当前列标记为主键列。 */
    public SyncColumnBuilder primaryKey() {
        column.primaryKey();
        return this;
    }

    /** 设置列注释，由方言决定最终使用行内定义还是独立 COMMENT SQL。 */
    public SyncColumnBuilder comment(String comment) {
        column.comment(comment);
        return this;
    }

    /** 校验列定义后加入当前表草稿，并返回表级 builder 继续描述。 */
    public SyncCreateOrAlterTableBuilder commit() {
        column.commit();
        return table;
    }
}
