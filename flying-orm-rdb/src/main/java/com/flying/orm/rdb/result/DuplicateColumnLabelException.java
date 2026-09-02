package com.flying.orm.rdb.result;

/**
 * 查询结果里出现了两个完全相同的列标签。
 *
 * <p>普通 Map 会让后一列悄悄覆盖前一列，这对联表查询尤其危险。flying-orm 直接拒绝这种结果，
 * 使用方给重复列加上明确别名后再执行。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class DuplicateColumnLabelException extends IllegalArgumentException {

    private final String columnLabel;

    private final int firstIndex;

    private final int duplicateIndex;

    DuplicateColumnLabelException(String columnLabel, int firstIndex, int duplicateIndex) {
        super("query result contains duplicate column label at indexes "
                + firstIndex + " and " + duplicateIndex);
        this.columnLabel = columnLabel;
        this.firstIndex = firstIndex;
        this.duplicateIndex = duplicateIndex;
    }

    public String columnLabel() {
        return columnLabel;
    }

    public int firstIndex() {
        return firstIndex;
    }

    public int duplicateIndex() {
        return duplicateIndex;
    }
}
