package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.List;

/**
 * 外键元数据只描述关系，不负责决定要不要真的建外键。
 * 动态表单场景里，上层可以先看计划，再决定是否人工迁移。
 *
 * @param name             外键名
 * @param columns          当前表里的列，顺序要和引用列对应
 * @param referenceTable   被引用的表
 * @param referenceColumns 被引用表里的列
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record ForeignKeyMetadata(String name,
                                 List<String> columns,
                                 String referenceTable,
                                 List<String> referenceColumns) {

    public ForeignKeyMetadata {
        // 外键的两组列按位置配对。这里一次性校验，后面的差异比较和 DDL 渲染就不用重复防御。
        name = Names.requireText(name, "foreign key name");
        columns = normalizeColumns(columns, "foreign key column name");
        referenceTable = Names.requireText(referenceTable, "foreign key reference table");
        referenceColumns = normalizeColumns(referenceColumns, "foreign key reference column name");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("foreign key columns must not be empty");
        }
        if (referenceColumns.isEmpty()) {
            throw new IllegalArgumentException("foreign key reference columns must not be empty");
        }
        if (columns.size() != referenceColumns.size()) {
            throw new IllegalArgumentException("foreign key column count must match reference column count");
        }
    }

    /** @return 用于元数据匹配的规范化外键名 */
    public String normalizedName() {
        return Names.key(name, "foreign key name");
    }

    /** @return 用于元数据匹配的规范化引用表名 */
    public String normalizedReferenceTable() {
        return Names.key(referenceTable, "foreign key reference table");
    }

    /**
     * 创建适合链式收集复合外键列的构建器。
     *
     * @param name 外键名
     * @return 新构建器
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    private static List<String> normalizeColumns(List<String> values, String fieldName) {
        return List.copyOf(values.stream()
                                 .map(value -> Names.requireText(value, fieldName))
                                 .toList());
    }

    /**
     * 可变构建阶段只在调用线程内使用；{@link #build()} 后得到的 ForeignKeyMetadata 会复制所有列集合。
     */
    public static final class Builder {

        private final String name;

        private final List<String> columns = new ArrayList<>();

        private String referenceTable;

        private final List<String> referenceColumns = new ArrayList<>();

        private Builder(String name) {
            this.name = Names.requireText(name, "foreign key name");
        }

        /** 添加当前表的一列，调用顺序就是复合外键的列顺序。 */
        public Builder addColumn(String columnName) {
            columns.add(Names.requireText(columnName, "foreign key column name"));
            return this;
        }

        /** 指定被引用的表。表名只做元数据规范化，是否存在由迁移检查阶段确认。 */
        public Builder referenceTable(String tableName) {
            referenceTable = Names.requireText(tableName, "foreign key reference table");
            return this;
        }

        /** 添加引用表的一列，必须与 {@link #addColumn(String)} 的位置对应。 */
        public Builder addReferenceColumn(String columnName) {
            referenceColumns.add(Names.requireText(columnName, "foreign key reference column name"));
            return this;
        }

        /**
         * 生成不可变外键定义；缺表名、空列或列数量不一致都会在这里明确失败。
         *
         * @return 校验后的外键元数据
         */
        public ForeignKeyMetadata build() {
            return new ForeignKeyMetadata(name, columns, referenceTable, referenceColumns);
        }
    }
}
