package com.flying.orm.rdb.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * createOrAlter 的危险动作开关。
 * <p>
 * 默认 safe() 什么危险动作都不做；调用 allowXxx() 才会把对应动作放进执行计划。
 *
 * @param dropColumnAllowed       是否允许删掉库里多出来的列
 * @param columnChangeAllowed     是否允许改列类型、长度、精度或小数位
 * @param primaryKeyChangeAllowed 是否允许主键变化进入危险迁移策略
 * @param dropIndexAllowed        是否允许删掉库里多出来的索引
 * @param rebuildIndexAllowed     是否允许重建定义不一致的索引
 * @param columnRenames           明确声明的旧字段名到新字段名映射
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SchemaMigrationOptions(boolean dropColumnAllowed,
                                     boolean columnChangeAllowed,
                                     boolean primaryKeyChangeAllowed,
                                     boolean dropIndexAllowed,
                                     boolean rebuildIndexAllowed,
                                     Map<String, String> columnRenames) {

    public SchemaMigrationOptions(boolean dropColumnAllowed,
                                  boolean columnChangeAllowed,
                                  boolean primaryKeyChangeAllowed,
                                  boolean dropIndexAllowed,
                                  boolean rebuildIndexAllowed) {
        this(dropColumnAllowed,
             columnChangeAllowed,
             primaryKeyChangeAllowed,
             dropIndexAllowed,
             rebuildIndexAllowed,
             Map.of());
    }

    public SchemaMigrationOptions {
        columnRenames = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(
                columnRenames,
                "column renames must not be null")));
    }

    public static SchemaMigrationOptions safe() {
        return new SchemaMigrationOptions(false, false, false, false, false, Map.of());
    }

    /**
     * 判断这份配置是否可能生成破坏性 DDL。只要显式放开删除、改型、主键、索引重建或字段重命名，
     * 就必须先走审核计划，不能再使用自动执行入口跳过风险确认。
     *
     * @return 需要先审核再执行时返回 true
     */
    public boolean requiresReviewedExecution() {
        return dropColumnAllowed
                || columnChangeAllowed
                || primaryKeyChangeAllowed
                || dropIndexAllowed
                || rebuildIndexAllowed
                || !columnRenames.isEmpty();
    }

    public SchemaMigrationOptions allowDropColumn() {
        return new SchemaMigrationOptions(true,
                                          columnChangeAllowed,
                                          primaryKeyChangeAllowed,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          columnRenames);
    }

    public SchemaMigrationOptions allowColumnChange() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          true,
                                          primaryKeyChangeAllowed,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          columnRenames);
    }

    public SchemaMigrationOptions allowPrimaryKeyChange() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          true,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          columnRenames);
    }

    public SchemaMigrationOptions allowDropIndex() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          primaryKeyChangeAllowed,
                                          true,
                                          rebuildIndexAllowed,
                                          columnRenames);
    }

    public SchemaMigrationOptions allowRebuildIndex() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          primaryKeyChangeAllowed,
                                          dropIndexAllowed,
                                          true,
                                          columnRenames);
    }

    /**
     * 明确告诉迁移器某个字段是改名，不要把它当成“新增一个、删除一个”。
     * flying-orm 不会按类型或位置猜字段重命名，因为猜错就可能丢数据。
     *
     * @param oldName 数据库当前字段名
     * @param newName 目标表单字段名
     * @return 带上这条重命名声明的新配置
     */
    public SchemaMigrationOptions renameColumn(String oldName, String newName) {
        String safeOldName = requireText(oldName, "old column name");
        String safeNewName = requireText(newName, "new column name");
        if (safeOldName.equalsIgnoreCase(safeNewName)) {
            throw new IllegalArgumentException("old and new column names must be different");
        }
        LinkedHashMap<String, String> renames = new LinkedHashMap<>(columnRenames);
        renames.forEach((existingOld, existingNew) -> {
            if (existingOld.equalsIgnoreCase(safeOldName)) {
                throw new IllegalArgumentException("column rename already exists for source");
            }
            if (existingNew.equalsIgnoreCase(safeNewName)) {
                throw new IllegalArgumentException("column rename target is already used");
            }
        });
        renames.put(safeOldName, safeNewName);
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          primaryKeyChangeAllowed,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          renames);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
