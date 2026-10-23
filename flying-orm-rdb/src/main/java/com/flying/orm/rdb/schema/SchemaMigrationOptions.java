package com.flying.orm.rdb.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * createOrAlter 的显式结构变更选项。
 * <p>
 * 默认 safe() 保留未选择的已有结构；调用 allowXxx() 显式选择相应变化。
 *
 * @param dropColumnAllowed       是否允许删掉库里多出来的列
 * @param columnChangeAllowed     是否允许改列类型、长度、精度或小数位
 * @param dropIndexAllowed        是否允许删掉库里多出来的索引
 * @param rebuildIndexAllowed     是否允许重建定义不一致的索引
 * @param columnRenames           明确声明的旧字段名到新字段名映射
 * @param concurrentIndexesPreferred 是否优先使用方言支持的并发索引语法
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SchemaMigrationOptions(boolean dropColumnAllowed,
                                     boolean columnChangeAllowed,
                                     boolean dropIndexAllowed,
                                     boolean rebuildIndexAllowed,
                                     Map<String, String> columnRenames,
                                     boolean concurrentIndexesPreferred) {

    /** 优先使用方言支持的并发索引语法；数据库会话和上线策略仍由调用方管理。 */
    public SchemaMigrationOptions preferConcurrentIndexes() {
        return new SchemaMigrationOptions(dropColumnAllowed, columnChangeAllowed,
                dropIndexAllowed, rebuildIndexAllowed, columnRenames, true);
    }

    public SchemaMigrationOptions {
        columnRenames = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(
                columnRenames,
                "column renames must not be null")));
    }

    public static SchemaMigrationOptions safe() {
        return new SchemaMigrationOptions(false, false, false, false, Map.of(), false);
    }

    public SchemaMigrationOptions allowDropColumn() {
        return new SchemaMigrationOptions(true,
                                          columnChangeAllowed,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          columnRenames, concurrentIndexesPreferred);
    }

    public SchemaMigrationOptions allowColumnChange() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          true,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          columnRenames, concurrentIndexesPreferred);
    }

    public SchemaMigrationOptions allowDropIndex() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          true,
                                          rebuildIndexAllowed,
                                          columnRenames, concurrentIndexesPreferred);
    }

    public SchemaMigrationOptions allowRebuildIndex() {
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          dropIndexAllowed,
                                          true,
                                          columnRenames, concurrentIndexesPreferred);
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
        if (safeOldName.equals(safeNewName)) {
            throw new IllegalArgumentException("old and new column names must be different");
        }
        LinkedHashMap<String, String> renames = new LinkedHashMap<>(columnRenames);
        renames.forEach((existingOld, existingNew) -> {
            if (existingOld.equals(safeOldName)) {
                throw new IllegalArgumentException("column rename already exists for source");
            }
            if (existingNew.equals(safeNewName)) {
                throw new IllegalArgumentException("column rename target is already used");
            }
        });
        renames.put(safeOldName, safeNewName);
        return new SchemaMigrationOptions(dropColumnAllowed,
                                          columnChangeAllowed,
                                          dropIndexAllowed,
                                          rebuildIndexAllowed,
                                          renames, concurrentIndexesPreferred);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
