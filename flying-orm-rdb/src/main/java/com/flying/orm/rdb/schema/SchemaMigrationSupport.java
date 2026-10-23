package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 迁移规划中共用的纯判断与目标收集方法。
 *
 * <p>这些方法不创建数据库连接，也不持有渲染器状态。单独放出来是为了让迁移编排代码只表达
 * “先比较什么、再生成什么”，而不是被大小写和改名细节淹没。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class SchemaMigrationSupport {

    private SchemaMigrationSupport() {
    }

    static RelationalTableDefinition tableDefinition(DynamicForm form, List<IndexDefinition> indexes,
                                                      List<ForeignKeyDefinition> foreignKeys) {
        return tableDefinition(form, indexes, foreignKeys, null);
    }

    static RelationalTableDefinition tableDefinition(DynamicForm form, List<IndexDefinition> indexes,
                                                      List<ForeignKeyDefinition> foreignKeys,
                                                      RelationalTableDefinition current) {
        RelationIdentity relation = form.relationIdentity();
        RelationalTableDefinition.Builder target = RelationalTableDefinition.builder(relation);
        form.fields().forEach(field -> target.addColumn(field.column()));
        if (current != null) {
            // 增量索引或外键可以引用未重复声明的已有列；使用回读事实，不编造列定义。
            Set<String> references = new LinkedHashSet<>();
            indexes.forEach(index -> references.addAll(index.columns()));
            foreignKeys.forEach(key -> references.addAll(key.columns()));
            for (String name : references) {
                if (form.findField(name).isEmpty()) current.findColumn(name).ifPresent(target::addColumn);
            }
        }
        List<String> primaryKeys = targetPrimaryKeys(form);
        if (!primaryKeys.isEmpty()) {
            target.primaryKey(PrimaryKeyDefinition.of("pk_" + relation.table(), primaryKeys.toArray(String[]::new)));
        }
        indexes.forEach(target::addIndex);
        foreignKeys.forEach(target::addForeignKey);
        return target.build();
    }

    static RelationIdentity relation(String table) {
        return RelationIdentity.parse(SqlIdentifiers.requireIdentifier(table));
    }

    static RelationalTableDefinition physicalColumns(SchemaSnapshot snapshot) {
        if (snapshot == null) {
            return null;
        }
        Set<SchemaSnapshot.UnknownAttribute> unknown = snapshot.unknownAttributes();
        if (snapshot.tableState() != SchemaSnapshot.State.PRESENT
                || snapshot.columns().state() != SchemaSnapshot.State.PRESENT
                || unknown.contains(SchemaSnapshot.UnknownAttribute.COLUMN_DEFAULT)
                || unknown.contains(SchemaSnapshot.UnknownAttribute.COLUMN_GENERATION)
                || unknown.contains(SchemaSnapshot.UnknownAttribute.COLUMN_CHARSET)
                || unknown.contains(SchemaSnapshot.UnknownAttribute.COLUMN_COLLATION)) {
            throw new IllegalStateException(
                    "rewriting an existing column requires known physical column attributes");
        }
        return snapshot.knownDefinition();
    }

    /** 同步与响应式执行共享同一份精确失效目标，不负责实际元数据读取或失效。 */
    static List<RelationIdentity> metadataRelations(SchemaMigrationPlan plan) {
        LinkedHashSet<RelationIdentity> tables = new LinkedHashSet<>();
        tables.add(plan.targetIdentity().orElseThrow(() ->
                new IllegalArgumentException("schema plan must contain a target relation")));
        tables.addAll(plan.additionalCreatedTables());
        return List.copyOf(tables);
    }

    static List<RelationIdentity> metadataTables(DynamicForm form) {
        return metadataTables(form, List.of());
    }

    static List<RelationIdentity> metadataTables(DynamicForm form, List<RelationIdentity> additionalTables) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        LinkedHashSet<RelationIdentity> tables = new LinkedHashSet<>();
        tables.add(safeForm.relationIdentity());
        tables.addAll(Objects.requireNonNull(additionalTables, "additional schema tables must not be null"));
        ProtectedContainsLayout.resolve(safeForm)
                               .map(ProtectedContainsLayout::table)
                               .map(DynamicForm::relationIdentity)
                               .ifPresent(tables::add);
        return List.copyOf(tables);
    }

    /** 增量列选择只执行一次；null 表示将变更加入实际操作。 */
    static String columnChangeSkipReason(ColumnDefinition current, DynamicField target,
                                         SchemaMigrationOptions options, RelationalSchemaSqlRenderer tables) {
        boolean temporalChanged = logicalTemporalTypeChanged(current.databaseType(), target.databaseType());
        boolean storageChanged = temporalChanged || !sameStorageShape(current, target, tables);
        boolean nullableChanged = current.nullable() != target.nullable();
        if (nullableChanged && !options.columnChangeAllowed()) {
            if (storageChanged) return "combined nullable and storage changes require explicit column change";
            if (!target.nullable()) return "SAFE mode does not make a nullable column NOT NULL";
        }
        if (!storageChanged || ProtectedSchemaTarget.sameProtectedStorage(current, target)) {
            return null;
        }
        if (!nullableChanged && !temporalChanged && safeWidening(current, target, tables)) {
            return null;
        }
        if (!options.columnChangeAllowed()) {
            return temporalChanged
                    ? "SAFE mode does not reinterpret existing values as a different time type"
                    : "SAFE mode does not change column type, length, precision, or scale";
        }
        return SchemaGeneratedValueComparison.same(current.generation(), target.generation(), tables.generatedValueStyle())
                ? null : "generated value changes are not supported by incremental column changes";
    }

    /**
     * 判断一次类型参数变化是否只会扩大可存储范围。
     *
     * <p>这里只自动放行同一种数据库类型的长度或精度扩宽，不猜测 {@code INT -> BIGINT} 之类的跨类型转换。
     * DECIMAL 除了总精度不能缩小，小数位和整数位容量也都不能减少；任何元数据缺失都会保守返回 false。</p>
     */
    static boolean safeWidening(ColumnDefinition current,
                                DynamicField target,
                                RelationalSchemaSqlRenderer tables) {
        if (current.nullable() != target.nullable()
                || !SchemaGeneratedValueComparison.same(current.generation(), target.generation(), tables.generatedValueStyle())) {
            return false;
        }
        return SchemaDialectTypeSupport.safeWideningDataType(tables.metadataDataType(current),
                                                              tables.dataType(target.column()));
    }

    static List<String> currentPrimaryKeys(RelationalTableDefinition current) {
        return current.primaryKey().map(PrimaryKeyDefinition::columns).orElseGet(List::of);
    }

    static List<String> targetPrimaryKeys(DynamicForm target) {
        return target.fields().stream()
                     .filter(DynamicField::primaryKey)
                     .map(DynamicField::name)
                     .toList();
    }

    static void validateColumnRenames(RelationalTableDefinition current,
                                      DynamicForm target,
                                      Map<String, String> renames) {
        renames.forEach((oldName, newName) -> {
            if (exactColumn(current, oldName) == null) {
                throw new IllegalArgumentException("rename source column does not exist");
            }
            if (exactField(target, newName) == null) {
                throw new IllegalArgumentException("rename target field does not exist");
            }
            if (exactColumn(current, newName) != null) {
                throw new IllegalArgumentException("rename target column already exists");
            }
            if (exactField(target, oldName) != null) {
                throw new IllegalArgumentException("rename source is still present in target form");
            }
        });
    }

    static ColumnDefinition exactColumn(RelationalTableDefinition table, String name) {
        String exactName = Names.requireText(name, "field name");
        return table.columns().stream()
                   .filter(column -> column.name().equals(exactName))
                   .findFirst()
                   .orElse(null);
    }

    static DynamicField exactField(DynamicForm form, String name) {
        String exactName = Names.requireText(name, "field name");
        return form.fields().stream()
                   .filter(field -> field.name().equals(exactName))
                   .findFirst()
                   .orElse(null);
    }

    static IndexDefinition exactIndex(RelationalTableDefinition table, String name) {
        String exactName = Names.requireText(name, "field name");
        return table.indexes().stream()
                    .filter(index -> index.name().equals(exactName))
                    .findFirst()
                    .orElse(null);
    }

    static ForeignKeyDefinition exactForeignKey(RelationalTableDefinition table, String name) {
        String exactName = Names.requireText(name, "field name");
        return table.foreignKeys().stream()
                    .filter(foreignKey -> foreignKey.name().equals(exactName))
                    .findFirst()
                    .orElse(null);
    }

    static Set<String> ambiguousFoldedNames(List<String> names) {
        Set<String> seen = new HashSet<>();
        Set<String> ambiguous = new HashSet<>();
        for (String name : names) {
            String folded = FieldIdentity.of(name).key();
            if (!seen.add(folded)) {
                ambiguous.add(folded);
            }
        }
        return Set.copyOf(ambiguous);
    }

    static List<String> renamedNames(List<String> names, Map<String, String> renames) {
        return names.stream()
                    .map(name -> {
                        String renamed = renameTargetForSource(renames, name);
                        return renamed == null ? name : renamed;
                    })
                    .toList();
    }

    static String renameSourceForTarget(Map<String, String> renames, String targetName) {
        String exact = renames.entrySet().stream()
                              .filter(entry -> entry.getValue().equals(targetName))
                              .map(Map.Entry::getKey)
                              .findFirst()
                              .orElse(null);
        if (exact != null) {
            return exact;
        }
        return uniqueFoldedRename(renames, targetName, false);
    }

    static String renameTargetForSource(Map<String, String> renames, String sourceName) {
        String exact = renames.entrySet().stream()
                              .filter(entry -> entry.getKey().equals(sourceName))
                              .map(Map.Entry::getValue)
                              .findFirst()
                              .orElse(null);
        if (exact != null) {
            return exact;
        }
        return uniqueFoldedRename(renames, sourceName, true);
    }

    private static String uniqueFoldedRename(Map<String, String> renames,
                                             String name,
                                             boolean source) {
        String folded = FieldIdentity.of(name).key();
        String match = null;
        for (Map.Entry<String, String> entry : renames.entrySet()) {
            String candidate = source ? entry.getKey() : entry.getValue();
            if (FieldIdentity.of(candidate).key().equals(folded)) {
                if (match != null) {
                    return null;
                }
                match = source ? entry.getValue() : entry.getKey();
            }
        }
        return match;
    }

    static boolean sameNames(List<String> left, List<String> right) {
        return left.equals(right);
    }

    static boolean logicalTemporalTypeChanged(DatabaseType currentType, DatabaseType targetType) {
        int currentKind = logicalTimeKind(currentType);
        int targetKind = logicalTimeKind(targetType);
        return currentKind != targetKind && (currentKind != 0 || targetKind != 0);
    }

    private static int logicalTimeKind(DatabaseType dataType) {
        if (dataType.logicalType() == LogicalType.OFFSET_TIME) {
            return 2;
        }
        return dataType.logicalType() == LogicalType.TIME ? 1 : 0;
    }

    /** nullable 单独变化时才允许生成 SQL，避免只改一半后把类型差异漏在数据库里。 */
    static boolean sameStorageShape(ColumnDefinition column,
                                    DynamicField target,
                                    RelationalSchemaSqlRenderer tables) {
        return tables.sameDataType(tables.metadataDataType(column), tables.dataType(target.column()))
                && SchemaGeneratedValueComparison.same(column.generation(), target.generation(), tables.generatedValueStyle());
    }

}
