package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 同一 catalog/schema 范围内的规范表集合。
 *
 * <p>Schema 不猜测点号层级。每张表都必须显式带着与集合一致的 catalog 和 schema；未指定范围的
 * 构建器会采用第一张表的范围，随后仍按同一规则校验其余表。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class RelationalSchemaDefinition {

    private final String catalog;
    private final String schema;
    private final List<RelationalTableDefinition> tables;
    private final MetadataNameIndex<RelationalTableDefinition> tablesByName;

    private RelationalSchemaDefinition(Builder builder) {
        List<RelationalTableDefinition> snapshot = List.copyOf(builder.tables);
        RelationIdentity firstIdentity = snapshot.isEmpty() ? null : snapshot.getFirst().identity();
        catalog = builder.scopeSpecified
                ? builder.catalog
                : firstIdentity == null ? null : firstIdentity.catalog().orElse(null);
        schema = builder.scopeSpecified
                ? builder.schema
                : firstIdentity == null ? null : firstIdentity.schema().orElse(null);

        for (RelationalTableDefinition table : snapshot) {
            requireSameScope(table.identity());
        }
        tables = snapshot;
        tablesByName = MetadataNameIndex.ofOwned(tables,
                                                 table -> table.identity().table(),
                                                 table -> Names.key(table.identity().table(), "table name"),
                                                 "table");
        validateManagedForeignKeys();
    }

    /** 创建一个从首张表推导范围的构建器。 */
    public static Builder builder() {
        return new Builder(false, null, null);
    }

    /** 创建带明确 catalog/schema 范围的构建器；任一段都允许为空。 */
    public static Builder builder(String catalog, String schema) {
        return new Builder(true, normalizeOptional(catalog, "catalog"),
                           normalizeOptional(schema, "schema"));
    }

    /** 从调用方集合创建独立的不可变快照。 */
    public static RelationalSchemaDefinition of(List<RelationalTableDefinition> tables) {
        Builder builder = builder();
        Objects.requireNonNull(tables, "relational tables must not be null").forEach(builder::addTable);
        return builder.build();
    }

    public Optional<String> catalog() {
        return Optional.ofNullable(catalog);
    }

    public Optional<String> schema() {
        return Optional.ofNullable(schema);
    }

    public List<RelationalTableDefinition> tables() {
        return tables;
    }

    public Optional<RelationalTableDefinition> findTable(String tableName) {
        return tablesByName.find(tableName, "table name");
    }

    public RelationalTableDefinition table(String tableName) {
        return findTable(tableName).orElseThrow(() -> new IllegalArgumentException(
                "table does not exist in relational schema"));
    }

    private void requireSameScope(RelationIdentity identity) {
        if (!Objects.equals(catalog, identity.catalog().orElse(null))
                || !Objects.equals(schema, identity.schema().orElse(null))) {
            throw new IllegalArgumentException("all relational tables must use the schema scope");
        }
    }

    private void validateManagedForeignKeys() {
        Map<RelationIdentity, RelationalTableDefinition> managedTables = new HashMap<>();
        for (RelationalTableDefinition table : tables) {
            managedTables.put(table.identity(), table);
        }
        for (RelationalTableDefinition table : tables) {
            for (ForeignKeyDefinition foreignKey : table.foreignKeys()) {
                RelationalTableDefinition target = managedTables.get(foreignKey.reference());
                if (target != null) {
                    validateManagedForeignKey(target, foreignKey);
                }
            }
        }
    }

    private static void validateManagedForeignKey(
            RelationalTableDefinition target,
            ForeignKeyDefinition foreignKey
    ) {
        for (String column : foreignKey.referenceColumns()) {
            if (target.columns().stream().noneMatch(candidate -> candidate.name().equals(column))) {
                throw new IllegalArgumentException("foreign key references an unknown managed target column");
            }
        }
        if (!isCandidateKey(target, foreignKey.referenceColumns())) {
            throw new IllegalArgumentException("foreign key managed target columns must form a candidate key");
        }
    }

    private static boolean isCandidateKey(RelationalTableDefinition table, List<String> columns) {
        if (table.primaryKey().map(key -> key.columns().equals(columns)).orElse(false)) {
            return true;
        }
        for (UniqueConstraintDefinition unique : table.uniqueConstraints()) {
            if (unique.columns().equals(columns)) {
                return true;
            }
        }
        for (IndexDefinition index : table.indexes()) {
            if (matchesUniqueIndex(index, columns)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesUniqueIndex(IndexDefinition index, List<String> columns) {
        if (!index.unique() || index.keys().size() != columns.size()) {
            return false;
        }
        for (int indexPosition = 0; indexPosition < columns.size(); indexPosition++) {
            if (!index.keys().get(indexPosition).column().equals(columns.get(indexPosition))) {
                return false;
            }
        }
        return true;
    }

    private static String normalizeOptional(String value, String name) {
        return value == null ? null : Names.requireText(value, name);
    }

    /** Schema 构建器只收集表；所有一致性检查集中在 build 边界执行一次。 */
    public static final class Builder {

        private final boolean scopeSpecified;
        private final String catalog;
        private final String schema;
        private final List<RelationalTableDefinition> tables = new ArrayList<>();

        private Builder(boolean scopeSpecified, String catalog, String schema) {
            this.scopeSpecified = scopeSpecified;
            this.catalog = catalog;
            this.schema = schema;
        }

        public Builder addTable(RelationalTableDefinition table) {
            tables.add(Objects.requireNonNull(table, "relational table must not be null"));
            return this;
        }

        public RelationalSchemaDefinition build() {
            return new RelationalSchemaDefinition(this);
        }
    }
}
