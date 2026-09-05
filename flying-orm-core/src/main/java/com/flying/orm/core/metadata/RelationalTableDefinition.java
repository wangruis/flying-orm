package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 一张表的规范关系定义。
 *
 * <p>旧的 {@link TableMetadata} 仍是普通 CRUD 使用的轻量视图；只有显式进入 Schema、迁移或
 * DDL 路径时才需要创建本对象。构造阶段会复制全部集合并一次性核对列引用，发布后可以安全地
 * 在线程间共享，也不会让可选关系能力给旧热路径增加对象或查找成本。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class RelationalTableDefinition {

    private final RelationIdentity identity;
    private final String comment;
    private final List<ColumnDefinition> columns;
    private final MetadataNameIndex<ColumnDefinition> columnsByName;
    private final PrimaryKeyDefinition primaryKey;
    private final List<UniqueConstraintDefinition> uniqueConstraints;
    private final List<IndexDefinition> indexes;
    private final List<ForeignKeyDefinition> foreignKeys;
    private final List<CheckConstraintDefinition> checks;
    private final TablePartitionDefinition partition;

    private RelationalTableDefinition(Builder builder) {
        identity = builder.identity;
        comment = normalizeComment(builder.comment);
        columns = List.copyOf(builder.columns);
        columnsByName = MetadataNameIndex.ofOwned(columns,
                                                  ColumnDefinition::name,
                                                  column -> Names.key(column.name(), "column name"),
                                                  "column");
        primaryKey = builder.primaryKey;
        uniqueConstraints = List.copyOf(builder.uniqueConstraints);
        indexes = List.copyOf(builder.indexes);
        foreignKeys = List.copyOf(builder.foreignKeys);
        checks = List.copyOf(builder.checks);
        partition = builder.partition;

        validateConstraintNames();
        validateColumnReferences();
    }

    /** 创建只在当前配置线程使用的构建器。 */
    public static Builder builder(RelationIdentity identity) {
        return new Builder(identity);
    }

    /** @return 保留 catalog、schema 和 table 分段的关系身份 */
    public RelationIdentity identity() {
        return identity;
    }

    /** @return 表注释；未声明时返回 {@code null} */
    public String comment() {
        return comment;
    }

    /** @return 按声明顺序发布的不可变列快照 */
    public List<ColumnDefinition> columns() {
        return columns;
    }

    /** 精确名称优先；只有大小写折叠后仍唯一时才提供兼容查找。 */
    public Optional<ColumnDefinition> findColumn(String name) {
        return columnsByName.find(name, "column name");
    }

    /** @return 指定列；不存在时给出不回显输入值的确定性错误 */
    public ColumnDefinition column(String name) {
        return findColumn(name).orElseThrow(() -> new IllegalArgumentException(
                "column does not exist in relational table"));
    }

    /** @return 可选的命名主键定义 */
    public Optional<PrimaryKeyDefinition> primaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    /** @return 命名唯一约束的不可变快照 */
    public List<UniqueConstraintDefinition> uniqueConstraints() {
        return uniqueConstraints;
    }

    /** 兼容更短的领域用语。 */
    public List<UniqueConstraintDefinition> uniques() {
        return uniqueConstraints;
    }

    /** @return 命名索引的不可变快照 */
    public List<IndexDefinition> indexes() {
        return indexes;
    }

    /** @return 命名外键的不可变快照 */
    public List<ForeignKeyDefinition> foreignKeys() {
        return foreignKeys;
    }

    /** @return 命名 CHECK 约束的不可变快照 */
    public List<CheckConstraintDefinition> checks() {
        return checks;
    }

    /** @return 可选的受控分区父表定义 */
    public Optional<TablePartitionDefinition> partition() {
        return Optional.ofNullable(partition);
    }

    private void validateConstraintNames() {
        Set<String> constraintNames = new HashSet<>();
        Set<String> indexNames = new HashSet<>();
        if (primaryKey != null) {
            addObjectName(constraintNames, primaryKey.name());
            addObjectName(indexNames, primaryKey.name());
        }
        for (UniqueConstraintDefinition unique : uniqueConstraints) {
            addObjectName(constraintNames, unique.name());
            addObjectName(indexNames, unique.name());
        }
        for (CheckConstraintDefinition check : checks) {
            addObjectName(constraintNames, check.name());
        }
        // PK/UK 的物理实现可能就是同名索引，不能再声明第二个对象；CHECK/FK 与索引则彼此独立。
        for (IndexDefinition index : indexes) {
            addObjectName(indexNames, index.name());
        }
        for (ForeignKeyDefinition foreignKey : foreignKeys) {
            addObjectName(constraintNames, foreignKey.name());
        }
    }

    private static String normalizeComment(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static void addObjectName(Set<String> names, String name) {
        String safeName = Names.requireText(name, "relational object name");
        if (!names.add(safeName)) {
            throw new IllegalArgumentException("duplicate relational object name");
        }
    }

    private void validateColumnReferences() {
        if (primaryKey != null) {
            requireColumns(primaryKey.columns(), "primary key");
            for (String columnName : primaryKey.columns()) {
                if (column(columnName).nullable()) {
                    throw new IllegalArgumentException("primary key column must not be nullable");
                }
            }
        }
        for (UniqueConstraintDefinition unique : uniqueConstraints) {
            requireColumns(unique.columns(), "unique constraint");
        }
        for (IndexDefinition index : indexes) {
            requireColumns(index.keys().stream().map(IndexKeyPart::column).toList(), "index");
        }
        for (ForeignKeyDefinition foreignKey : foreignKeys) {
            requireColumns(foreignKey.columns(), "foreign key");
        }
        for (CheckConstraintDefinition check : checks) {
            requirePredicateColumns(check.predicate());
        }
        if (partition != null) {
            ColumnDefinition key = columnsByName.findExact(partition.column(), "column name")
                    .orElseThrow(() -> new IllegalArgumentException(
                            "table partition references an unknown column"));
            if (key.databaseType().isArray() || switch (key.databaseType().logicalType()) {
                case DATE, TIMESTAMP, OFFSET_TIMESTAMP -> false;
                default -> true;
            }) {
                throw new IllegalArgumentException(
                        "table partition column must be a scalar date or timestamp");
            }
        }
    }

    private void requireColumns(List<String> names, String owner) {
        for (String name : names) {
            requireColumn(name, owner);
        }
    }

    private void requirePredicateColumns(CheckPredicate predicate) {
        switch (predicate) {
            case CheckPredicate.Comparison comparison -> requireColumn(comparison.column(), "check constraint");
            case CheckPredicate.Range range -> requireColumn(range.column(), "check constraint");
            case CheckPredicate.In in -> requireColumn(in.column(), "check constraint");
            case CheckPredicate.NullCheck nullCheck -> requireColumn(nullCheck.column(), "check constraint");
            case CheckPredicate.Logical logical -> logical.predicates().forEach(this::requirePredicateColumns);
            case CheckPredicate.Negation negation -> requirePredicateColumns(negation.predicate());
        }
    }

    private void requireColumn(String name, String owner) {
        if (columnsByName.findExact(name, "column name").isEmpty()) {
            throw new IllegalArgumentException(owner + " references an unknown column");
        }
    }

    /**
     * 规范表构建器。所有可变集合只存在于构建阶段，{@link #build()} 会发布独立快照。
     */
    public static final class Builder {

        private final RelationIdentity identity;
        private String comment;
        private final List<ColumnDefinition> columns = new ArrayList<>();
        private PrimaryKeyDefinition primaryKey;
        private final List<UniqueConstraintDefinition> uniqueConstraints = new ArrayList<>();
        private final List<IndexDefinition> indexes = new ArrayList<>();
        private final List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();
        private final List<CheckConstraintDefinition> checks = new ArrayList<>();
        private TablePartitionDefinition partition;

        private Builder(RelationIdentity identity) {
            this.identity = Objects.requireNonNull(identity, "relation identity must not be null");
        }

        /** 声明可选的表注释；空白值按未声明处理。 */
        public Builder comment(String value) {
            comment = value;
            return this;
        }

        public Builder addColumn(ColumnDefinition column) {
            columns.add(Objects.requireNonNull(column, "column definition must not be null"));
            return this;
        }

        public Builder primaryKey(PrimaryKeyDefinition definition) {
            if (primaryKey != null) {
                throw new IllegalStateException("primary key is already defined");
            }
            primaryKey = Objects.requireNonNull(definition, "primary key definition must not be null");
            return this;
        }

        public Builder addUnique(UniqueConstraintDefinition definition) {
            uniqueConstraints.add(Objects.requireNonNull(
                    definition, "unique constraint definition must not be null"));
            return this;
        }

        public Builder unique(UniqueConstraintDefinition definition) {
            return addUnique(definition);
        }

        public Builder addIndex(IndexDefinition definition) {
            indexes.add(Objects.requireNonNull(definition, "index definition must not be null"));
            return this;
        }

        public Builder index(IndexDefinition definition) {
            return addIndex(definition);
        }

        public Builder addForeignKey(ForeignKeyDefinition definition) {
            foreignKeys.add(Objects.requireNonNull(definition, "foreign key definition must not be null"));
            return this;
        }

        public Builder foreignKey(ForeignKeyDefinition definition) {
            return addForeignKey(definition);
        }

        public Builder addCheck(CheckConstraintDefinition definition) {
            checks.add(Objects.requireNonNull(definition, "check constraint definition must not be null"));
            return this;
        }

        public Builder check(CheckConstraintDefinition definition) {
            return addCheck(definition);
        }

        /** 声明受控分区父表；一张表只能有一项分区定义。 */
        public Builder partition(TablePartitionDefinition definition) {
            if (partition != null) {
                throw new IllegalStateException("table partition is already defined");
            }
            partition = Objects.requireNonNull(
                    definition, "table partition definition must not be null");
            return this;
        }

        public RelationalTableDefinition build() {
            return new RelationalTableDefinition(this);
        }
    }
}
