package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataAdapter;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 一次数据库结构读取的不可变事实快照。
 *
 * <p>数据库字典没有返回某项信息时必须记为 {@link State#UNKNOWN}，不能拿空集合冒充
 * “数据库里确认没有”。这三个状态让后续 diff 能区分不存在、已读取和无法确认。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaSnapshot {

    public enum State {
        PRESENT,
        ABSENT,
        UNKNOWN
    }

    /** 旧数据库字典模型能看到对象，却无法无损表达的细粒度事实。 */
    public enum UnknownAttribute {
        PRIMARY_KEY_NAME,
        COLUMN_DEFAULT,
        COLUMN_GENERATION,
        COLUMN_CHARSET,
        COLUMN_COLLATION,
        INDEX_DIRECTION,
        FOREIGN_KEY_ACTION,
        FOREIGN_KEY_REFERENCE_SCOPE
    }

    /** 一个可明确表达“有、没有、不知道”的只读观察值。 */
    public record Observed<T>(State state, T value) {

        public Observed {
            Objects.requireNonNull(state, "schema observation state must not be null");
            if (state == State.PRESENT) {
                Objects.requireNonNull(value, "present schema observation must have a value");
            } else if (value != null) {
                throw new IllegalArgumentException("absent or unknown schema observation must not have a value");
            }
        }

        public static <T> Observed<T> present(T value) {
            return new Observed<>(State.PRESENT, value);
        }

        public static <T> Observed<T> absent() {
            return new Observed<>(State.ABSENT, null);
        }

        public static <T> Observed<T> unknown() {
            return new Observed<>(State.UNKNOWN, null);
        }

        public Optional<T> optionalValue() {
            return Optional.ofNullable(value);
        }
    }

    private final RelationIdentity identity;
    private final State tableState;
    private final Observed<String> tableComment;
    private final Observed<List<ColumnDefinition>> columns;
    private final Observed<PrimaryKeyDefinition> primaryKey;
    private final Observed<List<UniqueConstraintDefinition>> uniqueConstraints;
    private final Observed<List<IndexDefinition>> indexes;
    private final Observed<List<ForeignKeyDefinition>> foreignKeys;
    private final Observed<List<CheckConstraintDefinition>> checks;
    private final Observed<TablePartitionDefinition> partition;
    private final Set<UnknownAttribute> unknownAttributes;
    private final RelationalTableDefinition knownDefinition;

    private SchemaSnapshot(Builder builder) {
        identity = builder.identity;
        tableState = builder.tableState;
        tableComment = builder.tableComment;
        columns = copyList(builder.columns);
        primaryKey = builder.primaryKey;
        uniqueConstraints = copyList(builder.uniqueConstraints);
        indexes = copyList(builder.indexes);
        foreignKeys = copyList(builder.foreignKeys);
        checks = copyList(builder.checks);
        partition = builder.partition;
        unknownAttributes = Set.copyOf(builder.unknownAttributes);

        if (tableState != State.PRESENT) {
            requireNoTableDetails();
            knownDefinition = null;
            return;
        }
        if (columns.state() != State.PRESENT) {
            throw new IllegalArgumentException("present table snapshot must contain observed columns");
        }
        knownDefinition = buildKnownDefinition();
    }

    public static Builder builder(RelationIdentity identity) {
        return new Builder(identity);
    }

    /** 把完整规范表投影成所有结构事实均已知的快照。 */
    public static SchemaSnapshot present(RelationalTableDefinition table) {
        RelationalTableDefinition source = Objects.requireNonNull(
                table, "relational table definition must not be null");
        Builder builder = builder(source.identity())
                .tablePresent()
                .columns(source.columns())
                .uniqueConstraints(source.uniqueConstraints())
                .indexes(source.indexes())
                .foreignKeys(source.foreignKeys())
                .checks(source.checks());
        source.partition().ifPresentOrElse(builder::partition, builder::partitionAbsent);
        if (source.comment() == null) {
            builder.tableCommentAbsent();
        } else {
            builder.tableComment(source.comment());
        }
        source.primaryKey().ifPresentOrElse(builder::primaryKey, builder::primaryKeyAbsent);
        return builder.build();
    }

    public static SchemaSnapshot absent(RelationIdentity identity) {
        return builder(identity).tableAbsent().build();
    }

    public static SchemaSnapshot unknown(RelationIdentity identity) {
        return builder(identity).tableUnknown().build();
    }

    /**
     * 把旧字段/索引/外键读取结果升级成诚实的部分快照。旧模型没有的信息继续标 UNKNOWN，
     * 因此该方法不会把合成主键名、默认值或外键动作冒充成数据库事实。
     */
    public static SchemaSnapshot fromLegacy(RelationIdentity identity, TableMetadata metadata) {
        return fromLegacy(identity, metadata, false, false);
    }

    /** 与 {@link #fromLegacy(RelationIdentity, TableMetadata)} 相同，并明确哪些附属字典查询真正执行过。 */
    public static SchemaSnapshot fromLegacy(RelationIdentity identity,
                                            TableMetadata metadata,
                                            boolean indexesObserved,
                                            boolean foreignKeysObserved) {
        RelationalTableDefinition known = RelationalMetadataAdapter.from(Objects.requireNonNull(
                metadata, "table metadata must not be null"));
        Builder builder = builder(identity)
                .tablePresent()
                .columns(known.columns())
                .unknown(UnknownAttribute.PRIMARY_KEY_NAME,
                         UnknownAttribute.COLUMN_DEFAULT,
                         UnknownAttribute.COLUMN_GENERATION,
                         UnknownAttribute.COLUMN_CHARSET,
                         UnknownAttribute.COLUMN_COLLATION,
                         UnknownAttribute.INDEX_DIRECTION,
                         UnknownAttribute.FOREIGN_KEY_ACTION,
                         UnknownAttribute.FOREIGN_KEY_REFERENCE_SCOPE);
        if (indexesObserved) {
            builder.indexes(known.indexes());
        }
        if (foreignKeysObserved) {
            builder.foreignKeys(known.foreignKeys());
        }
        known.primaryKey().ifPresentOrElse(builder::primaryKey, builder::primaryKeyAbsent);
        // 旧模型无法区分唯一索引和唯一约束，也没有 CHECK，因此这两项保留默认 UNKNOWN。
        return builder.build();
    }

    public RelationIdentity identity() {
        return identity;
    }
    public State tableState() {
        return tableState;
    }
    /** 表注释也是物理结构事实；UNKNOWN 不能当作“数据库没有注释”。 */
    public Observed<String> tableComment() {
        return tableComment;
    }
    public Observed<List<ColumnDefinition>> columns() {
        return columns;
    }
    public Observed<PrimaryKeyDefinition> primaryKey() {
        return primaryKey;
    }
    public Observed<List<UniqueConstraintDefinition>> uniqueConstraints() {
        return uniqueConstraints;
    }
    public Observed<List<IndexDefinition>> indexes() {
        return indexes;
    }
    public Observed<List<ForeignKeyDefinition>> foreignKeys() {
        return foreignKeys;
    }
    public Observed<List<CheckConstraintDefinition>> checks() {
        return checks;
    }
    /** 分区定义与表是否存在一样属于数据库结构事实，不能把 UNKNOWN 当成未分区。 */
    public Observed<TablePartitionDefinition> partition() {
        return partition;
    }
    public Set<UnknownAttribute> unknownAttributes() {
        return unknownAttributes;
    }
    /** 只有所有结构项都被数据库明确观察到时，才返回可直接参与精确 diff 的完整表。 */
    public Optional<RelationalTableDefinition> completeTable() {
        if (tableState != State.PRESENT
                || columns.state() != State.PRESENT
                || tableComment.state() == State.UNKNOWN
                || primaryKey.state() == State.UNKNOWN
                || uniqueConstraints.state() != State.PRESENT
                || indexes.state() != State.PRESENT
                || foreignKeys.state() != State.PRESENT
                || checks.state() != State.PRESENT
                || partition.state() == State.UNKNOWN
                || !unknownAttributes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(knownDefinition);
    }

    RelationalTableDefinition knownDefinition() {
        if (knownDefinition == null) {
            throw new IllegalStateException("schema snapshot does not contain a present table");
        }
        return knownDefinition;
    }

    private RelationalTableDefinition buildKnownDefinition() {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(identity);
        if (tableComment.state() == State.PRESENT) {
            table.comment(tableComment.value());
        }
        columns.value().forEach(table::addColumn);
        if (primaryKey.state() == State.PRESENT) {
            table.primaryKey(primaryKey.value());
        }
        addPresent(uniqueConstraints, table::addUnique);
        addPresent(indexes, table::addIndex);
        addPresent(foreignKeys, table::addForeignKey);
        addPresent(checks, table::addCheck);
        if (partition.state() == State.PRESENT) {
            table.partition(partition.value());
        }
        return table.build();
    }

    private void requireNoTableDetails() {
        if (columns.state() != State.UNKNOWN
                || tableComment.state() != State.UNKNOWN
                || primaryKey.state() != State.UNKNOWN
                || uniqueConstraints.state() != State.UNKNOWN
                || indexes.state() != State.UNKNOWN
                || foreignKeys.state() != State.UNKNOWN
                || checks.state() != State.UNKNOWN
                || partition.state() != State.UNKNOWN) {
            throw new IllegalArgumentException("absent or unknown table snapshot must not contain table details");
        }
    }

    private static <T> void addPresent(Observed<List<T>> observed,
                                       java.util.function.Consumer<T> consumer) {
        if (observed.state() == State.PRESENT) {
            observed.value().forEach(consumer);
        }
    }

    private static <T> Observed<List<T>> copyList(Observed<List<T>> observed) {
        Objects.requireNonNull(observed, "schema observation must not be null");
        return observed.state() == State.PRESENT
                ? Observed.present(List.copyOf(observed.value()))
                : new Observed<>(observed.state(), null);
    }

    /** 构建数据库实际观察结果；未显式设置的结构项默认 UNKNOWN。 */
    public static final class Builder {

        private final RelationIdentity identity;
        private State tableState = State.UNKNOWN;
        private Observed<String> tableComment = Observed.unknown();
        private Observed<List<ColumnDefinition>> columns = Observed.unknown();
        private Observed<PrimaryKeyDefinition> primaryKey = Observed.unknown();
        private Observed<List<UniqueConstraintDefinition>> uniqueConstraints = Observed.unknown();
        private Observed<List<IndexDefinition>> indexes = Observed.unknown();
        private Observed<List<ForeignKeyDefinition>> foreignKeys = Observed.unknown();
        private Observed<List<CheckConstraintDefinition>> checks = Observed.unknown();
        private Observed<TablePartitionDefinition> partition = Observed.unknown();
        private final EnumSet<UnknownAttribute> unknownAttributes = EnumSet.noneOf(UnknownAttribute.class);

        private Builder(RelationIdentity identity) {
            this.identity = Objects.requireNonNull(identity, "relation identity must not be null");
        }

        public Builder tablePresent() {
            tableState = State.PRESENT;
            return this;
        }

        public Builder tableAbsent() {
            tableState = State.ABSENT;
            return this;
        }

        public Builder tableUnknown() {
            tableState = State.UNKNOWN;
            return this;
        }

        public Builder columns(List<ColumnDefinition> value) {
            columns = Observed.present(List.copyOf(Objects.requireNonNull(value, "columns must not be null")));
            return this;
        }

        public Builder tableComment(String value) {
            tableComment = Observed.present(Objects.requireNonNull(
                    value, "table comment must not be null"));
            return this;
        }

        public Builder tableCommentAbsent() {
            tableComment = Observed.absent();
            return this;
        }

        public Builder primaryKey(PrimaryKeyDefinition value) {
            primaryKey = Observed.present(Objects.requireNonNull(value, "primary key must not be null"));
            return this;
        }

        public Builder primaryKeyAbsent() {
            primaryKey = Observed.absent();
            return this;
        }

        public Builder uniqueConstraints(List<UniqueConstraintDefinition> value) {
            uniqueConstraints = observedList(value, "unique constraints must not be null");
            return this;
        }

        public Builder indexes(List<IndexDefinition> value) {
            indexes = observedList(value, "indexes must not be null");
            return this;
        }

        public Builder foreignKeys(List<ForeignKeyDefinition> value) {
            foreignKeys = observedList(value, "foreign keys must not be null");
            return this;
        }

        public Builder checks(List<CheckConstraintDefinition> value) {
            checks = observedList(value, "check constraints must not be null");
            return this;
        }

        public Builder partition(TablePartitionDefinition value) {
            partition = Observed.present(Objects.requireNonNull(
                    value, "table partition must not be null"));
            return this;
        }

        public Builder partitionAbsent() {
            partition = Observed.absent();
            return this;
        }

        public Builder unknown(UnknownAttribute... attributes) {
            for (UnknownAttribute attribute : Objects.requireNonNull(
                    attributes, "unknown schema attributes must not be null")) {
                unknownAttributes.add(Objects.requireNonNull(
                        attribute, "unknown schema attribute must not be null"));
            }
            return this;
        }

        public SchemaSnapshot build() {
            return new SchemaSnapshot(this);
        }

        private static <T> Observed<List<T>> observedList(List<T> value, String message) {
            return Observed.present(List.copyOf(Objects.requireNonNull(value, message)));
        }
    }
}
