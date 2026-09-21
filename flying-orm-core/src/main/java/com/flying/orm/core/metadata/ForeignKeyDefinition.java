package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * 命名外键定义。
 *
 * <p>本地列与引用列按位置一一对应。对象只保存结构化关系和受控动作，不保存 SQL 片段。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class ForeignKeyDefinition {

    private final String name;
    private final List<String> columns;
    private final RelationIdentity reference;
    private final List<String> referenceColumns;
    private final ReferentialAction onDelete;
    private final ReferentialAction onUpdate;

    private ForeignKeyDefinition(Builder builder) {
        name = builder.name;
        columns = List.copyOf(builder.columns);
        reference = Objects.requireNonNull(builder.reference, "foreign key reference must not be null");
        referenceColumns = List.copyOf(builder.referenceColumns);
        onDelete = builder.onDelete;
        onUpdate = builder.onUpdate;
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("foreign key columns must not be empty");
        }
        if (referenceColumns.isEmpty()) {
            throw new IllegalArgumentException("foreign key reference columns must not be empty");
        }
        if (columns.size() != referenceColumns.size()) {
            throw new IllegalArgumentException("foreign key column count must match reference column count");
        }
        if (new HashSet<>(columns).size() != columns.size()) {
            throw new IllegalArgumentException("foreign key columns must not contain duplicates");
        }
        if (new HashSet<>(referenceColumns).size() != referenceColumns.size()) {
            throw new IllegalArgumentException("foreign key reference columns must not contain duplicates");
        }
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public String name() {
        return name;
    }

    /** 返回本地列，顺序与引用列一一对应。 */
    public List<String> columns() {
        return columns;
    }

    public RelationIdentity reference() {
        return reference;
    }

    /** {@link #reference()} 的可读别名，便于调用方表达目标表。 */
    public RelationIdentity referencedTable() {
        return reference;
    }

    /** 返回目标列，顺序与本地列一一对应。 */
    public List<String> referenceColumns() {
        return referenceColumns;
    }

    public ReferentialAction onDelete() {
        return onDelete;
    }

    public ReferentialAction onUpdate() {
        return onUpdate;
    }

    public static final class Builder {

        private final String name;
        private final List<String> columns = new ArrayList<>();
        private RelationIdentity reference;
        private final List<String> referenceColumns = new ArrayList<>();
        private ReferentialAction onDelete = ReferentialAction.NO_ACTION;
        private ReferentialAction onUpdate = ReferentialAction.NO_ACTION;

        private Builder(String name) {
            this.name = Names.requireText(name, "foreign key name");
        }

        public Builder addColumn(String column) {
            columns.add(Names.requireText(column, "foreign key column name"));
            return this;
        }

        public Builder reference(RelationIdentity reference) {
            this.reference = Objects.requireNonNull(reference, "foreign key reference must not be null");
            return this;
        }

        /** 兼容偏向属性命名的调用风格。 */
        public Builder referencedTable(RelationIdentity reference) {
            return reference(reference);
        }

        public Builder addReferenceColumn(String column) {
            referenceColumns.add(Names.requireText(column, "foreign key reference column name"));
            return this;
        }

        public Builder onDelete(ReferentialAction action) {
            onDelete = Objects.requireNonNull(action, "foreign key on-delete action must not be null");
            return this;
        }

        public Builder onUpdate(ReferentialAction action) {
            onUpdate = Objects.requireNonNull(action, "foreign key on-update action must not be null");
            return this;
        }

        public ForeignKeyDefinition build() {
            return new ForeignKeyDefinition(this);
        }
    }
}
