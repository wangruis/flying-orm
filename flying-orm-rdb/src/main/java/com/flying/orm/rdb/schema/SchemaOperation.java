package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;

import java.util.Objects;

/**
 * desired 与 actual 之间的一项结构化差异。
 *
 * <p>operation 只携带规范关系模型，不包含 SQL、连接或驱动对象。ADD 的 actual、DROP 的 desired
 * 为 {@code null}；CHANGE 同时保留两侧事实。这样风险分类和后续方言规划可以直接消费同一份
 * 不可变事实，不需要重新读取数据库或从说明文字里反向解析结构。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaOperation {

    /**
     * 固定枚举顺序也是单表 diff 的稳定阶段顺序：先创建和增加，再修改，最后移除依赖对象。
     */
    public enum Kind {
        CREATE_TABLE,
        ADD_COLUMN,
        CHANGE_COLUMN,
        ADD_PRIMARY_KEY,
        CHANGE_PRIMARY_KEY,
        ADD_UNIQUE,
        CHANGE_UNIQUE,
        ADD_INDEX,
        CHANGE_INDEX,
        ADD_CHECK,
        CHANGE_CHECK,
        ADD_FOREIGN_KEY,
        CHANGE_FOREIGN_KEY,
        DROP_FOREIGN_KEY,
        DROP_CHECK,
        DROP_INDEX,
        DROP_UNIQUE,
        DROP_PRIMARY_KEY,
        DROP_COLUMN,
        DROP_TABLE,
        VERIFY_MANUALLY
    }

    /** 该差异在本次已知事实下的兼容边界，不等同于上线风险级别。 */
    public enum Compatibility {
        /** 从 actual 向 desired 前进且能证明不会丢失或收窄已有数据。 */
        SAFE_INCREMENTAL,
        /** actual 中可以暂时保留、又不会破坏当前读写的受控 extra。 */
        COMPATIBLE_EXTRA,
        /** 需要更多事实或人工审核；任何兼容模式都不能自动把它判为安全。 */
        REQUIRES_REVIEW
    }

    private final Kind kind;
    private final RelationIdentity relation;
    private final String objectName;
    private final Object actual;
    private final Object desired;
    private final Compatibility compatibility;

    private SchemaOperation(Kind kind,
                            RelationIdentity relation,
                            String objectName,
                            Object actual,
                            Object desired,
                            Compatibility compatibility) {
        this.kind = Objects.requireNonNull(kind, "schema operation kind must not be null");
        this.relation = Objects.requireNonNull(relation, "schema operation relation must not be null");
        this.objectName = requireName(objectName);
        this.actual = actual;
        this.desired = desired;
        this.compatibility = Objects.requireNonNull(
                compatibility, "schema operation compatibility must not be null");
        validatePayload();
        validateCompatibility();
    }

    /**
     * 创建一项已经由纯 diff 分类的结构操作。
     *
     * @param actual actual 侧规范定义；ADD 时为 null
     * @param desired desired 侧规范定义；DROP 时为 null
     */
    public static SchemaOperation of(Kind kind,
                                     RelationIdentity relation,
                                     String objectName,
                                     Object actual,
                                     Object desired,
                                     Compatibility compatibility) {
        return new SchemaOperation(kind, relation, objectName, actual, desired, compatibility);
    }

    public Kind kind() {
        return kind;
    }

    public RelationIdentity relation() {
        return relation;
    }

    public String objectName() {
        return objectName;
    }

    /** @return actual 侧规范定义；该侧不存在时返回 null */
    public Object actual() {
        return actual;
    }

    /** @return desired 侧规范定义；该侧不存在时返回 null */
    public Object desired() {
        return desired;
    }

    public Compatibility compatibility() {
        return compatibility;
    }

    private void validatePayload() {
        switch (kind) {
            case CREATE_TABLE -> requireAdd(RelationalTableDefinition.class);
            case DROP_TABLE -> requireDrop(RelationalTableDefinition.class);
            case ADD_COLUMN -> requireAdd(ColumnDefinition.class);
            case CHANGE_COLUMN -> requireChange(ColumnDefinition.class);
            case DROP_COLUMN -> requireDrop(ColumnDefinition.class);
            case ADD_PRIMARY_KEY -> requireAdd(PrimaryKeyDefinition.class);
            case CHANGE_PRIMARY_KEY -> requireChange(PrimaryKeyDefinition.class);
            case DROP_PRIMARY_KEY -> requireDrop(PrimaryKeyDefinition.class);
            case ADD_UNIQUE -> requireAdd(UniqueConstraintDefinition.class);
            case CHANGE_UNIQUE -> requireChange(UniqueConstraintDefinition.class);
            case DROP_UNIQUE -> requireDrop(UniqueConstraintDefinition.class);
            case ADD_INDEX -> requireAdd(IndexDefinition.class);
            case CHANGE_INDEX -> requireChange(IndexDefinition.class);
            case DROP_INDEX -> requireDrop(IndexDefinition.class);
            case ADD_FOREIGN_KEY -> requireAdd(ForeignKeyDefinition.class);
            case CHANGE_FOREIGN_KEY -> requireChange(ForeignKeyDefinition.class);
            case DROP_FOREIGN_KEY -> requireDrop(ForeignKeyDefinition.class);
            case ADD_CHECK -> requireAdd(CheckConstraintDefinition.class);
            case CHANGE_CHECK -> requireChange(CheckConstraintDefinition.class);
            case DROP_CHECK -> requireDrop(CheckConstraintDefinition.class);
            case VERIFY_MANUALLY -> {
                // 无法观察到的事实可能没有可发布的 definition；名称负责指出缺失的事实域。
            }
        }
    }

    private void validateCompatibility() {
        if (compatibility == Compatibility.SAFE_INCREMENTAL
                && kind != Kind.CREATE_TABLE
                && kind != Kind.ADD_COLUMN) {
            throw new IllegalArgumentException("only table creation or column addition can be proven safe here");
        }
        if (compatibility == Compatibility.COMPATIBLE_EXTRA
                && kind != Kind.DROP_COLUMN
                && kind != Kind.DROP_INDEX) {
            throw new IllegalArgumentException("only controlled extra columns or indexes are rolling compatible");
        }
        if (kind == Kind.VERIFY_MANUALLY && compatibility != Compatibility.REQUIRES_REVIEW) {
            throw new IllegalArgumentException("unverified schema facts always require review");
        }
    }

    private void requireAdd(Class<?> type) {
        if (actual != null || !type.isInstance(desired)) {
            throw invalidPayload();
        }
    }

    private void requireDrop(Class<?> type) {
        if (!type.isInstance(actual) || desired != null) {
            throw invalidPayload();
        }
    }

    private void requireChange(Class<?> type) {
        if (!type.isInstance(actual) || !type.isInstance(desired)) {
            throw invalidPayload();
        }
    }

    private IllegalArgumentException invalidPayload() {
        return new IllegalArgumentException("schema operation payload does not match " + kind);
    }

    private static String requireName(String value) {
        String name = Objects.requireNonNull(value, "schema operation object name must not be null").trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("schema operation object name must not be blank");
        }
        return name;
    }

    @Override
    public String toString() {
        return kind + "[relation=" + relation + ", object=" + objectName
                + ", compatibility=" + compatibility + ']';
    }
}
