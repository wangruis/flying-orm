package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * 把规范 desired 表与数据库 actual 快照比较成确定顺序的结构 operation。
 *
 * <p>本类是无状态纯函数：不读取连接，不调用 metadata reader，也不渲染 SQL。同一组 desired、
 * actual 与 capability 事实总会得到相同的 operation 顺序。无法从快照证明的属性会明确输出
 * {@link SchemaOperation.Kind#VERIFY_MANUALLY}，绝不拿空值猜成“不存在”。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaDiffer {

    private static final Comparator<SchemaOperation> OPERATION_ORDER =
            Comparator.comparing(SchemaOperation::kind)
                      .thenComparing(SchemaOperation::objectName);

    /** 无状态对象可以安全复用；主要为依赖注入式调用保留。 */
    public SchemaDiffer() {
    }

    /**
     * 生成一份纯结构兼容报告。
     *
     * @param desired ORM 希望管理的完整规范表
     * @param actual 数据库已经观察到的事实快照
     * @param capabilities 当前已解析且不可变的方言能力事实
     * @param mode 本次兼容边界
     */
    public static SchemaCompatibilityReport diff(RelationalTableDefinition desired,
                                                 SchemaSnapshot actual,
                                                 DialectCapabilities capabilities,
                                                 SchemaCompatibilityMode mode) {
        return diff(desired, actual, capabilities, mode, null);
    }

    static SchemaCompatibilityReport diff(RelationalTableDefinition desired,
                                          SchemaSnapshot actual,
                                          DialectCapabilities capabilities,
                                          SchemaCompatibilityMode mode,
                                          String dialectId) {
        return diff(desired, actual, capabilities, mode, dialectId, null);
    }

    static SchemaCompatibilityReport diff(RelationalTableDefinition desired,
                                          SchemaSnapshot actual,
                                          DialectCapabilities capabilities,
                                          SchemaCompatibilityMode mode,
                                          String dialectId,
                                          SchemaDialect schemaDialect) {
        RelationalTableDefinition target = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        SchemaSnapshot observed = Objects.requireNonNull(actual, "actual schema snapshot must not be null");
        DialectCapabilities dialectCapabilities = Objects.requireNonNull(
                capabilities, "dialect capabilities must not be null");
        SchemaCompatibilityMode compatibilityMode = Objects.requireNonNull(
                mode, "schema compatibility mode must not be null");
        requireSameRelation(target.identity(), observed.identity(), schemaDialect);
        boolean mysqlMetadata = "mysql".equals(dialectId)
                || dialectCapabilities.supports(DialectCapabilityId.MYSQL_RELATIONAL_METADATA);

        List<SchemaOperation> operations = new ArrayList<>();
        switch (observed.tableState()) {
            case ABSENT -> operations.add(createTable(target, dialectCapabilities));
            case UNKNOWN -> operations.add(manual(target, observed, "table-state"));
            case PRESENT -> diffPresent(
                    target, observed, operations, mysqlMetadata, schemaDialect);
        }
        operations.sort(OPERATION_ORDER);
        return SchemaCompatibilityReport.of(compatibilityMode, operations);
    }

    /** 实例式别名；行为与 {@link #diff(RelationalTableDefinition, SchemaSnapshot, DialectCapabilities, SchemaCompatibilityMode)} 相同。 */
    public SchemaCompatibilityReport compare(RelationalTableDefinition desired,
                                             SchemaSnapshot actual,
                                             DialectCapabilities capabilities,
                                             SchemaCompatibilityMode mode) {
        return diff(desired, actual, capabilities, mode);
    }

    private static void diffPresent(RelationalTableDefinition desired,
                                    SchemaSnapshot actual,
                                    List<SchemaOperation> operations,
                                    boolean mysql,
                                    SchemaDialect schemaDialect) {
        RelationIdentity relation = desired.identity();
        diffTableComment(desired, actual, operations);
        diffColumns(relation, desired.columns(), actual.columns().value(), operations, schemaDialect);
        diffColumnOrder(desired, actual, operations, schemaDialect);
        diffPrimaryKey(desired, actual, operations, mysql, schemaDialect);
        diffObservedList(relation,
                         desired.uniqueConstraints(),
                         actual.uniqueConstraints(),
                         UniqueConstraintDefinition::name,
                         (left, right) -> SchemaDefinitionEquality.sameUnique(left, right, schemaDialect),
                         SchemaOperation.Kind.ADD_UNIQUE,
                         SchemaOperation.Kind.CHANGE_UNIQUE,
                         SchemaOperation.Kind.DROP_UNIQUE,
                         "unique-constraints",
                         operations,
                         schemaDialect);
        diffObservedList(relation,
                         desired.indexes(),
                         indexesForComparison(desired, actual.indexes(), mysql),
                         IndexDefinition::name,
                         (left, right) -> SchemaDefinitionEquality.sameIndex(left, right, schemaDialect),
                         SchemaOperation.Kind.ADD_INDEX,
                         SchemaOperation.Kind.CHANGE_INDEX,
                         SchemaOperation.Kind.DROP_INDEX,
                         "indexes",
                         operations,
                         schemaDialect);
        diffObservedList(relation,
                         desired.checks(),
                         actual.checks(),
                         CheckConstraintDefinition::name,
                         (left, right) -> SchemaDefinitionEquality.sameCheck(left, right, schemaDialect),
                         SchemaOperation.Kind.ADD_CHECK,
                         SchemaOperation.Kind.CHANGE_CHECK,
                         SchemaOperation.Kind.DROP_CHECK,
                         "checks",
                         operations,
                         schemaDialect);
        diffObservedList(relation,
                         desired.foreignKeys(),
                         actual.foreignKeys(),
                         ForeignKeyDefinition::name,
                         (left, right) -> SchemaDefinitionEquality.sameForeignKey(left, right, schemaDialect),
                         SchemaOperation.Kind.ADD_FOREIGN_KEY,
                         SchemaOperation.Kind.CHANGE_FOREIGN_KEY,
                         SchemaOperation.Kind.DROP_FOREIGN_KEY,
                         "foreign-keys",
                         operations,
                         schemaDialect);
        actual.unknownAttributes().stream().sorted().forEach(attribute -> operations.add(
                manual(desired, actual, "unknown-" + attribute.name().toLowerCase(java.util.Locale.ROOT))));
    }

    private static SchemaSnapshot.Observed<List<IndexDefinition>> indexesForComparison(
            RelationalTableDefinition desired,
            SchemaSnapshot.Observed<List<IndexDefinition>> actual,
            boolean mysql
    ) {
        if (!mysql || actual.state() != SchemaSnapshot.State.PRESENT) {
            return actual;
        }
        Set<String> declaredIndexNames = names(desired.indexes(), IndexDefinition::name);
        Map<String, ForeignKeyDefinition> foreignKeysByName = byName(
                desired.foreignKeys(), ForeignKeyDefinition::name);
        List<IndexDefinition> comparable = actual.value().stream()
                .filter(index -> declaredIndexNames.contains(index.name())
                        || !isMySqlForeignKeySupportIndex(index, foreignKeysByName.get(index.name())))
                .toList();
        return SchemaSnapshot.Observed.present(comparable);
    }

    private static boolean isMySqlForeignKeySupportIndex(IndexDefinition index,
                                                          ForeignKeyDefinition foreignKey) {
        if (foreignKey == null || index.unique() || index.keys().size() != foreignKey.columns().size()) {
            return false;
        }
        for (int i = 0; i < index.keys().size(); i++) {
            if (!index.keys().get(i).column().equals(foreignKey.columns().get(i))
                    || index.keys().get(i).direction()
                    != com.flying.orm.core.metadata.IndexKeyPart.Direction.ASC) {
                return false;
            }
        }
        return true;
    }

    private static void diffTableComment(RelationalTableDefinition desired,
                                         SchemaSnapshot actual,
                                         List<SchemaOperation> operations) {
        if (actual.tableComment().state() == SchemaSnapshot.State.UNKNOWN
                || actual.tableComment().state() == SchemaSnapshot.State.PRESENT
                && !Objects.equals(desired.comment(), actual.tableComment().value())
                || actual.tableComment().state() == SchemaSnapshot.State.ABSENT
                && desired.comment() != null) {
            operations.add(manual(desired, actual, "table-comment"));
        }
    }

    private static void diffColumns(RelationIdentity relation,
                                    List<ColumnDefinition> desired,
                                    List<ColumnDefinition> actual,
                                    List<SchemaOperation> operations,
                                    SchemaDialect schemaDialect) {
        Map<String, ColumnDefinition> desiredByName = byName(
                desired, ColumnDefinition::name, schemaDialect);
        Map<String, ColumnDefinition> actualByName = byName(
                actual, ColumnDefinition::name, schemaDialect);
        for (ColumnDefinition target : desired) {
            ColumnDefinition current = actualByName.get(
                    SchemaDefinitionEquality.canonicalName(target.name(), schemaDialect));
            if (current == null) {
                operations.add(SchemaOperation.of(
                        SchemaOperation.Kind.ADD_COLUMN,
                        relation,
                        target.name(),
                        null,
                        target,
                        safeColumnAddition(target)
                                ? SchemaOperation.Compatibility.SAFE_INCREMENTAL
                                : SchemaOperation.Compatibility.REQUIRES_REVIEW));
            } else if (!SchemaDefinitionEquality.sameColumn(current, target, schemaDialect, relation)) {
                operations.add(SchemaOperation.of(
                        SchemaOperation.Kind.CHANGE_COLUMN,
                        relation,
                        target.name(),
                        current,
                        target,
                        SchemaOperation.Compatibility.REQUIRES_REVIEW));
            }
        }
        for (ColumnDefinition current : actual) {
            if (!desiredByName.containsKey(
                    SchemaDefinitionEquality.canonicalName(current.name(), schemaDialect))) {
                operations.add(SchemaOperation.of(
                        SchemaOperation.Kind.DROP_COLUMN,
                        relation,
                        current.name(),
                        current,
                        null,
                        compatibleExtraColumn(current)
                                ? SchemaOperation.Compatibility.COMPATIBLE_EXTRA
                                : SchemaOperation.Compatibility.REQUIRES_REVIEW));
            }
        }
    }

    private static void diffColumnOrder(RelationalTableDefinition desired,
                                        SchemaSnapshot actual,
                                        List<SchemaOperation> operations,
                                        SchemaDialect schemaDialect) {
        Set<String> desiredNames = names(desired.columns(), ColumnDefinition::name, schemaDialect);
        Set<String> actualNames = names(
                actual.columns().value(), ColumnDefinition::name, schemaDialect);
        List<String> desiredCommon = desired.columns().stream()
                .map(ColumnDefinition::name)
                .map(name -> SchemaDefinitionEquality.canonicalName(name, schemaDialect))
                .filter(actualNames::contains)
                .toList();
        List<String> actualCommon = actual.columns().value().stream()
                .map(ColumnDefinition::name)
                .map(name -> SchemaDefinitionEquality.canonicalName(name, schemaDialect))
                .filter(desiredNames::contains)
                .toList();
        if (!desiredCommon.equals(actualCommon)) {
            operations.add(manual(desired, actual, "column-order"));
        }
    }

    private static void diffPrimaryKey(RelationalTableDefinition desired,
                                       SchemaSnapshot actual,
                                       List<SchemaOperation> operations,
                                       boolean mysqlPrimaryName,
                                       SchemaDialect schemaDialect) {
        if (actual.primaryKey().state() == SchemaSnapshot.State.UNKNOWN) {
            operations.add(manual(desired, actual, "primary-key"));
            return;
        }
        PrimaryKeyDefinition target = desired.primaryKey().orElse(null);
        PrimaryKeyDefinition current = actual.primaryKey().value();
        if (current == null && target == null) {
            return;
        }
        if (current == null) {
            operations.add(operation(SchemaOperation.Kind.ADD_PRIMARY_KEY,
                                     desired.identity(), target.name(), null, target));
        } else if (target == null) {
            operations.add(operation(SchemaOperation.Kind.DROP_PRIMARY_KEY,
                                     desired.identity(), current.name(), current, null));
        } else if (!SchemaDefinitionEquality.samePrimaryKey(current, target, schemaDialect)
                && !(mysqlPrimaryName
                     && "PRIMARY".equalsIgnoreCase(current.name())
                     && SchemaDefinitionEquality.sameNames(
                             current.columns(), target.columns(), schemaDialect))) {
            operations.add(operation(SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                                     desired.identity(), target.name(), current, target));
        }
    }

    private static <T> void diffObservedList(RelationIdentity relation,
                                             List<T> desired,
                                             SchemaSnapshot.Observed<List<T>> actual,
                                             Function<T, String> name,
                                             BiPredicate<T, T> same,
                                             SchemaOperation.Kind addKind,
                                             SchemaOperation.Kind changeKind,
                                             SchemaOperation.Kind dropKind,
                                             String unknownName,
                                             List<SchemaOperation> operations,
                                             SchemaDialect schemaDialect) {
        if (actual.state() != SchemaSnapshot.State.PRESENT) {
            operations.add(SchemaOperation.of(
                    SchemaOperation.Kind.VERIFY_MANUALLY,
                    relation,
                    unknownName,
                    actual,
                    desired,
                    SchemaOperation.Compatibility.REQUIRES_REVIEW));
            return;
        }
        Map<String, T> desiredByName = byName(desired, name, schemaDialect);
        Map<String, T> actualByName = byName(actual.value(), name, schemaDialect);
        for (T target : desired) {
            String objectName = name.apply(target);
            T current = actualByName.get(
                    SchemaDefinitionEquality.canonicalName(objectName, schemaDialect));
            if (current == null) {
                operations.add(operation(addKind, relation, objectName, null, target));
            } else if (!same.test(current, target)) {
                operations.add(operation(changeKind, relation, objectName, current, target));
            }
        }
        for (T current : actual.value()) {
            String objectName = name.apply(current);
            if (!desiredByName.containsKey(
                    SchemaDefinitionEquality.canonicalName(objectName, schemaDialect))) {
                SchemaOperation.Compatibility compatibility =
                        dropKind == SchemaOperation.Kind.DROP_INDEX
                                && !((IndexDefinition) current).unique()
                                ? SchemaOperation.Compatibility.COMPATIBLE_EXTRA
                                : SchemaOperation.Compatibility.REQUIRES_REVIEW;
                operations.add(SchemaOperation.of(
                        dropKind, relation, objectName, current, null, compatibility));
            }
        }
    }

    private static SchemaOperation createTable(RelationalTableDefinition desired,
                                               DialectCapabilities capabilities) {
        return SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                desired.identity(),
                desired.identity().table(),
                null,
                desired,
                safeTableCreation(desired, capabilities)
                        ? SchemaOperation.Compatibility.SAFE_INCREMENTAL
                        : SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation operation(SchemaOperation.Kind kind,
                                             RelationIdentity relation,
                                             String objectName,
                                             Object actual,
                                             Object desired) {
        return SchemaOperation.of(kind, relation, objectName, actual, desired,
                                  SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation manual(RelationalTableDefinition desired,
                                          SchemaSnapshot actual,
                                          String fact) {
        return SchemaOperation.of(
                SchemaOperation.Kind.VERIFY_MANUALLY,
                desired.identity(),
                fact,
                actual,
                desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static boolean safeTableCreation(RelationalTableDefinition table,
                                             DialectCapabilities capabilities) {
        if (!table.foreignKeys().isEmpty()) {
            return false;
        }
        for (ColumnDefinition column : table.columns()) {
            ValueGeneration.Strategy strategy = column.generation().strategy();
            if (strategy == ValueGeneration.Strategy.IDENTITY
                    && !capabilities.supports(DialectCapabilityId.IDENTITY_COLUMNS)) {
                return false;
            }
            if (strategy == ValueGeneration.Strategy.SEQUENCE
                    && !capabilities.supports(DialectCapabilityId.SEQUENCES)) {
                return false;
            }
        }
        return true;
    }

    private static boolean safeColumnAddition(ColumnDefinition column) {
        return column.nullable()
                && column.defaultValue().kind() == ColumnDefault.Kind.NONE
                && !column.generation().generated();
    }

    private static boolean compatibleExtraColumn(ColumnDefinition column) {
        return column.nullable()
                || column.defaultValue().kind() != ColumnDefault.Kind.NONE
                || column.generation().generated();
    }

    private static <T> Map<String, T> byName(List<T> values, Function<T, String> name) {
        return byName(values, name, null);
    }

    private static <T> Map<String, T> byName(List<T> values,
                                             Function<T, String> name,
                                             SchemaDialect schemaDialect) {
        Map<String, T> indexed = new HashMap<>(Math.max(4, values.size() * 2));
        for (T value : values) {
            String objectName = SchemaDefinitionEquality.canonicalName(
                    name.apply(value), schemaDialect);
            if (indexed.put(objectName, value) != null) {
                throw new IllegalArgumentException("duplicate schema object name");
            }
        }
        return indexed;
    }

    private static <T> Set<String> names(List<T> values, Function<T, String> name) {
        return names(values, name, null);
    }

    private static <T> Set<String> names(List<T> values,
                                         Function<T, String> name,
                                         SchemaDialect schemaDialect) {
        Set<String> names = new HashSet<>(Math.max(4, values.size() * 2));
        for (T value : values) {
            names.add(SchemaDefinitionEquality.canonicalName(name.apply(value), schemaDialect));
        }
        return names;
    }

    private static void requireSameRelation(RelationIdentity desired,
                                            RelationIdentity actual,
                                            SchemaDialect schemaDialect) {
        if (!SchemaDefinitionEquality.sameRelation(desired, actual, schemaDialect)) {
            throw new IllegalArgumentException("desired and actual schema identities must match");
        }
    }

}
