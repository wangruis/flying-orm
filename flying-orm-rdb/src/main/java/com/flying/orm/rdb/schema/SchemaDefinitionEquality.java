package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.internal.template.SchemaDefaultExpressions;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 关系结构值的方言感知等价规则。
 *
 * <p>这里只判断两个已经构造完成的结构事实是否等价，不创建迁移操作，也不读取数据库。
 * 集中这些规则可以让差异编排保持短小，同时避免列、约束和名称比较各自演化出不同语义。</p>
 */
final class SchemaDefinitionEquality {

    private static final String POSTGRESQL_VECTOR_EXTENSION_MARKER = "_flying_orm_pgvector_";

    private SchemaDefinitionEquality() {
    }

    static boolean sameColumn(ColumnDefinition left,
                              ColumnDefinition right,
                              SchemaDialect schemaDialect,
                              RelationIdentity relation,
                              boolean physicalActualTypes) {
        return sameName(left.name(), right.name(), schemaDialect)
                && sameColumnType(left, right, schemaDialect, physicalActualTypes)
                && sameColumnAttributes(left, right, schemaDialect, relation);
    }

    /** 类型已解析、改名已规划后，复用相同的列属性比较。 */
    static boolean sameColumnAttributes(ColumnDefinition left, ColumnDefinition right,
                                        SchemaDialect schemaDialect, RelationIdentity relation) {
        return left.nullable() == right.nullable()
                && sameDefault(left.defaultValue(), right.defaultValue(), schemaDialect)
                && Objects.equals(left.comment(), right.comment())
                && sameGeneration(left.generation(), right.generation(), schemaDialect, relation)
                // The left side is observed metadata; only explicitly desired options constrain it.
                && (right.defaultConstraintName() == null || sameNullableName(
                        left.defaultConstraintName(), right.defaultConstraintName(), schemaDialect))
                && (right.charset() == null || right.charset().equals(left.charset()))
                && (right.collation() == null || right.collation().equals(left.collation()));
    }

    static boolean samePrimaryKey(PrimaryKeyDefinition left,
                                  PrimaryKeyDefinition right,
                                  SchemaDialect schemaDialect) {
        return sameName(left.name(), right.name(), schemaDialect)
                && sameNames(left.columns(), right.columns(), schemaDialect);
    }

    static boolean sameUnique(UniqueConstraintDefinition left,
                              UniqueConstraintDefinition right,
                              SchemaDialect schemaDialect) {
        return sameName(left.name(), right.name(), schemaDialect)
                && sameNames(left.columns(), right.columns(), schemaDialect)
                && (left.nullPolicy() == right.nullPolicy() || nativeNullsDistinct(schemaDialect));
    }

    /** 只比较可观察的物理语义，不把读回的 DEFAULT 改写成用户的声明意图。 */
    static boolean nativeNullsDistinct(SchemaDialect dialect) {
        return dialect != null && switch (dialect.generatedValueStyle()) {
            case POSTGRESQL, MYSQL, H2 -> true;
            default -> false;
        };
    }

    static boolean ordinaryUnique(UniqueConstraintDefinition unique, SchemaDialect dialect) {
        return unique.nullPolicy() == UniqueNullPolicy.DEFAULT || nativeNullsDistinct(dialect);
    }

    static boolean sameCheck(CheckConstraintDefinition left,
                             CheckConstraintDefinition right,
                             SchemaDialect schemaDialect) {
        return sameName(left.name(), right.name(), schemaDialect)
                && SchemaPredicateEquality.samePredicate(left.predicate(), right.predicate(), schemaDialect);
    }

    static boolean sameIndex(IndexDefinition left,
                             IndexDefinition right,
                             SchemaDialect schemaDialect) {
        if (!sameName(left.name(), right.name(), schemaDialect)
                || left.unique() != right.unique()
                || left.keys().size() != right.keys().size()) {
            return false;
        }
        for (int index = 0; index < left.keys().size(); index++) {
            IndexKeyPart leftKey = left.keys().get(index);
            IndexKeyPart rightKey = right.keys().get(index);
            if (!sameName(leftKey.column(), rightKey.column(), schemaDialect)
                    || leftKey.direction() != rightKey.direction()) {
                return false;
            }
        }
        return true;
    }

    static boolean sameForeignKey(ForeignKeyDefinition left,
                                  ForeignKeyDefinition right,
                                  SchemaDialect schemaDialect) {
        return sameName(left.name(), right.name(), schemaDialect)
                && sameNames(left.columns(), right.columns(), schemaDialect)
                && sameRelation(left.reference(), right.reference(), schemaDialect)
                && sameNames(left.referenceColumns(), right.referenceColumns(), schemaDialect)
                && left.onDelete() == right.onDelete()
                && left.onUpdate() == right.onUpdate();
    }

    static boolean samePartition(TablePartitionDefinition left,
                                 TablePartitionDefinition right,
                                 SchemaDialect schemaDialect) {
        return left.strategy() == right.strategy()
                && sameName(left.column(), right.column(), schemaDialect);
    }

    static boolean sameRelation(RelationIdentity left,
                                RelationIdentity right,
                                SchemaDialect schemaDialect) {
        return sameNullableName(left.catalog().orElse(null), right.catalog().orElse(null), schemaDialect)
                && sameNullableName(left.schema().orElse(null), right.schema().orElse(null), schemaDialect)
                && sameName(left.table(), right.table(), schemaDialect);
    }

    static String canonicalName(String value, SchemaDialect schemaDialect) {
        return schemaDialect != null
                && schemaDialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2
                ? value.toLowerCase(Locale.ROOT) : value;
    }

    static boolean sameColumnType(ColumnDefinition left,
                                  ColumnDefinition right,
                                  SchemaDialect schemaDialect,
                                  boolean physicalActualTypes) {
        if (!physicalActualTypes && schemaDialect != null) {
            return schemaDialect.sameDataType(
                    desiredColumnType(schemaDialect, left), desiredColumnType(schemaDialect, right));
        }
        return sameColumnType(left, right, schemaDialect);
    }

    static boolean sameColumnType(ColumnDefinition left,
                                  ColumnDefinition right,
                                  SchemaDialect schemaDialect) {
        if (schemaDialect == null) {
            return left.databaseType().equals(right.databaseType())
                    && Objects.equals(left.length(), right.length())
                    && Objects.equals(left.precision(), right.precision())
                    && Objects.equals(left.scale(), right.scale())
                    && Objects.equals(left.temporalPrecision(), right.temporalPrecision());
        }
        String actual = observedColumnType(schemaDialect, left);
        return schemaDialect.sameDataType(actual, SchemaTypeMapping.preserveCharacterLengthUnit(
                schemaDialect, actual, desiredColumnType(schemaDialect, right)));
    }

    static String actualColumnDdlType(SchemaDialect dialect,
                                      ColumnDefinition column,
                                      boolean physicalActualTypes) {
        return physicalActualTypes
                ? actualColumnDdlType(dialect, column) : desiredColumnType(dialect, column);
    }

    static String actualColumnDdlType(SchemaDialect dialect, ColumnDefinition column) {
        String actual = observedColumnType(dialect, column);
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            return actual;
        }
        DatabaseType parsed = DatabaseType.of(actual).requireSafe("actual column data type");
        String base = parsed.baseName().toLowerCase(Locale.ROOT);
        if (!base.startsWith(POSTGRESQL_VECTOR_EXTENSION_MARKER)
                || !base.endsWith(".vector")) {
            return actual;
        }
        String schema = base.substring(
                POSTGRESQL_VECTOR_EXTENSION_MARKER.length(), base.length() - ".vector".length());
        String arguments = parsed.arguments().isEmpty()
                ? "" : "(" + String.join(",", parsed.arguments()) + ")";
        return schema + ".vector" + arguments + "[]".repeat(parsed.arrayDimensions());
    }

    static String observedColumnType(SchemaDialect dialect, ColumnDefinition column) {
        Integer precision = column.databaseType().logicalType().temporal()
                ? column.temporalPrecision() : column.precision();
        return dialect.physicalDataType(
                column.databaseType().declaration(), column.length(), precision, column.scale());
    }

    static String desiredColumnType(SchemaDialect dialect, ColumnDefinition column) {
        Integer precision = column.databaseType().logicalType().temporal()
                ? column.temporalPrecision() : column.precision();
        return dialect.dataType(
                column.databaseType().declaration(), column.length(), precision, column.scale());
    }

    static boolean sameGeneration(ValueGeneration actual,
                                          ValueGeneration desired,
                                          SchemaDialect schemaDialect,
                                          RelationIdentity relation) {
        return sameSequenceName(actual.sequenceName(), desired.sequenceName(), schemaDialect, relation)
                && SchemaGeneratedValueComparison.sameOptions(actual, desired,
                        schemaDialect == null ? null : schemaDialect.generatedValueStyle());
    }

    private static boolean sameSequenceName(String actual, String desired,
                                             SchemaDialect dialect, RelationIdentity relation) {
        if (sameNullableName(actual, desired, dialect)) {
            return true;
        }
        // Readers omit only a sequence's own table schema; do not infer the resolution of desired bare names.
        return actual != null && desired != null
                && sameName(observedSequenceName(actual, relation), desired, dialect);
    }

    static String observedSequenceName(String actual, RelationIdentity relation) {
        if (actual == null || actual.indexOf('.') >= 0 || relation.schema().isEmpty()) {
            return actual;
        }
        return relation.schema().orElseThrow() + "." + actual;
    }

    static boolean sameDefault(ColumnDefault left, ColumnDefault right, SchemaDialect dialect) {
        if (left.kind() != right.kind()) {
            return false;
        }
        if (left.kind() == ColumnDefault.Kind.NATIVE_EXPRESSION) {
            if (dialect == null) {
                return left.equals(right);
            }
            return SchemaDefaultExpressions.normalize(
                    (String) left.value().orElseThrow(), dialect.generatedValueStyle().name()).equals(
                    SchemaDefaultExpressions.normalize(
                            (String) right.value().orElseThrow(), dialect.generatedValueStyle().name()));
        }
        return left.kind() != ColumnDefault.Kind.LITERAL
                || SchemaPredicateEquality.sameLiteral(left.value().orElseThrow(), right.value().orElseThrow(), dialect);
    }

    static boolean sameNames(List<String> left,
                             List<String> right,
                             SchemaDialect schemaDialect) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!sameName(left.get(index), right.get(index), schemaDialect)) {
                return false;
            }
        }
        return true;
    }

    static boolean sameCandidateKeyColumns(List<String> left,
                                           List<String> right,
                                           SchemaDialect schemaDialect) {
        if (schemaDialect == null
                || schemaDialect.generatedValueStyle()
                != SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            return sameNames(left, right, schemaDialect);
        }
        return left.size() == right.size()
                && left.stream().allMatch(leftName -> right.stream()
                        .anyMatch(rightName -> sameName(leftName, rightName, schemaDialect)));
    }

    private static boolean sameNullableName(String left,
                                            String right,
                                            SchemaDialect schemaDialect) {
        if (left == null || right == null) {
            return left == null && right == null;
        }
        return sameName(left, right, schemaDialect);
    }

    static boolean sameName(String left, String right, SchemaDialect schemaDialect) {
        return canonicalName(left, schemaDialect).equals(canonicalName(right, schemaDialect));
    }
}
