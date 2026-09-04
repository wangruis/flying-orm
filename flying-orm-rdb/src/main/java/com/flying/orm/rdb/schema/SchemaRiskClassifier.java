package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;

import java.util.Objects;

/**
 * 根据结构化差异、数据库事实快照和已确认方言能力判定迁移风险。
 *
 * <p>只有能由三者共同证明的新表或普通可空列才返回 {@link SchemaMigrationRiskLevel#LOW}；
 * 缺失事实、未知能力和所有收窄或破坏性操作都保守进入审核。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaRiskClassifier {

    private SchemaRiskClassifier() {
    }

    /** 返回一项结构操作在当前事实下的上线风险。 */
    public static SchemaMigrationRiskLevel classify(SchemaOperation operation,
                                                     SchemaSnapshot actual,
                                                     DialectCapabilities capabilities) {
        SchemaOperation safeOperation = Objects.requireNonNull(
                operation, "schema operation must not be null");
        SchemaSnapshot safeActual = Objects.requireNonNull(actual, "schema snapshot must not be null");
        DialectCapabilities safeCapabilities = Objects.requireNonNull(
                capabilities, "dialect capabilities must not be null");

        if (safeOperation.compatibility() == SchemaOperation.Compatibility.SAFE_INCREMENTAL
                && safeOperation.relation().equals(safeActual.identity())) {
            if (safeOperation.kind() == SchemaOperation.Kind.CREATE_TABLE
                    && safeTableCreation(safeOperation, safeActual, safeCapabilities)) {
                return SchemaMigrationRiskLevel.LOW;
            }
            if (safeOperation.kind() == SchemaOperation.Kind.ADD_COLUMN
                    && safeNullableColumnAddition(safeOperation, safeActual)) {
                return SchemaMigrationRiskLevel.LOW;
            }
        }
        return reviewRisk(safeOperation.kind());
    }

    /** @return 当前事实是否足以证明该操作可以安全增量执行 */
    public static boolean safeIncremental(SchemaOperation operation,
                                          SchemaSnapshot actual,
                                          DialectCapabilities capabilities) {
        return classify(operation, actual, capabilities) == SchemaMigrationRiskLevel.LOW;
    }

    private static boolean safeTableCreation(SchemaOperation operation,
                                             SchemaSnapshot actual,
                                             DialectCapabilities capabilities) {
        if (actual.tableState() != SchemaSnapshot.State.ABSENT
                || !(operation.desired() instanceof RelationalTableDefinition table)
                || !table.identity().equals(operation.relation())
                || creationCompatibility(table, capabilities)
                != SchemaOperation.Compatibility.SAFE_INCREMENTAL) {
            return false;
        }
        return true;
    }

    static SchemaOperation.Compatibility creationCompatibility(
            RelationalTableDefinition table,
            DialectCapabilities capabilities) {
        RelationalTableDefinition safeTable = Objects.requireNonNull(
                table, "relational table definition must not be null");
        DialectCapabilities safeCapabilities = Objects.requireNonNull(
                capabilities, "dialect capabilities must not be null");
        if (!safeTable.foreignKeys().isEmpty()) {
            return SchemaOperation.Compatibility.REQUIRES_REVIEW;
        }
        for (ColumnDefinition column : safeTable.columns()) {
            if (!supports(column.generation(), safeCapabilities)) {
                return SchemaOperation.Compatibility.REQUIRES_REVIEW;
            }
        }
        return SchemaOperation.Compatibility.SAFE_INCREMENTAL;
    }

    private static boolean safeNullableColumnAddition(SchemaOperation operation,
                                                      SchemaSnapshot actual) {
        if (actual.tableState() != SchemaSnapshot.State.PRESENT
                || actual.columns().state() != SchemaSnapshot.State.PRESENT
                || !(operation.desired() instanceof ColumnDefinition desired)
                || !operation.objectName().equals(desired.name())
                || !desired.nullable()
                || desired.defaultValue().kind() != ColumnDefault.Kind.NONE
                || desired.generation().generated()) {
            return false;
        }
        return actual.columns().value().stream()
                     .noneMatch(column -> column.name().equals(desired.name()));
    }

    private static boolean supports(ValueGeneration generation, DialectCapabilities capabilities) {
        return switch (generation.strategy()) {
            case NONE -> true;
            case IDENTITY -> capabilities.supports(DialectCapabilityId.IDENTITY_COLUMNS);
            case SEQUENCE -> capabilities.supports(DialectCapabilityId.SEQUENCES);
        };
    }

    private static SchemaMigrationRiskLevel reviewRisk(SchemaOperation.Kind kind) {
        return switch (kind) {
            case ADD_PRIMARY_KEY, CHANGE_PRIMARY_KEY, DROP_PRIMARY_KEY,
                    ADD_FOREIGN_KEY, CHANGE_FOREIGN_KEY, DROP_FOREIGN_KEY,
                    CHANGE_COLUMN, DROP_COLUMN, DROP_TABLE -> SchemaMigrationRiskLevel.CRITICAL;
            case CREATE_TABLE, ADD_COLUMN, ADD_UNIQUE, CHANGE_UNIQUE, DROP_UNIQUE,
                    ADD_INDEX, CHANGE_INDEX, DROP_INDEX,
                    ADD_CHECK, CHANGE_CHECK, DROP_CHECK,
                    VERIFY_MANUALLY -> SchemaMigrationRiskLevel.HIGH;
        };
    }
}
