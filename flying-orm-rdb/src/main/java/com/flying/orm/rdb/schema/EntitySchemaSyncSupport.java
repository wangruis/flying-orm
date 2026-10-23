package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/** 启动同步共用的实体去重、结构差异校验和物理依赖检查。 */
final class EntitySchemaSyncSupport {

    private EntitySchemaSyncSupport() {
    }

    static List<EntitySchemaDescriptor<?>> targets(EntityModelRegistry models, Collection<Class<?>> entityTypes) {
        Collection<Class<?>> safeTypes = Objects.requireNonNull(entityTypes, "entity types must not be null");
        LinkedHashMap<Class<?>, EntitySchemaDescriptor<?>> byType = new LinkedHashMap<>();
        LinkedHashMap<RelationIdentity, Class<?>> byTable = new LinkedHashMap<>();
        for (Class<?> type : safeTypes) {
            Class<?> safeType = Objects.requireNonNull(type, "entity type must not be null");
            if (byType.containsKey(safeType)) {
                continue;
            }
            // 先完成全部实体的严格关系编译，再让任意元数据 reader 或 DDL 执行器接触数据库。
            EntitySchemaDescriptor<?> descriptor = models.schemaDescriptor(safeType);
            RelationIdentity tableKey = descriptor.table().identity();
            Class<?> previous = byTable.putIfAbsent(tableKey, safeType);
            if (previous != null) {
                throw new IllegalArgumentException("multiple entity types map to the same table '"
                                                           + descriptor.metadata().table() + "': " + previous.getName()
                                                           + " and " + safeType.getName());
            }
            byType.put(safeType, descriptor);
        }
        return List.copyOf(byType.values());
    }

    /** 父候选键仍被另一张实际表引用时，整批在第一条 SQL 前关闭。 */
    static void rejectReferencedCandidateKeyChanges(EntitySchemaSyncMode mode,
                                                     List<SchemaSnapshot> snapshots,
                                                     List<SchemaMigrationPlan> plans,
                                                     SchemaDialect dialect) {
        if (mode == EntitySchemaSyncMode.VALIDATE) {
            return;
        }
        for (SchemaMigrationPlan plan : plans) {
            for (SchemaPlanStep step : plan.steps()) {
                SchemaOperation operation = step.operation();
                boolean physicalActualTypes = snapshots.stream()
                        .filter(snapshot -> snapshot.identity().equals(operation.relation()))
                        .findFirst()
                        .map(SchemaSnapshot::physicalColumnTypes)
                        .orElse(true);
                for (SchemaSnapshot snapshot : snapshots) {
                    if (snapshot.identity().equals(operation.relation())
                            || snapshot.foreignKeys().state() != SchemaSnapshot.State.PRESENT) {
                        continue;
                    }
                    boolean blocked = snapshot.foreignKeys().value().stream()
                            .filter(foreignKey -> foreignKey.reference().equals(operation.relation()))
                            .anyMatch(foreignKey -> invalidatesReference(
                                    operation, foreignKey.referenceColumns(), dialect, physicalActualTypes));
                    if (blocked) {
                        throw new EntitySchemaSyncException(
                                "entity relational schema synchronization changes a candidate key "
                                        + "that is still referenced by another table",
                                new EntitySchemaSyncReport(mode, plans, List.of()));
                    }
                }
            }
        }
    }

    private static boolean invalidatesReference(SchemaOperation operation,
                                                List<String> referenceColumns,
                                                SchemaDialect dialect,
                                                boolean physicalActualTypes) {
        return switch (operation.kind()) {
            case CHANGE_PRIMARY_KEY, DROP_PRIMARY_KEY ->
                    SchemaDefinitionEquality.sameCandidateKeyColumns(
                            referenceColumns,
                            ((PrimaryKeyDefinition) operation.actual()).columns(),
                            dialect);
            case CHANGE_UNIQUE, DROP_UNIQUE ->
                    SchemaDefinitionEquality.sameCandidateKeyColumns(
                            referenceColumns,
                            ((UniqueConstraintDefinition) operation.actual()).columns(),
                            dialect);
            case CHANGE_INDEX, DROP_INDEX -> {
                IndexDefinition index = (IndexDefinition) operation.actual();
                yield index.unique() && SchemaDefinitionEquality.sameCandidateKeyColumns(
                        referenceColumns,
                        index.keys().stream().map(key -> key.column()).toList(),
                        dialect);
            }
            case CHANGE_COLUMN -> referenceColumns.contains(
                    ((ColumnDefinition) operation.actual()).name())
                    && changesPhysicalType((ColumnDefinition) operation.actual(),
                            (ColumnDefinition) operation.desired(), dialect, physicalActualTypes);
            case DROP_COLUMN -> referenceColumns.contains(
                    ((ColumnDefinition) operation.actual()).name());
            default -> false;
        };
    }

    private static boolean changesPhysicalType(ColumnDefinition actual,
                                               ColumnDefinition desired,
                                               SchemaDialect dialect,
                                               boolean physicalActualTypes) {
        return !SchemaDefinitionEquality.sameColumnType(actual, desired, dialect, physicalActualTypes);
    }

}
