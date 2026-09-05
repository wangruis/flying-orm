package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 把一份显式多表关系定义编排成稳定的两阶段结构计划。
 *
 * <p>阶段一按依赖顺序创建表、列、主键、唯一约束和 CHECK，但刻意不携带索引与外键；所有表存在后，
 * 阶段二才发布索引和外键 operation。外键环由 SCC 精确识别，调用方没有确认循环闭合能力时只发布
 * {@link SchemaOperation.Kind#VERIFY_MANUALLY}。本规划器不读取连接、不渲染 SQL，也不扫描实体。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class MultiTableSchemaPlanner {

    private final DatabaseDescriptor database;
    private final ForeignKeyCycleSupport cycleSupport;

    public MultiTableSchemaPlanner(DatabaseDescriptor database, ForeignKeyCycleSupport cycleSupport) {
        this.database = Objects.requireNonNull(database, "database descriptor must not be null");
        this.cycleSupport = Objects.requireNonNull(
                cycleSupport, "foreign key cycle support must not be null");
    }

    /**
     * 只消费调用方显式提供的 Schema 快照；返回计划始终绑定构造时的同一个数据库描述。
     */
    public Plan plan(RelationalSchemaDefinition desired) {
        RelationalSchemaDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational schema must not be null");
        safeDesired.tables().forEach(RelationalTableDdlValidator::validatePartitionKeys);
        SchemaDependencyGraph graph = SchemaDependencyGraph.of(safeDesired);
        SchemaStronglyConnectedComponents components = graph.stronglyConnectedComponents();
        Set<ForeignKeyDefinition> manualCycleForeignKeys = cycleSupport == ForeignKeyCycleSupport.SUPPORTED
                ? Set.of() : manualCycleForeignKeys(components);
        Set<RelationIdentity> managedRelations = graph.tables().stream()
                .map(RelationalTableDefinition::identity)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        List<SchemaOperation> firstPhase = new ArrayList<>(graph.tables().size());
        for (RelationalTableDefinition table : components.dependencyOrder()) {
            RelationalTableDefinition baseTable = baseTable(table);
            firstPhase.add(SchemaOperation.of(
                    SchemaOperation.Kind.CREATE_TABLE,
                    table.identity(),
                    table.identity().table(),
                    null,
                    baseTable,
                    SchemaRiskClassifier.creationCompatibility(
                            baseTable, database.capabilities())));
        }

        List<SchemaOperation> secondPhase = new ArrayList<>();
        for (RelationalTableDefinition table : components.dependencyOrder()) {
            secondPhase.addAll(SchemaIndexPlanner.addOperations(table));
            secondPhase.addAll(SchemaForeignKeyPlanner.addOperations(
                    table,
                    foreignKey -> manualCycleForeignKeys.contains(foreignKey)
                            || !managedRelations.contains(foreignKey.referencedTable())));
        }
        return new Plan(database, cycleSupport, firstPhase, secondPhase);
    }

    private static RelationalTableDefinition baseTable(RelationalTableDefinition source) {
        RelationalTableDefinition.Builder target = RelationalTableDefinition.builder(source.identity());
        if (source.comment() != null) {
            target.comment(source.comment());
        }
        source.columns().forEach(target::addColumn);
        source.primaryKey().ifPresent(target::primaryKey);
        source.uniqueConstraints().forEach(target::addUnique);
        source.checks().forEach(target::addCheck);
        source.partition().ifPresent(target::partition);
        return target.build();
    }

    private static Set<ForeignKeyDefinition> manualCycleForeignKeys(
            SchemaStronglyConnectedComponents components) {
        Set<ForeignKeyDefinition> manual = Collections.newSetFromMap(new IdentityHashMap<>());
        for (List<RelationalTableDefinition> component : components.components()) {
            Set<RelationIdentity> identities = new HashSet<>();
            component.forEach(table -> identities.add(table.identity()));
            boolean cyclic = component.size() > 1 || component.getFirst().foreignKeys().stream()
                    .anyMatch(foreignKey -> foreignKey.referencedTable().equals(
                            component.getFirst().identity()));
            if (!cyclic) {
                continue;
            }
            for (RelationalTableDefinition table : component) {
                table.foreignKeys().stream()
                        .filter(foreignKey -> identities.contains(foreignKey.referencedTable()))
                        .forEach(manual::add);
            }
        }
        return manual;
    }

    /** 调用方基于已经解析的方言事实明确声明外键环能否通过第二阶段闭合。 */
    public enum ForeignKeyCycleSupport {
        SUPPORTED,
        MANUAL_REQUIRED
    }

    /** 单数据库、两阶段的不可变规范 operation 计划。 */
    public static final class Plan {

        private final DatabaseDescriptor database;
        private final ForeignKeyCycleSupport cycleSupport;
        private final List<SchemaOperation> firstPhase;
        private final List<SchemaOperation> secondPhase;
        private final List<SchemaOperation> operations;

        private Plan(DatabaseDescriptor database,
                     ForeignKeyCycleSupport cycleSupport,
                     List<SchemaOperation> firstPhase,
                     List<SchemaOperation> secondPhase) {
            this.database = database;
            this.cycleSupport = cycleSupport;
            this.firstPhase = List.copyOf(firstPhase);
            this.secondPhase = List.copyOf(secondPhase);
            List<SchemaOperation> ordered = new ArrayList<>(firstPhase.size() + secondPhase.size());
            ordered.addAll(firstPhase);
            ordered.addAll(secondPhase);
            operations = List.copyOf(ordered);
        }

        public DatabaseDescriptor database() {
            return database;
        }

        public ForeignKeyCycleSupport cycleSupport() {
            return cycleSupport;
        }

        public List<SchemaOperation> firstPhase() {
            return firstPhase;
        }

        public List<SchemaOperation> secondPhase() {
            return secondPhase;
        }

        public List<SchemaOperation> operations() {
            return operations;
        }

        public boolean requiresManualAction() {
            return operations.stream().anyMatch(
                    operation -> operation.kind() == SchemaOperation.Kind.VERIFY_MANUALLY);
        }
    }
}
