package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 保存数据库身份、结构操作、目标定义和精确 SQL 的计划。
 *
 * <p>每项结构操作只保存一次，其 SQL 按操作顺序展开为不可变的 {@link #requests()}。
 * 执行边界使用这些请求，不重新 diff、渲染或排序。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaMigrationPlan {

    /** 调用方声明的关系目标，不代表执行后验证。 */
    public enum TargetState {
        PRESENT,
        ABSENT
    }

    private final DatabaseDescriptor database;
    private final SchemaCompatibilityMode compatibilityMode;
    private final SchemaSnapshotCoverage snapshotCoverage;
    private final RelationalTableDefinition desiredTable;
    private final RelationIdentity absentRelation;
    private final List<SchemaPlanStep> steps;
    private final List<SqlRequest> requests;
    private final boolean tableExists;
    private final List<SkippedSchemaChange> skippedChanges;
    private final List<RelationIdentity> additionalCreatedTables;

    private SchemaMigrationPlan(Builder builder) {
        this.database = builder.database;
        this.compatibilityMode = builder.compatibilityMode;
        this.snapshotCoverage = builder.snapshotCoverage;
        this.desiredTable = builder.desiredTable;
        this.absentRelation = builder.absentRelation;
        this.steps = List.copyOf(builder.steps);
        this.requests = builder.requests == null
                ? switch (steps.size()) {
                    case 0 -> List.of();
                    case 1 -> steps.getFirst().requests();
                    default -> steps.stream().flatMap(step -> step.requests().stream()).toList();
                }
                : List.copyOf(builder.requests);
        this.tableExists = builder.tableExists;
        this.skippedChanges = List.copyOf(builder.skippedChanges);
        this.additionalCreatedTables = List.copyOf(builder.additionalCreatedTables);
        if (builder.requests != null) {
            for (SqlRequest request : requests) {
                if (!request.parameters().isEmpty()) {
                    throw new IllegalArgumentException("schema DDL must not contain bound parameters");
                }
            }
        }
        requireMatchingTargetSteps();
    }


    /** 显式 SQL 计划；不推测自定义 SQL 与结构操作的对应关系。 */
    public SchemaMigrationPlan(RelationalTableDefinition target, boolean tableExists,
                               List<SqlRequest> requests, List<SkippedSchemaChange> skippedChanges) {
        this(target, tableExists, requests, skippedChanges, List.of());
    }

    public SchemaMigrationPlan(RelationalTableDefinition target, boolean tableExists,
                               List<SqlRequest> requests, List<SkippedSchemaChange> skippedChanges,
                               List<RelationIdentity> additionalCreatedTables) {
        this(new Builder(null).desiredTable(target).tableExists(tableExists)
                .sql(requests, skippedChanges, additionalCreatedTables));
    }

    public RelationalTableDefinition target() {
        return desiredTable;
    }

    static SchemaMigrationPlan planned(RelationalTableDefinition target, boolean tableExists,
                                       List<SchemaPlanStep> steps, List<SkippedSchemaChange> skipped) {
        return planned(target, tableExists, steps, skipped, List.of());
    }

    static SchemaMigrationPlan planned(RelationalTableDefinition target, boolean tableExists,
                                       List<SchemaPlanStep> steps, List<SkippedSchemaChange> skipped,
                                       List<RelationIdentity> additionalTables) {
        Builder builder = new Builder(null).desiredTable(target).tableExists(tableExists).steps(steps);
        builder.skippedChanges = skipped;
        builder.additionalCreatedTables = additionalTables;
        return builder.build();
    }

    public boolean tableExists() {
        return tableExists;
    }

    public List<SkippedSchemaChange> skippedChanges() {
        return skippedChanges;
    }

    public List<RelationIdentity> additionalCreatedTables() {
        return additionalCreatedTables;
    }

    public List<SqlRequest> requestsForExecution() {
        if (skippedChanges.stream().anyMatch(change -> change.kind() == SkippedSchemaChange.Kind.CHANGE_PRIMARY_KEY)) {
            throw new SchemaMigrationRejectedException(SchemaMigrationFailureCode.MANUAL_ACTION_REQUIRED,
                    "primary key changes require a complete table target; no schema SQL was executed");
        }
        return requests;
    }

    public boolean hasSkippedChanges() {
        return !skippedChanges.isEmpty();
    }

    public boolean hasExecutableSql() {
        return !requests.isEmpty();
    }

    public int executableSqlCount() {
        return requests.size();
    }

    public int skippedCount() {
        return skippedChanges.size();
    }

    public List<String> sqlTexts() {
        return requests.stream().map(SqlRequest::sql).toList();
    }

    public List<String> skippedSummaries() {
        return skippedChanges.stream().map(SkippedSchemaChange::summary).toList();
    }

    public static Builder builder(DatabaseDescriptor database) {
        return new Builder(Objects.requireNonNull(database, "database descriptor must not be null"));
    }

    /** 完整关系规划使用的数据库描述；显式 SQL 或普通表单未提供时为 null。 */
    public DatabaseDescriptor database() {
        return database;
    }

    public String capabilityFingerprint() {
        return database == null ? "" : database.capabilityFingerprint();
    }

    /** 完整关系比较模式；普通表单由其显式迁移选项选择操作，此处为 null。 */
    public SchemaCompatibilityMode compatibilityMode() {
        return compatibilityMode;
    }

    public SchemaSnapshotCoverage snapshotCoverage() {
        return snapshotCoverage;
    }

    /**
     * 返回规划使用的声明目标表；普通增量声明不等于执行后的完整结构快照。
     */
    public Optional<RelationalTableDefinition> desiredTable() {
        return Optional.ofNullable(desiredTable);
    }

    /**
     * 返回明确冻结的目标终态。未声明目标时返回空。
     */
    public Optional<TargetState> targetState() {
        return desiredTable != null ? Optional.of(TargetState.PRESENT)
                : absentRelation != null ? Optional.of(TargetState.ABSENT) : Optional.empty();
    }

    /**
     * 返回规划及元数据失效使用的关系身份。
     */
    public Optional<RelationIdentity> targetIdentity() {
        return Optional.ofNullable(desiredTable == null ? absentRelation : desiredTable.identity());
    }

    /** 已配置方言时必须与计划一致；显式 SQL 执行端不推断后端支持的操作。 */
    void requireExecutionDialect(RdbDialect configuredDialect) {
        if (database == null) return;
        if (!database.dialectId().equals(configuredDialect.name())
                || !database.capabilityFingerprint().equals(
                        configuredDialect.capabilities().fingerprint())) {
            throw new IllegalArgumentException(
                    "reviewed schema plan does not match the client dialect and capabilities");
        }
    }

    public List<SchemaPlanStep> steps() {
        return steps;
    }

    public List<SchemaOperation> operations() {
        return steps.stream().map(SchemaPlanStep::operation).toList();
    }

    /** 返回规划时生成的有序 SQL。 */
    public List<SqlRequest> requests() {
        return requests;
    }

    private void requireMatchingTargetSteps() {
        RelationIdentity relation = desiredTable == null ? absentRelation : desiredTable.identity();
        if (relation == null) {
            return;
        }
        for (SchemaPlanStep step : steps) {
            SchemaOperation operation = step.operation();
            if (!relation.equals(operation.relation())
                    && !additionalCreatedTables.contains(operation.relation())) {
                throw new IllegalArgumentException(
                        "schema plan step relation must match the verification target");
            }
            if (absentRelation != null
                    && operation.kind() != SchemaOperation.Kind.DROP_TABLE) {
                throw new IllegalArgumentException(
                        "absent schema target only accepts table removal");
            }
            if (desiredTable != null
                    && operation.kind() == SchemaOperation.Kind.DROP_TABLE) {
                throw new IllegalArgumentException(
                        "present schema target cannot contain a table removal step");
            }
        }
    }

    public static final class Builder {

        private final DatabaseDescriptor database;
        private final List<SchemaPlanStep> steps = new ArrayList<>();
        private SchemaCompatibilityMode compatibilityMode;
        private SchemaSnapshotCoverage snapshotCoverage = SchemaSnapshotCoverage.none();
        private RelationalTableDefinition desiredTable;
        private RelationIdentity absentRelation;
        private boolean tableExists;
        private List<SqlRequest> requests;
        private List<SkippedSchemaChange> skippedChanges = List.of();
        private List<RelationIdentity> additionalCreatedTables = List.of();

        private Builder(DatabaseDescriptor database) {
            this.database = database;
        }

        public Builder tableExists(boolean value) {
            tableExists = value;
            return this;
        }

        private Builder sql(List<SqlRequest> sql, List<SkippedSchemaChange> skipped, List<RelationIdentity> additional) {
            requests = Objects.requireNonNull(sql, "migration sql requests must not be null");
            skippedChanges = skipped;
            additionalCreatedTables = additional;
            return this;
        }

        public Builder compatibilityMode(SchemaCompatibilityMode mode) {
            this.compatibilityMode = Objects.requireNonNull(
                    mode, "schema compatibility mode must not be null");
            return this;
        }

        public Builder snapshotCoverage(SchemaSnapshotCoverage value) {
            this.snapshotCoverage = Objects.requireNonNull(
                    value, "schema snapshot coverage must not be null");
            return this;
        }

        /** 保存规划使用的完整目标定义，不参与 SQL 重新渲染。 */
        public Builder desiredTable(RelationalTableDefinition value) {
            if (absentRelation != null) {
                throw new IllegalStateException("schema target cannot be both present and absent");
            }
            this.desiredTable = Objects.requireNonNull(
                    value, "desired relational table must not be null");
            return this;
        }

        /** 保存调用方明确提出的关系删除目标。 */
        public Builder desiredAbsent(RelationIdentity value) {
            if (desiredTable != null) {
                throw new IllegalStateException("schema target cannot be both present and absent");
            }
            this.absentRelation = Objects.requireNonNull(
                    value, "absent relational target identity must not be null");
            return this;
        }

        public Builder addStep(SchemaPlanStep step) {
            steps.add(Objects.requireNonNull(step, "schema plan step must not be null"));
            return this;
        }

        public Builder steps(List<SchemaPlanStep> values) {
            steps.clear();
            steps.addAll(Objects.requireNonNull(values, "schema plan steps must not be null"));
            return this;
        }

        public SchemaMigrationPlan build() {
            return new SchemaMigrationPlan(this);
        }
    }
}
