package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 与数据库身份、结构快照和精确 SQL 一起冻结的审核计划。
 *
 * <p>Builder 只负责一次性组装；构造后的计划完全不可变。执行边界只能按 {@link #steps()} 的原顺序
 * 使用其中保存的请求，不能重新 diff、重新渲染或替换 SQL。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class ReviewedSchemaPlan {

    /** 审核计划执行后必须观察到的关系终态。 */
    public enum TargetState {
        PRESENT,
        ABSENT
    }

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("reviewed-schema-plan/v3");

    private final DatabaseDescriptor database;
    private final SchemaCompatibilityMode compatibilityMode;
    private final String desiredFingerprint;
    private final String actualFingerprint;
    private final SchemaSnapshotCoverage snapshotCoverage;
    private final RelationalTableDefinition desiredTable;
    private final TargetState targetState;
    private final RelationIdentity targetIdentity;
    private final SchemaDialect comparisonDialect;
    private final List<SchemaPlanStep> steps;
    private final boolean writesQuiescedRequired;
    private final String fingerprint;

    private ReviewedSchemaPlan(Builder builder) {
        this.database = builder.database;
        this.compatibilityMode = Objects.requireNonNull(
                builder.compatibilityMode, "schema compatibility mode must not be null");
        this.desiredFingerprint = requireText(
                builder.desiredFingerprint, "desired schema fingerprint");
        this.actualFingerprint = requireText(
                builder.actualFingerprint, "actual schema fingerprint");
        this.snapshotCoverage = Objects.requireNonNull(
                builder.snapshotCoverage, "schema snapshot coverage must not be null");
        this.desiredTable = builder.desiredTable;
        this.targetState = builder.targetState;
        this.targetIdentity = builder.targetIdentity;
        this.comparisonDialect = builder.comparisonDialect;
        requireMatchingTarget();
        this.steps = List.copyOf(builder.steps);
        for (int index = 0; index < steps.size(); index++) {
            if (steps.get(index).order() != index) {
                throw new IllegalArgumentException(
                        "schema plan step order must be contiguous and start at zero");
            }
        }
        requireMatchingTargetSteps();
        requireExecutableDialectSupport();
        this.writesQuiescedRequired = ("oracle".equals(database.dialectId())
                || "mysql".equals(database.dialectId())) && steps.stream().anyMatch(step ->
                step.executable() && step.operation().kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY);
        this.fingerprint = fingerprint(database, compatibilityMode, desiredFingerprint,
                                       actualFingerprint, snapshotCoverage.fingerprint(),
                                       targetState, targetIdentity, steps, writesQuiescedRequired);
    }

    public static Builder builder(DatabaseDescriptor database) {
        return new Builder(database);
    }

    public DatabaseDescriptor database() {
        return database;
    }

    public String capabilityFingerprint() {
        return database.capabilityFingerprint();
    }

    public SchemaCompatibilityMode compatibilityMode() {
        return compatibilityMode;
    }

    public String desiredFingerprint() {
        return desiredFingerprint;
    }

    public String actualFingerprint() {
        return actualFingerprint;
    }

    public SchemaSnapshotCoverage snapshotCoverage() {
        return snapshotCoverage;
    }

    public String snapshotCoverageFingerprint() {
        return snapshotCoverage.fingerprint();
    }

    /**
     * 返回执行后重新比对使用的完整目标表。旧的仅指纹计划可以继续构造，但不能进入验证执行入口。
     */
    public Optional<RelationalTableDefinition> desiredTable() {
        return Optional.ofNullable(desiredTable);
    }

    /**
     * 返回明确冻结的目标终态。旧的仅指纹计划没有目标终态，因此返回空。
     */
    public Optional<TargetState> targetState() {
        return Optional.ofNullable(targetState);
    }

    /**
     * 返回执行前读取、缓存失效和执行后验证共同使用的关系身份。
     */
    public Optional<RelationIdentity> targetIdentity() {
        return Optional.ofNullable(targetIdentity);
    }

    SchemaDialect comparisonDialect() {
        return comparisonDialect;
    }

    /** 客户端必须证明它执行冻结 SQL 时使用的方言与计划一致。 */
    void requireExecutionDialect(RdbDialect configuredDialect) {
        if (configuredDialect == null) {
            for (SchemaPlanStep step : steps) {
                if (step.executable() && !existingNonPostgreSqlOperation(step.operation().kind())) {
                    throw new IllegalArgumentException(
                            "custom schema renderer cannot execute this reviewed operation");
                }
            }
            return;
        }
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

    /** 返回已经审核并保存的可执行请求；人工步骤不会伪装成空 SQL。 */
    public List<SqlRequest> requests() {
        return steps.stream().flatMap(step -> step.request().stream()).toList();
    }

    public boolean requiresManualAction() {
        return steps.stream().anyMatch(step -> !step.executable());
    }

    /**
     * 计划是否要求调用方保持相关写入静止。MySQL/Oracle FK 替换不能原子交接约束，必须使用
     * {@link SchemaMigrationApproval#approveWithWritesQuiesced(ReviewedSchemaPlan, String)} 才能执行。
     * 这只是执行合同，不代表 ORM 已停止或检查了任何数据库写入。
     */
    public boolean requiresWritesQuiesced() {
        return writesQuiescedRequired;
    }

    /** 实体批次预检和两个执行端口复用同一批准规则，避免执行到中途才发现后续表未被授权。 */
    boolean acceptsApproval(SchemaMigrationApproval approval) {
        if (requiresManualAction()) {
            return false;
        }
        boolean quiesced = requiresWritesQuiesced();
        if (!quiesced && risk() == SchemaMigrationRiskLevel.LOW) {
            return true;
        }
        return approval != null && fingerprint.equals(approval.planFingerprint())
                && (!quiesced || approval.writesQuiesced());
    }

    public SchemaMigrationRiskLevel risk() {
        return steps.stream()
                    .map(SchemaPlanStep::risk)
                    .max(java.util.Comparator.naturalOrder())
                    .orElse(SchemaMigrationRiskLevel.LOW);
    }

    public String fingerprint() {
        return fingerprint;
    }

    private void requireMatchingTarget() {
        if (targetState == null) {
            if (targetIdentity != null || desiredTable != null) {
                throw new IllegalArgumentException("schema target state and identity must be declared together");
            }
            return;
        }
        if (targetIdentity == null) {
            throw new IllegalArgumentException("schema target identity must not be null");
        }
        String expectedFingerprint;
        if (targetState == TargetState.PRESENT) {
            if (desiredTable == null || !targetIdentity.equals(desiredTable.identity())) {
                throw new IllegalArgumentException("present schema target must contain the same desired table");
            }
            expectedFingerprint = RelationalMetadataFingerprint.of(desiredTable);
        } else {
            if (desiredTable != null) {
                throw new IllegalArgumentException("absent schema target must not contain a desired table");
            }
            expectedFingerprint = SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(targetIdentity));
        }
        if (!desiredFingerprint.equals(expectedFingerprint)) {
            throw new IllegalArgumentException(
                    "desired schema fingerprint must match the verification target");
        }
    }

    private void requireMatchingTargetSteps() {
        if (targetState == null) {
            return;
        }
        for (SchemaPlanStep step : steps) {
            SchemaOperation operation = step.operation();
            if (!targetIdentity.equals(operation.relation())) {
                throw new IllegalArgumentException(
                        "schema plan step relation must match the verification target");
            }
            if (targetState == TargetState.ABSENT
                    && operation.kind() != SchemaOperation.Kind.DROP_TABLE
                    && operation.kind() != SchemaOperation.Kind.VERIFY_MANUALLY) {
                throw new IllegalArgumentException(
                        "absent schema target only accepts table removal or manual verification");
            }
            if (targetState == TargetState.PRESENT
                    && operation.kind() == SchemaOperation.Kind.DROP_TABLE) {
                throw new IllegalArgumentException(
                        "present schema target cannot contain a table removal step");
            }
        }
    }

    private void requireExecutableDialectSupport() {
        if (targetState == null) {
            return;
        }
        boolean postgreSql = "postgresql".equals(database.dialectId());
        for (SchemaPlanStep step : steps) {
            if (!step.executable()) {
                continue;
            }
            SchemaOperation.Kind kind = step.operation().kind();
            if (kind == SchemaOperation.Kind.VERIFY_MANUALLY
                    || !postgreSql && !existingNonPostgreSqlOperation(kind)
                    && !builtInEvolutionOperation(kind)) {
                throw new IllegalArgumentException(
                        "reviewed schema operation is not executable for this dialect");
            }
        }
    }

    private boolean builtInEvolutionOperation(SchemaOperation.Kind kind) {
        return switch (database.dialectId()) {
            case "mysql" -> kind != SchemaOperation.Kind.VERIFY_MANUALLY;
            case "h2", "sqlserver", "oracle" -> kind == SchemaOperation.Kind.CHANGE_PRIMARY_KEY
                    || kind == SchemaOperation.Kind.CHANGE_UNIQUE || kind == SchemaOperation.Kind.CHANGE_FOREIGN_KEY
                    || safeBuiltInEvolutionKind(kind);
            default -> false;
        };
    }

    private static boolean safeBuiltInEvolutionKind(SchemaOperation.Kind kind) {
        return switch (kind) {
                case CHANGE_COLUMN, CHANGE_INDEX, DROP_FOREIGN_KEY, DROP_CHECK,
                        DROP_UNIQUE, DROP_PRIMARY_KEY, DROP_COLUMN, DROP_TABLE -> true;
                default -> false;
        };
    }

    private static boolean existingNonPostgreSqlOperation(SchemaOperation.Kind kind) {
        return switch (kind) {
            case CREATE_TABLE, ADD_COLUMN, ADD_PRIMARY_KEY, ADD_UNIQUE, ADD_INDEX,
                    ADD_CHECK, ADD_FOREIGN_KEY, DROP_INDEX -> true;
            case CHANGE_COLUMN, CHANGE_PRIMARY_KEY, CHANGE_UNIQUE, CHANGE_INDEX,
                    CHANGE_CHECK, CHANGE_FOREIGN_KEY, DROP_FOREIGN_KEY, DROP_CHECK,
                    DROP_UNIQUE, DROP_PRIMARY_KEY, DROP_COLUMN, DROP_TABLE,
                    VERIFY_MANUALLY -> false;
        };
    }

    private static String fingerprint(DatabaseDescriptor database,
                                      SchemaCompatibilityMode mode,
                                      String desired,
                                      String actual,
                                      String coverage,
                                      TargetState targetState,
                                      RelationIdentity targetIdentity,
                                      List<SchemaPlanStep> steps,
                                      boolean writesQuiescedRequired) {
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .text("DATABASE", database.fingerprint())
                                            .text("CAPABILITIES", database.capabilityFingerprint())
                                            .text("MODE", mode.name())
                                            .text("DESIRED", desired)
                                            .text("ACTUAL", actual)
                                            .text("SNAPSHOT_COVERAGE", coverage)
                                            .nullableText("TARGET_STATE",
                                                    targetState == null ? null : targetState.name())
                                            .nullableText("TARGET_CATALOG",
                                                    targetIdentity == null
                                                            ? null : targetIdentity.catalog().orElse(null))
                                            .nullableText("TARGET_SCHEMA",
                                                    targetIdentity == null
                                                            ? null : targetIdentity.schema().orElse(null))
                                            .nullableText("TARGET_TABLE",
                                                    targetIdentity == null ? null : targetIdentity.table())
                                            .integer("STEP_COUNT", steps.size());
        if (writesQuiescedRequired) {
            // 只有此类新计划增加合同标记；普通计划的既有指纹格式不变。
            encoder.marker("WRITES_QUIESCED_REQUIRED_V1");
        }
        for (SchemaPlanStep step : steps) {
            SchemaOperation operation = step.operation();
            RelationIdentity relation = operation.relation();
            encoder.marker("STEP")
                   .integer("ORDER", step.order())
                   .text("OPERATION", operation.kind().name())
                   .nullableText("CATALOG", relation.catalog().orElse(null))
                   .nullableText("SCHEMA", relation.schema().orElse(null))
                   .text("TABLE", relation.table())
                   .text("OBJECT", operation.objectName())
                   .text("COMPATIBILITY", operation.compatibility().name())
                   .text("RISK", step.risk().name())
                   .bool("EXECUTABLE", step.executable());
            step.request().ifPresent(request -> encoder
                    .text("SQL", request.sql())
                    .text("BIND_MARKERS", request.bindMarkerStyle().name())
                    .integer("PARAMETER_COUNT", request.parameters().size()));
            encoder.integer("PRECONDITION_COUNT", step.preconditions().size());
            for (SchemaPlanPrecondition precondition : step.preconditions()) {
                encoder.text("PRECONDITION_KIND", precondition.kind().name())
                       .text("PRECONDITION_EXPECTED", precondition.expectedFingerprint());
            }
        }
        return encoder.finishHex();
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

    public static final class Builder {

        private final DatabaseDescriptor database;
        private final List<SchemaPlanStep> steps = new ArrayList<>();
        private SchemaCompatibilityMode compatibilityMode;
        private String desiredFingerprint;
        private String actualFingerprint;
        private SchemaSnapshotCoverage snapshotCoverage = SchemaSnapshotCoverage.complete();
        private RelationalTableDefinition desiredTable;
        private TargetState targetState;
        private RelationIdentity targetIdentity;
        private SchemaDialect comparisonDialect;

        private Builder(DatabaseDescriptor database) {
            this.database = Objects.requireNonNull(database, "database descriptor must not be null");
        }

        public Builder compatibilityMode(SchemaCompatibilityMode mode) {
            this.compatibilityMode = Objects.requireNonNull(
                    mode, "schema compatibility mode must not be null");
            return this;
        }

        public Builder desiredFingerprint(String value) {
            this.desiredFingerprint = value;
            return this;
        }

        public Builder actualFingerprint(String value) {
            this.actualFingerprint = value;
            return this;
        }

        public Builder snapshotCoverage(SchemaSnapshotCoverage value) {
            this.snapshotCoverage = Objects.requireNonNull(
                    value, "schema snapshot coverage must not be null");
            return this;
        }

        /** 保存执行后验证所需的完整目标定义，不参与 SQL 重新渲染。 */
        public Builder desiredTable(RelationalTableDefinition value) {
            if (targetState == TargetState.ABSENT) {
                throw new IllegalStateException("schema target cannot be both present and absent");
            }
            this.desiredTable = Objects.requireNonNull(
                    value, "desired relational table must not be null");
            this.targetState = TargetState.PRESENT;
            this.targetIdentity = desiredTable.identity();
            return this;
        }

        /** 保存一个由调用方明确提出、执行后必须确认不存在的关系目标。 */
        public Builder desiredAbsent(RelationIdentity value) {
            if (targetState == TargetState.PRESENT) {
                throw new IllegalStateException("schema target cannot be both present and absent");
            }
            this.targetState = TargetState.ABSENT;
            this.targetIdentity = Objects.requireNonNull(
                    value, "absent relational target identity must not be null");
            return this;
        }

        Builder comparisonDialect(SchemaDialect value) {
            this.comparisonDialect = Objects.requireNonNull(
                    value, "schema comparison dialect must not be null");
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

        public ReviewedSchemaPlan build() {
            return new ReviewedSchemaPlan(this);
        }
    }
}
