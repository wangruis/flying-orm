package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;

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

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("reviewed-schema-plan/v2");

    private final DatabaseDescriptor database;
    private final SchemaCompatibilityMode compatibilityMode;
    private final String desiredFingerprint;
    private final String actualFingerprint;
    private final SchemaSnapshotCoverage snapshotCoverage;
    private final RelationalTableDefinition desiredTable;
    private final SchemaDialect comparisonDialect;
    private final List<SchemaPlanStep> steps;
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
        this.comparisonDialect = builder.comparisonDialect;
        if (desiredTable != null
                && !desiredFingerprint.equals(RelationalMetadataFingerprint.of(desiredTable))) {
            throw new IllegalArgumentException(
                    "desired schema fingerprint must match the verification table");
        }
        this.steps = List.copyOf(builder.steps);
        requireStableOrder(steps);
        this.fingerprint = fingerprint(database, compatibilityMode, desiredFingerprint,
                                       actualFingerprint, snapshotCoverage.fingerprint(), steps);
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

    SchemaDialect comparisonDialect() {
        return comparisonDialect;
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

    public SchemaMigrationRiskLevel risk() {
        return steps.stream()
                    .map(SchemaPlanStep::risk)
                    .max(java.util.Comparator.naturalOrder())
                    .orElse(SchemaMigrationRiskLevel.LOW);
    }

    public String fingerprint() {
        return fingerprint;
    }

    private static void requireStableOrder(List<SchemaPlanStep> steps) {
        for (int index = 0; index < steps.size(); index++) {
            if (steps.get(index) == null) {
                throw new IllegalArgumentException("schema plan steps must not contain null");
            }
            if (steps.get(index).order() != index) {
                throw new IllegalArgumentException(
                        "schema plan step order must be contiguous and start at zero");
            }
        }
    }

    private static String fingerprint(DatabaseDescriptor database,
                                      SchemaCompatibilityMode mode,
                                      String desired,
                                      String actual,
                                      String coverage,
                                      List<SchemaPlanStep> steps) {
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .text("DATABASE", database.fingerprint())
                                            .text("CAPABILITIES", database.capabilityFingerprint())
                                            .text("MODE", mode.name())
                                            .text("DESIRED", desired)
                                            .text("ACTUAL", actual)
                                            .text("SNAPSHOT_COVERAGE", coverage)
                                            .integer("STEP_COUNT", steps.size());
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
            this.desiredTable = Objects.requireNonNull(
                    value, "desired relational table must not be null");
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
