package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 审核计划中已经冻结顺序的一步。
 *
 * <p>可执行步骤直接保存审核时看到的 {@link SqlRequest}；执行器不能再渲染或重排。人工步骤没有
 * SQL，用于明确表示当前事实或方言能力不足，不能把它悄悄当成空操作成功。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaPlanStep {

    private final int order;
    private final SchemaOperation operation;
    private final SqlRequest request;
    private final SchemaMigrationRiskLevel risk;
    private final List<SchemaPlanPrecondition> preconditions;

    private SchemaPlanStep(int order,
                           SchemaOperation operation,
                           SqlRequest request,
                           SchemaMigrationRiskLevel risk,
                           List<SchemaPlanPrecondition> preconditions) {
        if (order < 0) {
            throw new IllegalArgumentException("schema plan step order must not be negative");
        }
        this.order = order;
        this.operation = Objects.requireNonNull(operation, "schema plan operation must not be null");
        this.request = request;
        this.risk = Objects.requireNonNull(risk, "schema plan risk must not be null");
        this.preconditions = List.copyOf(Objects.requireNonNull(
                preconditions, "schema plan preconditions must not be null"));
        if (this.preconditions.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("schema plan preconditions must not contain null");
        }
        if (request != null && !request.parameters().isEmpty()) {
            throw new IllegalArgumentException("reviewed schema SQL must not contain bound parameters");
        }
    }

    public static SchemaPlanStep executable(int order,
                                            SchemaOperation operation,
                                            SqlRequest request,
                                            SchemaMigrationRiskLevel risk,
                                            List<SchemaPlanPrecondition> preconditions) {
        return new SchemaPlanStep(order, operation,
                                  Objects.requireNonNull(request, "schema plan SQL request must not be null"),
                                  risk, preconditions);
    }

    public static SchemaPlanStep manual(int order,
                                        SchemaOperation operation,
                                        SchemaMigrationRiskLevel risk,
                                        List<SchemaPlanPrecondition> preconditions) {
        return new SchemaPlanStep(order, operation, null, risk, preconditions);
    }

    public int order() {
        return order;
    }

    public SchemaOperation operation() {
        return operation;
    }

    public Optional<SqlRequest> request() {
        return Optional.ofNullable(request);
    }

    public boolean executable() {
        return request != null;
    }

    public SchemaMigrationRiskLevel risk() {
        return risk;
    }

    public List<SchemaPlanPrecondition> preconditions() {
        return preconditions;
    }
}
