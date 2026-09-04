package com.flying.orm.rdb.form;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.aggregate.AggregateResultDecoder;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.FormAggregatePlanner;
import com.flying.orm.rdb.form.spec.QuerySpec;
import reactor.core.publisher.Flux;

import java.util.Objects;

/** 聚合专用响应式协作者；普通 CRUD 客户端不会为未调用的可选能力创建对象。 */
final class ReactiveFormAggregateOperations {

    private ReactiveFormAggregateOperations() {
    }

    static Flux<AggregateRow> aggregate(ReactiveFormOperationContext context,
                                        AggregateSpec spec) {
        ReactiveFormOperationContext safeContext = Objects.requireNonNull(
                context, "form operation context must not be null");
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        return requiresProtectedPlanning(safeContext, safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> plan(safeContext, safeSpec))
                                               .flatMapMany(value -> execute(safeContext, value))
                : execute(safeContext, plan(safeContext, safeSpec));
    }

    static FieldUseSnapshot preview(ReactiveFormOperationContext context,
                                    AggregateSpec spec) {
        return plan(Objects.requireNonNull(context, "form operation context must not be null"),
                    Objects.requireNonNull(spec, "aggregate spec must not be null"))
                .fieldUse();
    }

    private static Flux<AggregateRow> execute(ReactiveFormOperationContext context,
                                              FormAggregatePlanner.Plan plan) {
        AggregateResultDecoder decoder = new AggregateResultDecoder(plan);
        return context.executor().query(plan.request(), plan.options()).map(decoder::decode);
    }

    private static FormAggregatePlanner.Plan plan(ReactiveFormOperationContext context,
                                                  AggregateSpec spec) {
        return new FormAggregatePlanner(
                context.renderer(), context.structuredConditionResolver(), context.defaultDataScope(),
                context.defaultExecutionOptions(), context.fieldUsePolicy(), context.queryShapeLimits())
                .plan(spec);
    }

    private static boolean requiresProtectedPlanning(ReactiveFormOperationContext context,
                                                     AggregateSpec spec) {
        QuerySpec query = spec.query();
        if (query.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = context.defaultDataScope().and(query.scope());
        return ReactiveProtectionCpuBoundary.usesEncryptedCondition(query.form(), query.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(query.form(), effectiveScope)
                || query.structuredInput().isPresent();
    }
}
