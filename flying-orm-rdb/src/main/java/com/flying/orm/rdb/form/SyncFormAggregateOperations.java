package com.flying.orm.rdb.form;

import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.aggregate.AggregateResultDecoder;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.FormAggregatePlanner;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;

/** 原生 JDBC 聚合协作者；同步门面只转发规格和共享计划。 */
final class SyncFormAggregateOperations {

    private SyncFormAggregateOperations() {
    }

    static List<AggregateRow> aggregate(SyncSqlExecutor executor,
                                        SyncFormConfiguration configuration,
                                        AggregateSpec spec) {
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(
                executor, "sync SQL executor must not be null");
        FormAggregatePlanner.Plan plan = plan(configuration, spec);
        AggregateResultDecoder decoder = new AggregateResultDecoder(plan);
        return safeExecutor.queryMapped(plan.request(), plan.options(), decoder::decode, 0);
    }

    static FieldUseSnapshot preview(SyncFormConfiguration configuration,
                                    AggregateSpec spec) {
        return plan(configuration, spec).fieldUse();
    }

    private static FormAggregatePlanner.Plan plan(SyncFormConfiguration configuration,
                                                  AggregateSpec spec) {
        SyncFormConfiguration safeConfiguration = Objects.requireNonNull(
                configuration, "sync form configuration must not be null");
        return new FormAggregatePlanner(
                safeConfiguration.renderer(), safeConfiguration.resolver(), safeConfiguration.dataScope(),
                safeConfiguration.executionOptions(), safeConfiguration.fieldUsePolicy(),
                safeConfiguration.queryShapeLimits())
                .plan(Objects.requireNonNull(spec, "aggregate spec must not be null"));
    }
}
