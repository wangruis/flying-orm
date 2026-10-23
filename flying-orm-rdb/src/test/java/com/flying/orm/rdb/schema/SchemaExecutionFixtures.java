package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class SchemaExecutionFixtures {

    static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    static final ColumnDefinition ID = ColumnDefinition.builder("id", "BIGINT")
            .nullable(false)
            .build();
    static final ColumnDefinition NOTE = ColumnDefinition.builder("note", "VARCHAR")
            .length(80)
            .build();
    static final ColumnDefinition TAG = ColumnDefinition.builder("tag", "VARCHAR")
            .length(40)
            .build();

    private SchemaExecutionFixtures() {
    }

    static SchemaMigrationResult executeJdbc(SchemaMigrationPlan plan, SyncSqlExecutor executor,
                                              Runnable invalidator, SqlExecutionOptions options) {
        return new JdbcSchemaMigrationExecutor(executor, SchemaMigrationObserver.noop())
                .execute(plan, SchemaMigrationSupport.metadataRelations(plan), ignored -> invalidator.run(), options);
    }

    static Mono<SchemaMigrationResult> executeReactive(SchemaMigrationPlan plan, ReactiveSqlExecutor executor,
                                                      Runnable invalidator, SqlExecutionOptions options) {
        return new SchemaMigrationExecutor(executor, SchemaMigrationObserver.noop())
                .execute(plan, SchemaMigrationSupport.metadataRelations(plan), ignored -> invalidator.run(), options);
    }

    static RelationalTableDefinition table(ColumnDefinition... columns) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(ACCOUNTS);
        for (ColumnDefinition column : columns) {
            table.addColumn(column);
        }
        return table.build();
    }

    static SchemaMigrationPlan plan(SchemaSnapshot before,
                                   RelationalTableDefinition desired,
                                   SqlRequest... requests) {
        SchemaMigrationPlan.Builder plan = SchemaMigrationPlan.builder(DatabaseDescriptor.of(
                        "H2", "2.3", RdbDialect.h2()))
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(desired);
        int existingColumns = before.tableState() == SchemaSnapshot.State.PRESENT
                ? before.columns().value().size() : 0;
        for (int index = 0; index < requests.length; index++) {
            ColumnDefinition column = desired.columns().get(existingColumns + index);
            SchemaOperation operation = SchemaOperation.of(
                    SchemaOperation.Kind.ADD_COLUMN,
                    desired.identity(),
                    column.name(),
                    null,
                    column,
                    SchemaOperation.Compatibility.SAFE_INCREMENTAL);
            plan.addStep(new SchemaPlanStep(operation, List.of(requests[index])));
        }
        return plan.build();
    }

    static final class SyncExecutor implements com.flying.orm.rdb.support.ListQueryFixture {

        private final Function<SqlRequest, Long> behavior;
        final List<SqlRequest> requests = new ArrayList<>();

        SyncExecutor(Function<SqlRequest, Long> behavior) {
            this.behavior = behavior;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            requests.add(request);
            return behavior.apply(request);
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new UnsupportedOperationException();
        }
    }

    static final class ReactiveExecutor implements ReactiveSqlExecutor {

        private final Function<SqlRequest, Mono<Long>> behavior;
        final List<SqlRequest> requests = new ArrayList<>();

        ReactiveExecutor(Function<SqlRequest, Mono<Long>> behavior) {
            this.behavior = behavior;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            return behavior.apply(request);
        }
    }
}
