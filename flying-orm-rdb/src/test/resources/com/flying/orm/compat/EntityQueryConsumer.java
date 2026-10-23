package consumer;

import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.operator.DmlOperator;
import com.flying.orm.rdb.operator.SyncDmlOperator;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.GroupSelection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/** Compiled as a library consumer, outside every implementation package. */
public final class EntityQueryConsumer {
    public List<User> syncEntityEntry(SyncDmlOperator sync, Long id) {
        return sync.entity(User.class).query().where(User::getId, id).fetch();
    }

    public com.flying.orm.rdb.operator.SyncSqlTemplateOperator syncTemplateView(
            com.flying.orm.rdb.operator.SyncDatabaseOperator sync,
            com.flying.orm.rdb.template.SqlTemplateRegistry registry,
            com.flying.orm.rdb.template.SyncSqlTemplateParameterProvider parameters,
            String templateId) {
        return sync.withSqlTemplates(registry, parameters).sqlTemplate(templateId);
    }

    public Flux<RankedSummary> mappedReport(
            com.flying.orm.rdb.operator.SqlTemplateOperator reactive,
            com.flying.orm.rdb.operator.SyncSqlTemplateOperator sync) {
        com.flying.orm.rdb.mapping.RowMapper<RankedSummary> mapper =
                com.flying.orm.rdb.mapping.RowMapper.nested(RankedSummary.class).withAliases(Map.of(
                        "department", "summary.name", "orders", "summary.count",
                        "gross", "summary.amount", "rank", "position.position"));
        List<RankedSummary> rows = sync.query(mapper);
        return reactive.query(mapper);
    }

    public Mono<Long> fluentDdl(com.flying.orm.rdb.operator.DatabaseOperator reactive,
                                 com.flying.orm.rdb.operator.SyncDatabaseOperator sync) {
        sync.ddl().createOrAlter("notes")
                .addColumn("id", column -> column.number(19).primaryKey())
                .addColumn("body", column -> column.varchar(128)).commit();
        return reactive.ddl().createOrAlter("notes")
                .addColumn("id", column -> column.number(19).primaryKey())
                .addColumn("body", column -> column.varchar(128)).commit();
    }

    public Mono<com.flying.orm.rdb.schema.SchemaMigrationResult> relationalDdl(
            com.flying.orm.rdb.operator.DatabaseOperator reactive,
            com.flying.orm.rdb.operator.SyncDatabaseOperator sync,
            com.flying.orm.rdb.dialect.DatabaseDescriptor database) {
        java.util.function.Consumer<com.flying.orm.core.metadata.RelationalTableDefinition.Builder> table = schema -> schema
                .addColumn("id", "BIGINT", column -> column.nullable(false)
                        .generation(com.flying.orm.core.metadata.ValueGeneration.identity()))
                .addColumn("amount", "DECIMAL", column -> column.precision(18).scale(2)
                        .defaultValue(com.flying.orm.core.metadata.ColumnDefault.literal(0)))
                .primaryKey(com.flying.orm.core.metadata.PrimaryKeyDefinition.of("pk_payments", "id"))
                .addCheck(com.flying.orm.core.metadata.CheckConstraintDefinition.of("ck_amount",
                        com.flying.orm.core.metadata.CheckPredicate.compare("amount",
                                com.flying.orm.core.metadata.CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, 0)));
        sync.ddl().createOrAlter(database, "payments", table);
        return reactive.ddl().createOrAlter(database, "payments", table);
    }

    public Mono<com.flying.orm.rdb.schema.SchemaMigrationResult> qualifiedDdl(
            com.flying.orm.rdb.operator.DatabaseOperator reactive,
            com.flying.orm.rdb.operator.SyncDatabaseOperator sync,
            com.flying.orm.rdb.dialect.DatabaseDescriptor database) {
        com.flying.orm.core.metadata.RelationIdentity identity =
                com.flying.orm.core.metadata.RelationIdentity.of(null, "sales", "notes");
        java.util.function.Consumer<com.flying.orm.core.metadata.RelationalTableDefinition.Builder> table = schema -> schema
                .addColumn("status", "VARCHAR(20)", column -> column
                        .defaultValue(com.flying.orm.core.metadata.ColumnDefault.literal("NEW")));
        sync.ddl().createOrAlter(database, identity, table);
        return reactive.ddl().createOrAlter(database, identity, table);
    }

    public void upsert(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive, User entity) {
        com.flying.orm.rdb.batch.BatchExecutionEvidence written = sync.upsert(entity);
        Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> pending = reactive.upsert(entity);
    }

    public void fullJoins(DmlOperator reactive, SyncDmlOperator sync, DynamicForm users, DynamicForm orders) {
        reactive.joinQuery(users).fullJoin(orders, "id", "user_id")
                .selectAs(users, "id", "user_id").selectAs(orders, "user_id", "order_user_id").executeRows();
        sync.joinQuery(users).fullJoin(orders, "id", "user_id").select(users, "id").executeRows();
        reactive.joinQuery(User.class).fullJoin(Order.class, User::getId, Order::userId)
                .select(User.class, User::getId).executeRows();
        sync.joinQuery(User.class).fullJoin(Order.class, User::getId, Order::userId)
                .select(User.class, User::getId).executeRows();
        reactive.joinQuery(users).fullJoin(orders, (user, order) ->
                com.flying.orm.core.join.JoinCondition.compare(user.field("id"), "=", order.field("user_id")))
                .select(users, "id").executeRows();
        sync.joinQuery(users).fullJoin(orders, (user, order) ->
                com.flying.orm.core.join.JoinCondition.compare(user.field("id"), "=", order.field("user_id")))
                .select(users, "id").executeRows();
        reactive.joinQuery(User.class).fullJoin(Order.class, (user, order) ->
                com.flying.orm.core.join.JoinCondition.compare(user.field(User::getId), "=", order.field(Order::userId)))
                .select(User.class, User::getId).executeRows();
        sync.joinQuery(User.class).fullJoin(Order.class, (user, order) ->
                com.flying.orm.core.join.JoinCondition.compare(user.field(User::getId), "=", order.field(Order::userId)))
                .select(User.class, User::getId).executeRows();
    }

    public void joinedProjection(DmlOperator reactive, SyncDmlOperator sync, DynamicForm users) {
        com.flying.orm.rdb.operator.JoinQueryOperator query = reactive.joinQuery(users);
        query.select(query.field(users, "id").add(com.flying.orm.core.expression.ValueExpression.value(1)), "next_id")
                .executeRows();
        com.flying.orm.rdb.operator.SyncEntityJoinQueryOperator<User> typed = sync.joinQuery(User.class);
        typed.select(typed.field(User.class, User::getId)
                .add(com.flying.orm.core.expression.ValueExpression.value(1)), "next_id").executeRows();
    }

    public void computedProjection(ReactiveFormRepository<User> users, SyncFormRepository<User> syncUsers) {
        users.createQuery().selectExpression(e -> e.field(User::getId), "selected_id")
                .orderByAsc("selected_id").fetch(IdProjection.class);
        syncUsers.createQuery().selectExpression(e -> e.field(User::getId), "selected_id")
                .orderByDesc("selected_id").page(1, 10, IdProjection.class);
    }

    public record IdProjection(Long selected_id) { }

    public void entityCursors(ReactiveFormRepository<User> users, SyncFormRepository<User> syncUsers) {
        KeysetPageQuery cursor = KeysetPageQuery.first(20, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        KeysetPageQuery keyset = KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST));
        Mono<KeysetPageResult<User>> reactiveCursor = users.createQuery().keysetPage(cursor);
        Mono<KeysetPageResult<User>> reactiveKeyset = users.createQuery().keysetPage(keyset);
        KeysetPageResult<User> syncCursor = syncUsers.createQuery().keysetPage(cursor);
        KeysetPageResult<User> syncKeyset = syncUsers.createQuery().keysetPage(keyset);
        users.createQuery().select(User::getId).keysetPage(cursor, User.class);
        syncUsers.createQuery().select(User::getId).keysetPage(cursor, User.class);
        users.createQuery().selectExpression(e -> e.field(User::getId), "selected_id")
                .keysetPage(keyset, IdProjection.class);
        syncUsers.createQuery().selectExpression(e -> e.field(User::getId), "selected_id")
                .keysetPage(keyset, IdProjection.class);
    }

    public void updateExpressions(ReactiveFormRepository<User> users, SyncFormRepository<User> syncUsers,
                                  com.flying.orm.rdb.operator.DatabaseOperator operator,
                                  com.flying.orm.rdb.operator.SyncDatabaseOperator syncOperator,
                                  DynamicForm form) {
        users.createUpdate().setExpression(User::getId, e -> e.field(User::getId)
                .add(com.flying.orm.core.expression.ValueExpression.value(1))).where(User::getId, 1L).execute();
        syncUsers.createUpdate().setExpression(User::getId, e -> e.field(User::getId)
                .add(com.flying.orm.core.expression.ValueExpression.value(1))).where(User::getId, 1L).execute();
        operator.dml().update(form).setExpression("amount", com.flying.orm.core.expression.ValueExpression.field("quantity"))
                .where("id", 1L).execute();
        syncOperator.dml().update(form).setExpression("amount", com.flying.orm.core.expression.ValueExpression.field("quantity"))
                .where("id", 1L).execute();
    }
    public com.flying.orm.core.sql.render.SqlRenderer reportFunctions() {
        com.flying.orm.core.sql.expression.SqlExpressionFunctions functions =
                com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder().register("text-length", call -> {
                    call.requireArity(1);
                    call.sql(call.dialect().equals("sqlserver") ? "len(" : "length(")
                            .argument(0, com.flying.orm.core.type.LogicalType.TEXT).sql(")");
                    return com.flying.orm.core.type.LogicalType.INTEGER;
                }).build();
        return com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().expressionFunctions(functions).build();
    }

    public void computedReports(ReactiveFormRepository<User> users, SyncFormRepository<User> syncUsers) {
        users.createQuery().aggregate(report -> report.sum(report.field(User.class, User::getId)
                .coalesce(com.flying.orm.core.expression.ValueExpression.value(0)), "ids"));
        syncUsers.createQuery().aggregate(report -> report.avg(report.field(User.class, User::getId)
                .add(com.flying.orm.core.expression.ValueExpression.value(2)), "average"));
        users.createQuery().aggregate(report -> {
            com.flying.orm.core.expression.ValueExpression id = report.field(User.class, User::getId)
                    .coalesce(com.flying.orm.core.expression.ValueExpression.value(0L));
            report.count(id, "counted");
            report.countDistinct(id, "distinct_ids");
            report.min(id, "minimum", com.flying.orm.core.type.LogicalType.BIG_INTEGER, Long.class);
            report.max(id, "maximum", com.flying.orm.core.type.LogicalType.BIG_INTEGER, Long.class);
        });
    }

    public <T> com.flying.orm.rdb.mapping.EntitySchemaDescriptor<T> namedFieldMappings(
            Class<T> entityType, Class<?> valueType, com.flying.orm.core.codec.ValueCodec text,
            com.flying.orm.core.codec.ValueCodec numeric) {
        com.flying.orm.rdb.mapping.EntityTypeMappingRegistry mappings =
                com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.builder()
                        .registerNamed("text-value", valueType, com.flying.orm.core.type.DatabaseType.of("VARCHAR"), text)
                        .registerNamed("numeric-value", valueType, com.flying.orm.core.type.DatabaseType.of("INTEGER"), numeric)
                        .build();
        return com.flying.orm.rdb.mapping.EntitySchemaDescriptor.builder(entityType).typeMappings(mappings).build();
    }
    public void entityReports(DmlOperator reactive, SyncDmlOperator sync,
                              ReactiveFormRepository<User> users, SyncFormRepository<User> syncUsers) {
        users.createQuery().where(User::getId, ">", 0L).aggregate(report -> {
            report.group(User.class, User::getName, "name");
            report.countRows("rows");
            report.orderBy(com.flying.orm.core.page.PageSort.desc("rows"));
        });
        syncUsers.createQuery().aggregatePage(report -> report.countRows("rows"), 1, 20);
        reactive.joinQuery(User.class).leftJoin(Order.class, User::getId, Order::userId)
                .aggregatePage(report -> {
                    report.group(User.class, User::getName, "name");
                    report.count(Order.class, Order::userId, "orders");
                    report.having(ConditionGroup.and().where("orders", ">", 0L).build());
                }, 1, 20);
        sync.joinQuery(User.class).leftJoin(Order.class, User::getId, Order::userId)
                .aggregate(report -> report.max(Order.class, Order::status, "latest", String.class));
    }
    public void complexOn(DmlOperator reactive, SyncDmlOperator sync, DynamicForm users, DynamicForm orders) {
        reactive.joinQuery(users).leftJoin(orders, (user, order) -> com.flying.orm.core.join.JoinCondition.and(
                com.flying.orm.core.join.JoinCondition.compare(user.field("id"), "=", order.field("user_id")),
                com.flying.orm.core.join.JoinCondition.where(order, ConditionGroup.and().where("status", "=", "paid").build())))
                .select(users, "id").executeRows();
        sync.joinQuery(User.class).leftJoin(Order.class, (user, order) -> com.flying.orm.core.join.JoinCondition.and(
                com.flying.orm.core.join.JoinCondition.compare(user.field(User::getId), "=", order.field(Order::userId)),
                order.where(where -> where.where(Order::status, "paid"))))
                .select(User.class, User::getId).executeRows();
    }
    public void queryComposition(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive, String name) {
        com.flying.orm.rdb.operator.EntityDmlQueryOperator<User> base = reactive.createQuery()
                .whereIfPresent(User::getName, "like", name).exclude(User::getName);
        Mono<User> first = base.copy().where(User::getId, 1L).first();
        sync.createQuery().whereIfPresent(User::getName, name).exclude(User::getId).copy().fetch();
        sync.createUpdate().set(User::getName, "Ada").whereIfPresent(User::getId, ">", 0L).execute();
        reactive.createDelete().whereIfPresent(User::getId, 1L).execute();
    }
    public void entityAssignments(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive, User entity) {
        sync.createUpdate().set(entity, User::getName).where(User::getId, 1L).execute();
        Mono<Long> changed = reactive.createUpdate().set(entity).where(User::getId, 1L).execute();
        reactive.createUpdate().set(entity, User::getName).set(User::getName, null)
                .where(User::getId, 1L).execute();
    }
    public void entityKeys(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive, User entity) {
        User one = sync.findById(1L);
        Mono<User> keyed = reactive.findById(Map.of("id", 1L));
        long changed = sync.update(entity);
        Mono<Long> updated = reactive.update(entity);
        long deleted = sync.delete(entity);
        Mono<Long> removed = reactive.delete(entity);
        sync.deleteById(1L);
        reactive.deleteById(Map.of("id", 1L));
    }
    public void entityFilters(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive,
                              StructuredConditionInput input) {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().allowOnlyFields(List.of("name"));
        long count = sync.createQuery().filter(input, policy).where(User::getId, 1L).count();
        Mono<User> first = reactive.createQuery().filter(input).orderByAsc(User::getId).first();
        sync.createQuery().filter(input).page(1, 20);
        Flux<User> rows = reactive.createQuery().filter(input, policy).fetch();
    }
    public void dynamicTerminals(DmlOperator reactive, SyncDmlOperator sync, DynamicForm form) {
        reactive.query(form).where("id", 1L).first();
        reactive.query(form).groupBy("name").count();
        sync.query().from("users").select("name").first(User.class);
        sync.query().from("users").count();
        com.flying.orm.rdb.mapping.RowMapper<Long> count = com.flying.orm.rdb.mapping.RowMapper.count();
    }
    public void joinedReports(DmlOperator reactive, SyncDmlOperator sync, DynamicForm customers, DynamicForm orders) {
        reactive.joinQuery(customers).leftJoin(orders, "id", "customer_id")
                .aggregate(report -> report.group(GroupSelection.of("name", "customer"))
                        .aggregate(orders, AggregateExpression.sum("amount", "gross")));
        sync.joinQuery(customers).leftJoin(orders, "id", "customer_id")
                .aggregatePage(report -> report.group(GroupSelection.of("name", "customer"))
                        .aggregate(orders, AggregateExpression.sum("amount", "gross"))
                        .orderBy(com.flying.orm.core.page.PageSort.desc("gross")), 1, 20);
    }

    public void aggregatePages(DmlOperator reactive, SyncDmlOperator sync, DynamicForm form) {
        reactive.query().from(form).where("status", "paid").orderByDesc("gross")
                .aggregatePage(report -> report.group(GroupSelection.of("status", "status"))
                        .aggregate(AggregateExpression.countRows("rows"))
                        .aggregate(AggregateExpression.sum("amount", "gross"))
                        .having(ConditionGroup.and().where("gross", ">", 10).build()), 1, 20);
        sync.query().from(form).where("status", "paid").orderByDesc("gross")
                .aggregatePage(report -> report.group(GroupSelection.of("status", "status"))
                        .aggregate(AggregateExpression.countRows("rows"))
                        .aggregate(AggregateExpression.sum("amount", "gross")), 1, 20);
    }

    public void batchUpdates(DmlOperator reactive, SyncDmlOperator sync, DynamicForm form) {
        ConditionGroup where = ConditionGroup.and().where("id", "=", 1L).build();
        BatchUpdate ordinary = new BatchUpdate(Map.of("name", "Ada"), where);
        BatchUpdate versioned = new BatchUpdate(Map.of("name", "Ada"), where,
                OptimisticLockOptions.increment("version", 3L));
        reactive.updateBatch(form, Flux.just(ordinary));
        sync.updateBatch(form, List.of(ordinary));
        reactive.updateBatch(form, Flux.just(versioned));
        sync.updateBatch(form, List.of(versioned));
    }

    public void replaceConditions(DmlOperator reactive, SyncDmlOperator sync) {
        reactive.query().from("users").where("id", 1).replaceWhere(where -> where.is("id", 2));
        sync.query().from("users").where("id", 1).replaceWhere(where -> where.is("id", 2));
        reactive.update("users").set("name", "Ada").where("id", 1).replaceWhere(where -> where.is("id", 2));
        sync.update("users").set("name", "Ada").where("id", 1).replaceWhere(where -> where.is("id", 2));
        reactive.delete("users").where("id", 1).replaceWhere(where -> where.is("id", 2));
        sync.delete("users").where("id", 1).replaceWhere(where -> where.is("id", 2));
    }

    public void query(SyncFormRepository<User> sync, ReactiveFormRepository<User> reactive) {
        long total = sync.createQuery().where(User::getName, "Ada").count();
        Mono<Long> grouped = reactive.createQuery().groupBy(User::getName).count();
        User first = sync.createQuery().orderByAsc(User::getName).first();
        Mono<User> next = reactive.createQuery().select(User::getName).first();
        sync.createQuery()
                .or(group -> group.andGroup(nested -> nested.notIn(User::getId, List.of(1L)))
                        .or(nested -> nested.where(User::getName, "Ada")))
                .select(User::getName)
                .onLoad(user -> user.name = user.name.trim())
                .page(1, 20);
        reactive.createQuery()
                .andGroup(group -> group.or(nested -> nested.notIn(User::getId, List.of(1L)))
                        .andGroup(nested -> nested.where(User::getName, "Ada")))
                .select(User::getName)
                .onLoad(user -> Mono.fromRunnable(() -> user.name = user.name.trim()))
                .one();
    }

    public static final class User {
        private Long id;
        private String name;
        public Long getId() { return id; }
        public String getName() { return name; }
    }
    public record Order(Long userId, String status) { }
    public record Summary(String name, Long count, java.math.BigDecimal amount) { }
    public record Position(Long position) { }
    public record RankedSummary(Summary summary, Position position) { }
}
