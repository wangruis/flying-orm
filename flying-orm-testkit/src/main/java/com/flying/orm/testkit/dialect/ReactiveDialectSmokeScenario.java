package com.flying.orm.testkit.dialect;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 真实数据库兼容测试复用这一条小链路：建表、插入、upsert、分页、删除。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public final class ReactiveDialectSmokeScenario {

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Z][A-Z0-9_]{0,62}");

    private final String tableName;

    private ReactiveDialectSmokeScenario(String tableName) {
        this.tableName = validateTableName(tableName);
    }

    public static ReactiveDialectSmokeScenario forTable(String tableName) {
        return new ReactiveDialectSmokeScenario(tableName);
    }

    public Mono<ReactiveDialectSmokeResult> run(ReactiveSqlExecutor executor, RdbDialect dialect) {
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        DynamicForm form = form();
        ReactiveFormClient client = ReactiveFormClient.create(
                safeExecutor, FormDataSqlRenderer.create(renderer(), safeDialect));

        Mono<Void> createTable = Flux.fromIterable(createTableRequests(safeDialect))
                                     .concatMap(safeExecutor::rowsUpdated)
                                     .then();

        return createTable.then(client.insert(WriteSpec.insert(
                                  form, row("ID", "u1", "NAME", "Alice", "AGE", 18))))
                          .flatMap(inserted -> client.writeBatch(BatchSpec.upsert(
                                  form,
                                  Flux.fromIterable(List.of(row("ID", "u1",
                                                                              "NAME", "Alice 2",
                                                                              "AGE", 19),
                                                                          row("ID", "u2",
                                                                              "NAME", "Bob",
                                                                              "AGE", 20)))))
                                                     .map(upserted -> new Counts(inserted,
                                                                                 upserted.affectedRows())))
                          .flatMap(counts -> client.page(QuerySpec.of(
                                                         form,
                                                         ConditionGroup.and()
                                                                       .where("NAME", "=", "Alice 2")
                                                                       .build()),
                                                         PageQuery.of(1, 10, PageSort.asc("ID")))
                                                   .map(page -> new PageStep(counts, page)))
                          .flatMap(step -> client.delete(WriteSpec.delete(
                                                         form,
                                                         ConditionGroup.and()
                                                                       .where("ID", "=", "u2")
                                                                       .build()))
                                                 .map(deleted -> new DeleteStep(step, deleted)))
                          .flatMap(step -> client.select(QuerySpec.of(form, ConditionGroup.and().build()))
                                                 .collectList()
                                                 .map(rows -> new ReactiveDialectSmokeResult(
                                                         step.pageStep().counts().inserted(),
                                                         step.pageStep().counts().upserted(),
                                                         step.pageStep().page(),
                                                         step.deleted(),
                                                         rows)));
    }

    public List<SqlRequest> createTableRequests(RdbDialect dialect) {
        return FormSchemaSqlRenderer.create(Objects.requireNonNull(dialect, "rdb dialect must not be null"))
                                    .createTable(form());
    }

    private DynamicForm form() {
        return DynamicForm.builder("dialectSmoke", tableName)
                          .addField(DynamicField.primaryKey("ID", "VARCHAR"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .addField(DynamicField.of("AGE", "INTEGER"))
                          .build();
    }

    private static SqlRenderer renderer() {
        return SqlRenderer.builder()
                          .addTerm(SqlTermHandler.equalsTo())
                          .build();
    }

    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], pairs[i + 1]);
        }
        return values;
    }

    private static String validateTableName(String tableName) {
        String safeTableName = Objects.requireNonNull(tableName, "table name must not be null");
        if (!SAFE_TABLE_NAME.matcher(safeTableName).matches()) {
            throw new IllegalArgumentException("table name must start with A-Z and contain only A-Z, 0-9 or underscore");
        }
        return safeTableName;
    }

    private record Counts(long inserted, long upserted) {
    }

    private record PageStep(Counts counts, com.flying.orm.core.page.PageResult<DynamicRow> page) {
    }

    private record DeleteStep(PageStep pageStep, long deleted) {
    }
}
