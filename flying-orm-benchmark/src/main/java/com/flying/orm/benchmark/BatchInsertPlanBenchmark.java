package com.flying.orm.benchmark;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 测动态表单批量插入计划和参数布局整理的开销。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BatchInsertPlanBenchmark {

    private FormDataSqlRenderer renderer;

    private DynamicForm form;

    private Map<String, Object> firstRow;

    private List<Map<String, Object>> rows;

    /**
     * 准备一个接近动态表单真实场景的小表。
     */
    @Setup(Level.Trial)
    public void setUp() {
        renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(), RdbDialect.h2());
        form = DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .addField(DynamicField.of("amount", "DECIMAL"))
                          .addField(DynamicField.of("enabled", "BOOLEAN"))
                          .addField(DynamicField.of("createdAt", "TIMESTAMP"))
                          .build();
        firstRow = row(1L, "Alice", 18, "10.00", true, "2026-07-24T10:00:00");
        rows = new ArrayList<>(64);
        for (int i = 0; i < 64; i++) {
            rows.add(row((long) i, "user-" + i, 18 + i, "10." + i, i % 2 == 0, "2026-07-24T10:00:00"));
        }
    }

    /**
     * 测单行场景下的 SQL 计划创建。
     *
     * @return SQL 请求
     */
    @Benchmark
    public BatchWriteRequest compileInsertPlan() {
        return renderer.insertBatch(form, List.of(firstRow));
    }

    /**
     * 测固定字段布局下整理一批参数的开销。
     *
     * @return 批量 SQL 请求
     */
    @Benchmark
    public BatchWriteRequest mapBatchRows() {
        return renderer.insertBatch(form, rows);
    }

    @Benchmark
    public BatchWriteRequest compileUpsertPlan() {
        return renderer.upsertBatch(form, List.of(firstRow));
    }

    @Benchmark
    public BatchWriteRequest mapUpsertRows() {
        return renderer.upsertBatch(form, rows);
    }

    private static Map<String, Object> row(long id,
                                           String name,
                                           int age,
                                           String amount,
                                           boolean enabled,
                                           String createdAt) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("age", age);
        values.put("amount", amount);
        values.put("enabled", enabled);
        values.put("createdAt", createdAt);
        return values;
    }
}
