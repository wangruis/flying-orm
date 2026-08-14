package com.flying.orm.benchmark;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 测前端结构化条件编译为内部条件 AST 的开销。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StructuredConditionBenchmark {

    private StructuredConditionCompiler compiler;

    private DynamicForm form;

    private StructuredConditionInput input;

    /**
     * 准备一个常见列表查询条件。
     */
    @Setup(Level.Trial)
    public void setUp() {
        compiler = StructuredConditionCompiler.create();
        form = DynamicForm.builder("userForm", "Users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("age", "INTEGER"))
                          .addField(DynamicField.of("status", "VARCHAR"))
                          .addField(DynamicField.of("orgId", "BIGINT"))
                          .build();
        input = StructuredConditionInput.and(
                StructuredConditionInput.term("status", "eq", "enabled"),
                StructuredConditionInput.term("age", "gt", 18),
                StructuredConditionInput.or(
                        StructuredConditionInput.term("name", "like", "wang"),
                        StructuredConditionInput.term("orgId", "in", List.of(1L, 2L, 3L))));
    }

    /**
     * 编译结构化条件。
     *
     * @return 条件 AST
     */
    @Benchmark
    public ConditionGroup compileStructuredConditions() {
        return compiler.compile(form, input);
    }
}
