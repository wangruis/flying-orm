package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionPackage;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Objects;

/**
 * 负责结构化条件和参数驱动查询。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormStructuredQueryOperations extends ReactiveFormOperationSupport {

    private final ReactiveFormQueryOperations plainQueries;

    ReactiveFormStructuredQueryOperations(ReactiveFormOperationSupport runtime,
                                          ReactiveFormQueryOperations plainQueries) {
        super(runtime);
        this.plainQueries = Objects.requireNonNull(plainQueries, "plain query operations must not be null");
    }
    Flux<DynamicRow> select(DynamicForm form, StructuredConditionInput input) {
        return select(form, input, StructuredConditionPolicy.defaults());
    }

    Flux<DynamicRow> select(DynamicForm form,
                                            StructuredConditionInput input,
                                            SqlExecutionOptions options) {
        return select(form, input, StructuredConditionPolicy.defaults(), options);
    }

    /**
     * 使用前端结构化条件查询动态表单数据并映射成业务对象。
     *
     * @param form  动态表单
     * @param input 前端结构化条件
     * @param type  目标对象类型
     * @param <T>   目标对象类型
     * @return 业务对象流
     */
    <T> Flux<T> select(DynamicForm form, StructuredConditionInput input, Class<T> type) {
        return select(form, input, StructuredConditionPolicy.defaults(), type);
    }

    <T> Flux<T> select(DynamicForm form,
                              StructuredConditionInput input,
                              Class<T> type,
                              SqlExecutionOptions options) {
        return select(form, input, StructuredConditionPolicy.defaults(), type, options);
    }

    /**
     * 查询动态表单数据，直接接收前端结构化条件并使用指定安全策略。
     *
     * @param form   动态表单
     * @param input  前端结构化条件
     * @param policy 安全策略
     * @return 行数据流
     */
    Flux<DynamicRow> select(DynamicForm form,
                                            StructuredConditionInput input,
                                            StructuredConditionPolicy policy) {
        ScopedRead read = scopes.scopedStructuredRead(form, input, policy);
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where())));
    }

    Flux<DynamicRow> select(DynamicForm form,
                                            StructuredConditionInput input,
                                            StructuredConditionPolicy policy,
                                            SqlExecutionOptions options) {
        ScopedRead read = scopes.scopedStructuredRead(form, input, policy);
        return results.decodeRows(read.form(), executor.query(renderer.select(read.form(), read.where()), options), options);
    }

    /**
     * 使用前端结构化条件查询动态表单数据并映射成业务对象。
     *
     * @param form   动态表单
     * @param input  前端结构化条件
     * @param policy 安全策略
     * @param type   目标对象类型
     * @param <T>    目标对象类型
     * @return 业务对象流
     */
    <T> Flux<T> select(DynamicForm form,
                              StructuredConditionInput input,
                              StructuredConditionPolicy policy,
                              Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, input, policy).map(mapper::map);
    }

    <T> Flux<T> select(DynamicForm form,
                              StructuredConditionInput input,
                              StructuredConditionPolicy policy,
                              Class<T> type,
                              SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(form, input, policy, options).map(mapper::map);
    }

    /**
     * 使用参数编译器查询动态表单数据。
     *
     * @param form       动态表单
     * @param compiler   参数条件编译器
     * @param parameters 请求参数
     * @return 行数据流
     */
    Flux<DynamicRow> select(DynamicForm form,
                                            ParameterConditionCompiler compiler,
                                            Map<String, ?> parameters) {
        return plainQueries.select(form,
                                   Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                          .compile(parameters));
    }

    Flux<DynamicRow> select(DynamicForm form,
                                            ParameterConditionCompiler compiler,
                                            Map<String, ?> parameters,
                                            SqlExecutionOptions options) {
        return plainQueries.select(form,
                                   Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                          .compile(parameters),
                                   options);
    }

    /**
     * 使用参数编译器查询动态表单数据并映射成业务对象。
     *
     * @param form       动态表单
     * @param compiler   参数条件编译器
     * @param parameters 请求参数
     * @param type       目标对象类型
     * @param <T>        目标对象类型
     * @return 业务对象流
     */
    <T> Flux<T> select(DynamicForm form,
                              ParameterConditionCompiler compiler,
                              Map<String, ?> parameters,
                              Class<T> type) {
        return plainQueries.select(form,
                                   Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                          .compile(parameters),
                                   type);
    }

    <T> Flux<T> select(DynamicForm form,
                              ParameterConditionCompiler compiler,
                              Map<String, ?> parameters,
                              Class<T> type,
                              SqlExecutionOptions options) {
        return plainQueries.select(form,
                                   Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                          .compile(parameters),
                                   type,
                                   options);
    }

    /**
     * 使用参数条件命名包查询动态表单数据，适合数据权限等可复用参数映射场景。
     *
     * @param form             动态表单
     * @param conditionPackage 参数条件命名包
     * @param parameters       请求参数
     * @return 行数据流
     */
    Flux<DynamicRow> select(DynamicForm form,
                                            ParameterConditionPackage conditionPackage,
                                            Map<String, ?> parameters) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return select(form, compiler, parameters);
    }

    Flux<DynamicRow> select(DynamicForm form,
                                            ParameterConditionPackage conditionPackage,
                                            Map<String, ?> parameters,
                                            SqlExecutionOptions options) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return select(form, compiler, parameters, options);
    }

    /**
     * 使用参数条件命名包查询动态表单数据并映射成业务对象。
     *
     * @param form             动态表单
     * @param conditionPackage 参数条件命名包
     * @param parameters       请求参数
     * @param type             目标对象类型
     * @param <T>              目标对象类型
     * @return 业务对象流
     */
    <T> Flux<T> select(DynamicForm form,
                              ParameterConditionPackage conditionPackage,
                              Map<String, ?> parameters,
                              Class<T> type) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return select(form, compiler, parameters, type);
    }

    <T> Flux<T> select(DynamicForm form,
                              ParameterConditionPackage conditionPackage,
                              Map<String, ?> parameters,
                              Class<T> type,
                              SqlExecutionOptions options) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return select(form, compiler, parameters, type, options);
    }

    /**
     * 分页查询动态表单数据。
     *
     * @param form  动态表单
     * @param where 查询条件
     * @param page  分页请求
     * @return 分页结果
     */
}
