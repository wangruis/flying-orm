package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

/**
 * 负责前端结构化条件和参数编译器分页。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormStructuredPageOperations extends ReactiveFormOperationSupport {

    private final ReactiveFormPlainPageOperations plainPages;

    ReactiveFormStructuredPageOperations(ReactiveFormOperationSupport runtime,
                                         ReactiveFormPlainPageOperations plainPages) {
        super(runtime);
        this.plainPages = Objects.requireNonNull(plainPages, "plain page operations must not be null");
    }
    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      StructuredConditionInput input,
                                                      StructuredConditionPolicy policy,
                                                      PageQuery page) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedStructuredRead(form, input, policy);
        return results.pageWithActiveWhere(read.form(), read.where(), safePage);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      StructuredConditionInput input,
                                                      StructuredConditionPolicy policy,
                                                      PageQuery page,
                                                      SqlExecutionOptions options) {
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        ScopedRead read = scopes.scopedStructuredRead(form, input, policy);
        return results.pageWithActiveWhere(read.form(), read.where(), safePage, options);
    }

    /**
     * 使用前端结构化条件分页查询动态表单数据并映射成业务对象。
     *
     * @param form   动态表单
     * @param input  前端结构化条件
     * @param policy 安全策略
     * @param page   分页请求
     * @param type   目标对象类型
     * @param <T>    目标对象类型
     * @return 分页结果
     */
    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        StructuredConditionInput input,
                                        StructuredConditionPolicy policy,
                                        PageQuery page,
                                        Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, input, policy, page)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        StructuredConditionInput input,
                                        StructuredConditionPolicy policy,
                                        PageQuery page,
                                        Class<T> type,
                                        SqlExecutionOptions options) {
        RowMapper<T> mapper = results.rowMapper(type, "form page result type must not be null");
        return page(form, input, policy, page, options)
                .map(result -> new PageResult<>(result.rows().stream().map(mapper::map).toList(),
                                                result.total(),
                                                result.page(),
                                                result.size()));
    }

    /**
     * 使用参数编译器分页查询动态表单数据。
     *
     * @param form       动态表单
     * @param compiler   参数条件编译器
     * @param parameters 请求参数
     * @param page       分页请求
     * @return 分页结果
     */
    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ParameterConditionCompiler compiler,
                                                      Map<String, ?> parameters,
                                                      PageQuery page) {
        return plainPages.page(form,
                               Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                      .compile(parameters),
                               page);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ParameterConditionCompiler compiler,
                                                      Map<String, ?> parameters,
                                                      PageQuery page,
                                                      SqlExecutionOptions options) {
        return plainPages.page(form,
                               Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                      .compile(parameters),
                               page,
                               options);
    }

    /**
     * 使用参数编译器分页查询动态表单数据并映射成业务对象。
     *
     * @param form       动态表单
     * @param compiler   参数条件编译器
     * @param parameters 请求参数
     * @param page       分页请求
     * @param type       目标对象类型
     * @param <T>        目标对象类型
     * @return 分页结果
     */
    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ParameterConditionCompiler compiler,
                                        Map<String, ?> parameters,
                                        PageQuery page,
                                        Class<T> type) {
        return plainPages.page(form,
                               Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                      .compile(parameters),
                               page,
                               type);
    }

    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ParameterConditionCompiler compiler,
                                        Map<String, ?> parameters,
                                        PageQuery page,
                                        Class<T> type,
                                        SqlExecutionOptions options) {
        return plainPages.page(form,
                               Objects.requireNonNull(compiler, "parameter condition compiler must not be null")
                                      .compile(parameters),
                               page,
                               type,
                               options);
    }

    /**
     * 使用参数条件命名包分页查询动态表单数据。
     *
     * @param form             动态表单
     * @param conditionPackage 参数条件命名包
     * @param parameters       请求参数
     * @param page             分页请求
     * @return 分页结果
     */
}
