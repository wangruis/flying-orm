package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionPackage;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

/**
 * 负责参数条件命名包分页。
 *
 * <p>具体行为从原客户端原样迁移，SQL、参数、Scope、执行保护和响应式订阅语义不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormParameterPageOperations extends ReactiveFormOperationSupport {

    private final ReactiveFormStructuredPageOperations structuredPages;

    ReactiveFormParameterPageOperations(ReactiveFormOperationSupport runtime,
                                        ReactiveFormStructuredPageOperations structuredPages) {
        super(runtime);
        this.structuredPages = Objects.requireNonNull(structuredPages, "structured page operations must not be null");
    }
    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ParameterConditionPackage conditionPackage,
                                                      Map<String, ?> parameters,
                                                      PageQuery page) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return structuredPages.page(form, compiler, parameters, page);
    }

    Mono<PageResult<DynamicRow>> page(DynamicForm form,
                                                      ParameterConditionPackage conditionPackage,
                                                      Map<String, ?> parameters,
                                                      PageQuery page,
                                                      SqlExecutionOptions options) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return structuredPages.page(form, compiler, parameters, page, options);
    }

    /**
     * 使用参数条件命名包分页查询动态表单数据并映射成业务对象。
     *
     * @param form             动态表单
     * @param conditionPackage 参数条件命名包
     * @param parameters       请求参数
     * @param page             分页请求
     * @param type             目标对象类型
     * @param <T>              目标对象类型
     * @return 分页结果
     */
    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ParameterConditionPackage conditionPackage,
                                        Map<String, ?> parameters,
                                        PageQuery page,
                                        Class<T> type) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return structuredPages.page(form, compiler, parameters, page, type);
    }

    <T> Mono<PageResult<T>> page(DynamicForm form,
                                        ParameterConditionPackage conditionPackage,
                                        Map<String, ?> parameters,
                                        PageQuery page,
                                        Class<T> type,
                                        SqlExecutionOptions options) {
        ParameterConditionCompiler compiler = scopes.parameterCompiler(conditionPackage);
        return structuredPages.page(form, compiler, parameters, page, type, options);
    }

    /**
     * 更新动态表单数据。
     *
     * @param form   动态表单
     * @param values 字段值
     * @param where  更新条件
     * @return 影响行数
     */
}
