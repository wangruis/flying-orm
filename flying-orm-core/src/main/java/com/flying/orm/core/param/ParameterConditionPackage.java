package com.flying.orm.core.param;

import com.flying.orm.core.internal.Names;

import com.flying.orm.core.condition.TermRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 参数条件命名包承载一组可复用请求参数映射规则，便于业务按领域一次性装配动态条件。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public interface ParameterConditionPackage {

    /**
     * 返回命名包名称，用于诊断、日志和后续配置化装配。
     *
     * @return 命名包名称
     */
    String name();

    /**
     * 返回命名包内的参数条件规则，只读且发布后不可变。
     *
     * @return 参数条件规则集合
     */
    List<ParameterConditionSpec> specs();

    /**
     * 返回这个参数包使用的业务 term 元数据。参数映射和 term 形状放在同一个包里，调用方不用重复配置。
     *
     * @return 只读 term 注册表
     */
    TermRegistry terms();

    /**
     * 创建参数条件命名包。
     *
     * @param name  命名包名称
     * @param specs 参数条件规则数组
     * @return 参数条件命名包
     */
    static ParameterConditionPackage of(String name, ParameterConditionSpec... specs) {
        Objects.requireNonNull(specs, "parameter condition specs must not be null");
        return of(name, TermRegistry.empty(), Arrays.asList(specs));
    }

    /**
     * 创建同时携带参数映射和业务 term 元数据的命名包。
     */
    static ParameterConditionPackage of(String name,
                                        TermRegistry terms,
                                        ParameterConditionSpec... specs) {
        Objects.requireNonNull(specs, "parameter condition specs must not be null");
        return of(name, terms, Arrays.asList(specs));
    }

    /**
     * 创建参数条件命名包。
     *
     * @param name  命名包名称
     * @param specs 参数条件规则集合
     * @return 参数条件命名包
     */
    static ParameterConditionPackage of(String name, Iterable<? extends ParameterConditionSpec> specs) {
        return of(name, TermRegistry.empty(), specs);
    }

    /**
     * 创建同时携带参数映射和业务 term 元数据的命名包。
     */
    static ParameterConditionPackage of(String name,
                                        TermRegistry terms,
                                        Iterable<? extends ParameterConditionSpec> specs) {
        List<ParameterConditionSpec> copiedSpecs = new ArrayList<>();
        for (ParameterConditionSpec spec : Objects.requireNonNull(specs, "parameter condition specs must not be null")) {
            copiedSpecs.add(Objects.requireNonNull(spec, "parameter condition spec must not be null"));
        }
        return new SimpleParameterConditionPackage(name,
                                                   copiedSpecs,
                                                   Objects.requireNonNull(
                                                           terms,
                                                           "parameter condition terms must not be null"));
    }
}

/**
 * 简单参数条件命名包实现，负责名称规范化和规则集合冻结。
 *
 * @param name  命名包名称
 * @param specs 参数条件规则集合
 * @param terms 参数包用到的业务 term 元数据；没有自定义 term 时为空注册表
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
record SimpleParameterConditionPackage(String name,
                                       List<ParameterConditionSpec> specs,
                                       TermRegistry terms)
        implements ParameterConditionPackage {

    /**
     * 创建简单参数条件命名包。
     */
    SimpleParameterConditionPackage {
        name = Names.key(name, "parameter condition package name");
        specs = Objects.requireNonNull(specs, "parameter condition package specs must not be null");
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("parameter condition package specs must not be empty");
        }
        specs = Collections.unmodifiableList(specs);
        terms = Objects.requireNonNull(terms, "parameter condition terms must not be null");
    }
}
