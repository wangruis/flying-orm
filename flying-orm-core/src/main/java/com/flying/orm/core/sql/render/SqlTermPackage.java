package com.flying.orm.core.sql.render;

import com.flying.orm.core.internal.Names;

import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * SQL term 命名包承载一组可复用业务条件算子，便于应用按领域一次性注册。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public interface SqlTermPackage {

    /**
     * 返回命名包名称，用于日志、诊断和后续配置化装配。
     *
     * @return 命名包名称
     */
    String name();

    /**
     * 返回命名包内的 SQL term handler，只读且发布后不可变。
     *
     * @return SQL term handler 集合
     */
    List<SqlTermHandler> handlers();

    /**
     * 返回由 handler 自动汇总出的值形状元数据，可直接交给 {@code ConditionGroup.and(terms)}。
     */
    TermRegistry terms();

    /**
     * 创建 SQL term 命名包。
     *
     * @param name     命名包名称
     * @param handlers SQL term handler 数组
     * @return SQL term 命名包
     */
    static SqlTermPackage of(String name, SqlTermHandler... handlers) {
        Objects.requireNonNull(handlers, "sql term handlers must not be null");
        return of(name, Arrays.asList(handlers));
    }

    /**
     * 创建 SQL term 命名包。
     *
     * @param name     命名包名称
     * @param handlers SQL term handler 集合
     * @return SQL term 命名包
     */
    static SqlTermPackage of(String name, Iterable<? extends SqlTermHandler> handlers) {
        List<SqlTermHandler> copiedHandlers = new ArrayList<>();
        for (SqlTermHandler handler : Objects.requireNonNull(handlers, "sql term handlers must not be null")) {
            copiedHandlers.add(Objects.requireNonNull(handler, "sql term handler must not be null"));
        }
        TermRegistry.Builder terms = TermRegistry.builder();
        for (SqlTermHandler handler : copiedHandlers) {
            terms.add(handler.descriptor()
                             .<TermHandler>map(descriptor -> TermHandler.described(descriptor, handler.shape()))
                             .orElseGet(() -> TermHandler.simple(handler.id(), handler.shape())));
        }
        return new SimpleSqlTermPackage(name, copiedHandlers, terms.build());
    }
}

/**
 * 简单 SQL term 命名包实现，负责名称规范化和 handler 集合冻结。
 *
 * @param name     命名包名称
 * @param handlers SQL term handler 集合
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
record SimpleSqlTermPackage(String name,
                            List<SqlTermHandler> handlers,
                            TermRegistry terms) implements SqlTermPackage {

    /**
     * 创建简单 SQL term 命名包。
     */
    SimpleSqlTermPackage {
        name = Names.key(name, "sql term package name");
        handlers = Objects.requireNonNull(handlers, "sql term package handlers must not be null");
        if (handlers.isEmpty()) {
            throw new IllegalArgumentException("sql term package handlers must not be empty");
        }
        handlers = Collections.unmodifiableList(handlers);
        terms = Objects.requireNonNull(terms, "sql term package terms must not be null");
    }
}
