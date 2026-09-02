package com.flying.orm.core.sql.render;

import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.internal.value.OwnedBindableValues;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 渲染后的不可变执行请求：一个可复用 SQL 结构计划和本次调用严格有序的参数值。
 *
 * <p>SQL 结构与业务值分离后，同形状请求可以复用已校验、已完成驱动占位符转换的计划；执行器只绑定当前值。
 * 参数列表含可变容器时，具体执行边界仍按类型策略做一次必要快照。</p>
 *
 * @param statement  可复用 SQL 结构计划
 * @param parameters 本次调用的有序参数
 * @author wangr
 * @date 2026-07-21
 * @version v3.1
 */
public record SqlRequest(SqlStatementPlan statement, List<Object> parameters) {

    /** 创建使用 flying-orm 统一问号参数标记的请求。 */
    public SqlRequest(String sql, List<Object> parameters) {
        this(sql, parameters, SqlBindMarkerStyle.CANONICAL);
    }

    /** 创建尚未针对具体驱动编译传输 SQL 的请求。 */
    public SqlRequest(String sql, List<Object> parameters, SqlBindMarkerStyle bindMarkerStyle) {
        this(SqlStatementPlan.canonical(
                     sql,
                     bindMarkerStyle,
                     Objects.requireNonNull(parameters, "sql parameters must not be null").size()),
             parameters);
    }

    /**
     * 创建数据库原生 SQL 请求。SQL 文本必须来自服务端代码或可信配置；动态业务值仍必须参数化绑定。
     */
    public static SqlRequest nativeSql(String sql, List<Object> parameters) {
        return new SqlRequest(sql, parameters, SqlBindMarkerStyle.NATIVE);
    }

    public SqlRequest {
        statement = Objects.requireNonNull(statement, "SQL statement plan must not be null");
        List<Object> source = Objects.requireNonNull(parameters, "sql parameters must not be null");
        if (statement.parameterCount() != source.size()) {
            throw new IllegalArgumentException(
                    "SQL statement parameter count does not match request values");
        }
        parameters = immutableBindableParameters(source);
    }

    public String sql() {
        return statement.sql();
    }

    public SqlBindMarkerStyle bindMarkerStyle() {
        return statement.bindMarkerStyle();
    }

    private static List<Object> immutableBindableParameters(List<Object> source) {
        if (OwnedBindableValues.isPublished(source)
                && !OwnedBindableValues.requiresImmutableSnapshot(source)) {
            return source;
        }
        List<Object> copied = null;
        int index = 0;
        for (Iterator<?> iterator = source.iterator(); iterator.hasNext(); index++) {
            Object parameter = iterator.next();
            Object bindable = SqlFragment.bindableParameter(parameter);
            if (requiresMutableSnapshot(bindable)) {
                return BindableValueSnapshots.immutableValues(new BindableParameters(source));
            }
            if (copied == null && parameter != null && bindable == parameter) {
                continue;
            }
            if (copied == null) {
                copied = new ArrayList<>(source.size());
                for (Object value : source.subList(0, index)) {
                    copied.add(SqlFragment.bindableParameter(value));
                }
            }
            copied.add(bindable);
        }
        return copied == null ? List.copyOf(source) : Collections.unmodifiableList(copied);
    }

    private static boolean requiresMutableSnapshot(Object value) {
        return BindableValueSnapshots.requiresImmutableSnapshot(value);
    }

    /** 只在最终请求边界解开渲染期参数包装；共享 snapshot session 保留同一值的引用关系。 */
    private static final class BindableParameters extends AbstractList<Object> {

        private final List<Object> parameters;

        private BindableParameters(List<Object> parameters) {
            this.parameters = parameters;
        }

        @Override
        public Object get(int index) {
            return SqlFragment.bindableParameter(parameters.get(index));
        }

        @Override
        public Iterator<Object> iterator() {
            Iterator<?> source = parameters.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return source.hasNext();
                }

                @Override
                public Object next() {
                    return SqlFragment.bindableParameter(source.next());
                }
            };
        }

        @Override
        public int size() {
            return parameters.size();
        }
    }

}
