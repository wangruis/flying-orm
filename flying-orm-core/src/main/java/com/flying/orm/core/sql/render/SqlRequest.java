package com.flying.orm.core.sql.render;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * SqlRequest 是渲染后的可执行请求，包含参数化 SQL 文本和按占位符顺序排列的参数。
 *
 * @param sql        参数化 SQL 文本
 * @param parameters     SQL 参数集合
 * @param bindMarkerStyle 参数标记的来源
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record SqlRequest(String sql, List<Object> parameters, SqlBindMarkerStyle bindMarkerStyle) {

    /**
     * 创建使用 flying-orm 统一参数标记的请求。
     *
     * @param sql        参数化 SQL 文本
     * @param parameters SQL 参数集合
     */
    public SqlRequest(String sql, List<Object> parameters) {
        this(sql, parameters, SqlBindMarkerStyle.CANONICAL);
    }

    /**
     * 创建数据库原生 SQL 请求。执行器不会改写里面的参数标记或运算符。
     * SQL 文本必须来自服务端代码或可信配置，不能直接使用前端、表单值或请求参数；动态值仍然放进 parameters 绑定。
     *
     * @param sql        数据库原生 SQL
     * @param parameters SQL 参数集合
     * @return 原生 SQL 请求
     */
    public static SqlRequest nativeSql(String sql, List<Object> parameters) {
        return new SqlRequest(sql, parameters, SqlBindMarkerStyle.NATIVE);
    }

    /**
     * 创建 SQL 请求并发布只读参数集合。
     *
     * @param sql             参数化 SQL 文本
     * @param parameters      SQL 参数集合
     * @param bindMarkerStyle 参数标记的来源
     */
    public SqlRequest {
        sql = RenderNames.requireText(sql, "sql request");
        List<Object> bindableParameters = new ArrayList<>();
        Objects.requireNonNull(parameters, "sql parameters must not be null")
               .stream()
               .map(SqlFragment::bindableParameter)
               .forEach(bindableParameters::add);
        parameters = Collections.unmodifiableList(bindableParameters);
        bindMarkerStyle = Objects.requireNonNull(bindMarkerStyle, "sql bind marker style must not be null");
    }
}
