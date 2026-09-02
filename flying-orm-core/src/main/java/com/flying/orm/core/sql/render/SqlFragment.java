package com.flying.orm.core.sql.render;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.internal.value.OwnedBindableValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * SQL 片段保存局部 SQL 文本和对应参数，片段文本只能包含占位符，不能直接拼接业务参数值。
 *
 * @param sql        SQL 片段文本
 * @param parameters 片段参数集合
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public record SqlFragment(String sql, List<Object> parameters) {

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    /**
     * 创建 SQL 片段并发布只读参数集合。
     *
     * @param sql        SQL 片段文本
     * @param parameters 片段参数集合
     */
    public SqlFragment {
        sql = Objects.requireNonNull(sql, "sql fragment must not be null").trim();
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql parameters must not be null");
        parameters = OwnedBindableValues.isPublished(safeParameters)
                ? safeParameters : Collections.unmodifiableList(copyBindableParameters(safeParameters));
    }

    /**
     * 创建 SQL 片段。
     *
     * @param sql        SQL 片段文本
     * @param parameters 片段参数
     * @return SQL 片段
     */
    public static SqlFragment of(String sql, Object... parameters) {
        return new SqlFragment(sql, convertParameters(Arrays.asList(parameters)));
    }

    private static List<Object> convertParameters(List<Object> parameters) {
        OwnedBindableValues.Buffer converted = OwnedBindableValues.buffer(parameters.size());
        for (Object parameter : parameters) {
            Object encoded = parameter instanceof EncodedParameter marker
                    ? marker.value()
                    : VALUE_CODECS.write(parameter);
            converted.add(BindableValueSnapshots.immutableValue(encoded));
        }
        return converted.publish();
    }

    private static List<Object> copyBindableParameters(List<Object> parameters) {
        List<Object> copied = new ArrayList<>(parameters.size());
        parameters.stream().map(SqlFragment::bindableParameter).forEach(copied::add);
        return copied;
    }

    static Object encodedParameter(Object parameter) {
        return new EncodedParameter(parameter);
    }

    static Object bindableParameter(Object parameter) {
        return parameter instanceof EncodedParameter encoded ? encoded.value() : parameter;
    }

    private record EncodedParameter(Object value) {
    }

}
