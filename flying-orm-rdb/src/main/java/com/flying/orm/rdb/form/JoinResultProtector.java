package com.flying.orm.rdb.form;

import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 按 JOIN 投影来源完成受保护字段解密和声明式脱敏，再恢复调用方声明的结果别名。
 *
 * <p>每个来源每行最多转换一次；外连接产生的 null 值原样保留。隐藏盲索引从不进入结果模型。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JoinResultProtector {

    private final FormDataSqlRenderer renderer;

    JoinResultProtector(FormDataSqlRenderer renderer) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
    }

    DynamicRow transform(JoinQuerySpec spec,
                         DynamicRow row,
                         Map<JoinSource, DataScope> scopes,
                         SensitiveDisplayMode displayMode) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        DynamicRow safeRow = Objects.requireNonNull(row, "join result row must not be null");
        Map<JoinSource, Map<String, Object>> sourceValues = new LinkedHashMap<>();
        for (JoinProjection projection : safeSpec.projections()) {
            sourceValues.computeIfAbsent(projection.field().source(), ignored -> new LinkedHashMap<>())
                        .put(projection.field().field(), safeRow.get(projection.alias()));
        }
        Map<JoinSource, DynamicRow> transformed = new LinkedHashMap<>();
        sourceValues.forEach((source, values) -> transformed.put(
                source,
                renderer.protection().transform(
                        source.form(), DynamicRow.copyOf(values),
                        scopes.getOrDefault(source, DataScope.none()), displayMode)));
        Map<String, Object> result = new LinkedHashMap<>();
        for (JoinProjection projection : safeSpec.projections()) {
            result.put(projection.alias(), transformed.get(projection.field().source())
                                                      .get(projection.field().field()));
        }
        return DynamicRow.copyOf(result);
    }
}
