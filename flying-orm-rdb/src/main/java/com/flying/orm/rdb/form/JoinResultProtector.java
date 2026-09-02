package com.flying.orm.rdb.form;

import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

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

    ResultPlan plan(JoinQuerySpec spec,
                    Map<JoinSource, DataScope> scopes,
                    SensitiveDisplayMode displayMode) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        Map<JoinSource, DataScope> safeScopes = Objects.requireNonNull(
                scopes, "join scopes must not be null");
        SensitiveDisplayMode safeDisplayMode = Objects.requireNonNull(
                displayMode, "sensitive display mode must not be null");
        Map<JoinSource, List<JoinProjection>> projectionsBySource = new LinkedHashMap<>();
        for (JoinProjection projection : safeSpec.projections()) {
            projectionsBySource.computeIfAbsent(projection.field().source(), ignored -> new ArrayList<>())
                               .add(projection);
        }
        List<SourcePlan> protectedSources = new ArrayList<>();
        projectionsBySource.forEach((source, projections) -> {
            if (requiresTransform(source, projections, safeDisplayMode)) {
                DataScope sourceScope = safeScopes.getOrDefault(source, DataScope.none());
                protectedSources.add(new SourcePlan(
                        source, projections, safeSpec.projections(),
                        renderer.protection().resultOperation(
                                source.form(), sourceScope, safeDisplayMode),
                        projectsEncryptedField(source, projections)));
            }
        });
        return new ResultPlan(List.copyOf(protectedSources));
    }

    private static boolean requiresTransform(JoinSource source,
                                             List<JoinProjection> projections,
                                             SensitiveDisplayMode displayMode) {
        for (JoinProjection projection : projections) {
            String field = projection.field().field();
            if (source.form().protections().encrypted(field).isPresent()) {
                return true;
            }
            MaskedFieldDefinition masking = source.form().protections().masked(field).orElse(null);
            if (masking != null && displayedAsMasked(masking, displayMode)) {
                return true;
            }
        }
        return false;
    }

    private static boolean displayedAsMasked(MaskedFieldDefinition definition,
                                              SensitiveDisplayMode requested) {
        SensitiveDisplayMode effective = requested == SensitiveDisplayMode.DECLARED
                ? definition.display()
                : requested;
        return effective == SensitiveDisplayMode.MASKED;
    }

    private static boolean projectsEncryptedField(JoinSource source, List<JoinProjection> projections) {
        return projections.stream().anyMatch(projection -> source.form().protections()
                .encrypted(projection.field().field()).isPresent());
    }

    /** 查询级不可变结果计划；普通投影直接返回解码行，受保护来源才创建轻量视图。 */
    static final class ResultPlan {

        private final List<SourcePlan> sources;

        private ResultPlan(List<SourcePlan> sources) {
            this.sources = sources;
        }

        boolean direct() {
            return sources.isEmpty();
        }

        boolean requiresCpuBoundary() {
            return sources.stream().anyMatch(SourcePlan::encrypted);
        }

        DynamicRow transform(DynamicRow row) {
            DynamicRow safeRow = Objects.requireNonNull(row, "join result row must not be null");
            if (direct()) {
                return safeRow;
            }
            BoundResultPlan bound = safeRow.mappingBinding(this, () -> bind(safeRow));
            Map<Integer, Object> replacements = null;
            for (BoundSource source : bound.sources()) {
                DynamicRow sourceView = safeRow.renameColumnsBound(source.plan());
                DynamicRow transformed = source.plan().resultOperation().transform(sourceView);
                for (BoundProjection projection : source.projections()) {
                    Object value = transformed.value(projection.protectedIndex());
                    if (value != safeRow.value(projection.outputIndex())) {
                        if (replacements == null) {
                            replacements = new HashMap<>();
                        }
                        replacements.put(projection.outputIndex(), value);
                    }
                }
            }
            return replacements == null ? safeRow : safeRow.withValues(replacements);
        }

        private BoundResultPlan bind(DynamicRow row) {
            Map<String, Integer> indexes = new HashMap<>(Math.max(16, row.columnCount() * 2));
            for (int index = 0; index < row.columnCount(); index++) {
                indexes.put(row.columnName(index), index);
            }
            List<BoundSource> boundSources = new ArrayList<>(sources.size());
            for (SourcePlan source : sources) {
                List<BoundProjection> projections = new ArrayList<>(source.projections().size());
                for (JoinProjection projection : source.projections()) {
                    int outputIndex = requiredIndex(indexes, projection.alias());
                    int protectedIndex = requiredIndex(
                            indexes, source.primaryAlias(projection.field().field()));
                    projections.add(new BoundProjection(outputIndex, protectedIndex));
                }
                boundSources.add(new BoundSource(source, List.copyOf(projections)));
            }
            return new BoundResultPlan(List.copyOf(boundSources));
        }

        private static int requiredIndex(Map<String, Integer> indexes, String alias) {
            Integer index = indexes.get(alias);
            if (index == null) {
                throw new IllegalStateException("join result is missing projection alias: " + alias);
            }
            return index;
        }
    }

    /** 把一个受保护来源绑定为与原结果共享值数组的列名视图。 */
    private static final class SourcePlan implements UnaryOperator<String> {

        private final JoinSource source;
        private final List<JoinProjection> projections;
        private final ProtectedFieldRuntime.ResultOperation resultOperation;
        private final Map<String, String> renamedColumns;
        private final Map<String, String> primaryAliases;
        private final boolean encrypted;

        private SourcePlan(JoinSource source,
                           List<JoinProjection> projections,
                           List<JoinProjection> allProjections,
                           ProtectedFieldRuntime.ResultOperation resultOperation,
                           boolean encrypted) {
            this.source = source;
            this.projections = List.copyOf(projections);
            this.resultOperation = resultOperation;
            this.encrypted = encrypted;
            this.renamedColumns = new LinkedHashMap<>();
            this.primaryAliases = new LinkedHashMap<>();
            for (int index = 0; index < allProjections.size(); index++) {
                JoinProjection projection = allProjections.get(index);
                String target = ignoredColumn(index);
                if (projection.field().source().equals(source)
                        && primaryAliases.putIfAbsent(
                                projection.field().field(), projection.alias()) == null) {
                    target = projection.field().field();
                }
                renamedColumns.put(projection.alias(), target);
            }
        }

        private String ignoredColumn(int projectionIndex) {
            String candidate = "__join_skip_s" + source.ordinal() + "_c" + projectionIndex;
            while (source.form().findField(candidate).isPresent()) {
                candidate = '_' + candidate;
            }
            return candidate;
        }

        private List<JoinProjection> projections() {
            return projections;
        }

        private ProtectedFieldRuntime.ResultOperation resultOperation() {
            return resultOperation;
        }

        private boolean encrypted() {
            return encrypted;
        }

        private String primaryAlias(String field) {
            return primaryAliases.get(field);
        }

        @Override
        public String apply(String column) {
            return renamedColumns.getOrDefault(column, column);
        }
    }

    private record BoundResultPlan(List<BoundSource> sources) {
    }

    private record BoundSource(SourcePlan plan, List<BoundProjection> projections) {
    }

    private record BoundProjection(int outputIndex, int protectedIndex) {
    }
}
