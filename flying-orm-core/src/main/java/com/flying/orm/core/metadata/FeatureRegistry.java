package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 只读扩展注册表。本层相同 id 覆盖父层，构建时合并查找表，运行时不逐层遍历。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class FeatureRegistry {

    private static final FeatureRegistry EMPTY = new FeatureRegistry(List.of(), null);

    private final List<Feature> localFeatures;
    private final FeatureRegistry parent;
    private final List<Feature> features;
    private final Map<String, Feature> featuresById;

    private FeatureRegistry(List<Feature> localFeatures, FeatureRegistry parent) {
        this.localFeatures = List.copyOf(localFeatures);
        this.parent = parent;
        Map<String, Feature> local = new LinkedHashMap<>(Names.mapCapacity(localFeatures.size()));
        for (Feature feature : localFeatures) {
            String normalizedId = Names.key(feature.id(), "feature id");
            if (local.putIfAbsent(normalizedId, feature) != null) {
                throw new IllegalArgumentException("duplicate feature id");
            }
        }
        Map<String, Feature> effective = parent == null ? local : new LinkedHashMap<>(parent.featuresById);
        if (parent != null) effective.putAll(local);
        this.features = List.copyOf(effective.values());
        this.featuresById = Map.copyOf(effective);
    }

    /**
     * 返回空 feature 注册表。
     *
     * @return 空注册表
     */
    public static FeatureRegistry empty() {
        return EMPTY;
    }

    /**
     * 创建 feature 注册表构建器。
     *
     * @return feature 注册表构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回只读 feature 集合。
     *
     * @return 只读 feature 集合
     */
    public List<Feature> features() {
        return features;
    }

    /** 只替换继承来源，本层声明不变；复用字段时不会把旧表的配置变成本层覆盖。 */
    public FeatureRegistry withParent(FeatureRegistry parent) {
        FeatureRegistry inherited = Objects.requireNonNull(parent, "parent features");
        if (inherited.features.isEmpty()) inherited = null;
        if (this.parent == inherited) return this;
        if (localFeatures.isEmpty() && inherited == null) return EMPTY;
        return new FeatureRegistry(localFeatures, inherited);
    }

    /**
     * 按规范化 id 查找 feature。
     *
     * @param id feature 标识
     * @return 匹配 feature；不存在时返回空
     */
    public Optional<Feature> find(String id) {
        return Optional.ofNullable(featuresById.get(Names.key(id, "feature id")));
    }

    /**
     * 按规范化 id 获取 feature，不存在时抛出确定性异常。
     *
     * @param id feature 标识
     * @return 匹配 feature
     * @throws IllegalArgumentException feature 不存在时抛出
     */
    public Feature feature(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("feature does not exist"));
    }

    /**
     * 按契约类型查找唯一 feature；同类型多个 id 应改用 id 查找，不任意选择。
     *
     * @param type feature 具体类型
     * @param <T>  feature 类型
     * @return 匹配 feature；不存在时返回空
     */
    public <T extends Feature> Optional<T> find(Class<T> type) {
        Objects.requireNonNull(type, "feature type must not be null");
        T found = null;
        for (Feature feature : features) {
            if (!type.isInstance(feature)) continue;
            if (found != null) throw new IllegalArgumentException("multiple features of type: " + type.getName());
            found = type.cast(feature);
        }
        return Optional.ofNullable(found);
    }

    /**
     * 按具体类型获取 feature，不存在时抛出确定性异常。
     *
     * @param type feature 具体类型
     * @param <T>  feature 类型
     * @return 匹配 feature
     * @throws IllegalArgumentException feature 不存在时抛出
     */
    public <T extends Feature> T feature(Class<T> type) {
        return find(type).orElseThrow(() -> new IllegalArgumentException(
                "feature type [" + type.getName() + "] does not exist"));
    }

    /**
     * Feature 注册表构建器，用于发布前收集扩展能力。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final List<Feature> features = new ArrayList<>();
        private FeatureRegistry parent;

        private Builder() {
        }

        /**
         * 添加 feature。
         *
         * @param feature feature 实例
         * @return 当前构建器
         */
        public Builder add(Feature feature) {
            features.add(Objects.requireNonNull(feature, "feature must not be null"));
            return this;
        }

        /** 设置数据库/Schema/表等上层提供的默认扩展。 */
        public Builder parent(FeatureRegistry parent) {
            this.parent = Objects.requireNonNull(parent, "parent features");
            return this;
        }

        /**
         * 构建只读 feature 注册表。
         *
         * @return feature 注册表
         */
        public FeatureRegistry build() {
            if (features.isEmpty() && (parent == null || parent.features.isEmpty())) {
                return EMPTY;
            }
            return new FeatureRegistry(features, parent);
        }
    }
}
