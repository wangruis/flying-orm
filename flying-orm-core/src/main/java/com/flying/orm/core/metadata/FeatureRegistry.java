package com.flying.orm.core.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Feature 注册表以只读方式发布扩展能力，支持按规范化 id 和具体类型快速查找。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class FeatureRegistry {

    private static final FeatureRegistry EMPTY = new FeatureRegistry(List.of());

    private final List<Feature> features;

    private final Map<String, Feature> featuresById;

    private final Map<Class<? extends Feature>, Feature> featuresByType;

    private FeatureRegistry(List<Feature> features) {
        List<Feature> copiedFeatures = List.copyOf(features);
        Map<String, Feature> indexedById = new LinkedHashMap<>(MetadataNames.mapCapacity(copiedFeatures.size()));
        Map<Class<? extends Feature>, Feature> indexedByType = new LinkedHashMap<>(MetadataNames.mapCapacity(copiedFeatures.size()));

        for (Feature feature : copiedFeatures) {
            Feature safeFeature = Objects.requireNonNull(feature, "feature must not be null");
            String normalizedId = MetadataNames.normalize(safeFeature.id(), "feature id");
            Feature previousById = indexedById.putIfAbsent(normalizedId, safeFeature);
            if (previousById != null) {
                throw new IllegalArgumentException("duplicate feature id");
            }

            @SuppressWarnings("unchecked")
            Class<? extends Feature> featureType = (Class<? extends Feature>) safeFeature.getClass();
            Feature previousByType = indexedByType.putIfAbsent(featureType, safeFeature);
            if (previousByType != null) {
                throw new IllegalArgumentException("duplicate feature type: " + featureType.getName());
            }
        }

        this.features = copiedFeatures;
        this.featuresById = Map.copyOf(indexedById);
        this.featuresByType = Map.copyOf(indexedByType);
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

    /**
     * 按规范化 id 查找 feature。
     *
     * @param id feature 标识
     * @return 匹配 feature；不存在时返回空
     */
    public Optional<Feature> find(String id) {
        return Optional.ofNullable(featuresById.get(MetadataNames.normalize(id, "feature id")));
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
     * 按具体类型查找 feature。
     *
     * @param type feature 具体类型
     * @param <T>  feature 类型
     * @return 匹配 feature；不存在时返回空
     */
    public <T extends Feature> Optional<T> find(Class<T> type) {
        Objects.requireNonNull(type, "feature type must not be null");
        return Optional.ofNullable(type.cast(featuresByType.get(type)));
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

        /**
         * 构建只读 feature 注册表。
         *
         * @return feature 注册表
         */
        public FeatureRegistry build() {
            if (features.isEmpty()) {
                return EMPTY;
            }
            return new FeatureRegistry(features);
        }
    }
}
