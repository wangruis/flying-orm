package com.flying.orm.core.metadata;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * 元数据物理名称索引：精确名称始终优先；只有忽略大小写后仍唯一时，才保留历史宽松查找。
 *
 * <p>发布后只读，可安全复用于 Schema 和表目录的并发查询。折叠键发生歧义时不猜测目标，调用方必须提供精确物理名称。</p>
 *
 * @param <T> 被索引的元数据类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class MetadataNameIndex<T> {

    private final Map<String, T> exactValues;

    private final Map<String, T> unambiguousFoldedValues;

    private MetadataNameIndex(Map<String, T> exactValues, Map<String, T> unambiguousFoldedValues) {
        this.exactValues = Map.copyOf(exactValues);
        this.unambiguousFoldedValues = Map.copyOf(unambiguousFoldedValues);
    }

    static <T> MetadataNameIndex<T> of(List<T> values,
                                       Function<T, String> nameExtractor,
                                       String itemName) {
        Objects.requireNonNull(values, itemName + " list must not be null");
        Function<T, String> safeNameExtractor = Objects.requireNonNull(nameExtractor,
                                                                         "metadata name extractor must not be null");
        Map<String, T> exactValues = new LinkedHashMap<>(MetadataNames.mapCapacity(values.size()));
        Map<String, T> foldedValues = new LinkedHashMap<>(MetadataNames.mapCapacity(values.size()));
        Set<String> ambiguousFoldedNames = new HashSet<>();
        for (T value : values) {
            T safeValue = Objects.requireNonNull(value, itemName + " must not be null");
            String exactName = MetadataNames.requireText(safeNameExtractor.apply(safeValue), itemName + " name");
            T previous = exactValues.putIfAbsent(exactName, safeValue);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate " + itemName + " name");
            }
            String foldedName = MetadataNames.normalize(exactName, itemName + " name");
            if (foldedValues.containsKey(foldedName)) {
                foldedValues.remove(foldedName);
                ambiguousFoldedNames.add(foldedName);
            } else if (!ambiguousFoldedNames.contains(foldedName)) {
                foldedValues.put(foldedName, safeValue);
            }
        }
        return new MetadataNameIndex<>(exactValues, foldedValues);
    }

    Optional<T> find(String name, String fieldName) {
        String exactName = MetadataNames.requireText(name, fieldName);
        T exactValue = exactValues.get(exactName);
        if (exactValue != null) {
            return Optional.of(exactValue);
        }
        return Optional.ofNullable(unambiguousFoldedValues.get(MetadataNames.normalize(exactName, fieldName)));
    }
}
