package com.flying.orm.core.metadata;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        this.exactValues = Collections.unmodifiableMap(exactValues);
        this.unambiguousFoldedValues = Collections.unmodifiableMap(unambiguousFoldedValues);
    }

    static <T> MetadataNameIndex<T> ofOwned(List<T> values,
                                            Function<T, String> nameExtractor,
                                            Function<T, String> keyExtractor,
                                            String itemName) {
        Map<String, T> exactValues = new LinkedHashMap<>(Names.mapCapacity(values.size()));
        Map<String, T> foldedValues = new LinkedHashMap<>(Names.mapCapacity(values.size()));
        Set<String> ambiguousFoldedNames = new HashSet<>();
        for (T value : values) {
            String exactName = nameExtractor.apply(value);
            T previous = exactValues.putIfAbsent(exactName, value);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate " + itemName + " name");
            }
            String foldedName = keyExtractor.apply(value);
            if (foldedValues.containsKey(foldedName)) {
                foldedValues.remove(foldedName);
                ambiguousFoldedNames.add(foldedName);
            } else if (!ambiguousFoldedNames.contains(foldedName)) {
                foldedValues.put(foldedName, value);
            }
        }
        return new MetadataNameIndex<>(exactValues, foldedValues);
    }

    Optional<T> find(String name, String fieldName) {
        FieldIdentity identity = FieldIdentity.of(name);
        String exactName = identity.name();
        T exactValue = exactValues.get(exactName);
        if (exactValue != null) {
            return Optional.of(exactValue);
        }
        return Optional.ofNullable(unambiguousFoldedValues.get(identity.key()));
    }
}
