package com.flying.orm.rdb.mapping;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Read-only values observed around one entity mapping operation.
 *
 * <p>The top-level map is copied once at construction and exposed as one unmodifiable view. Nested
 * values are the mapping layer's owned values and are intentionally not cloned or traversed. The
 * listener runs synchronously in the mapping call, so implementations must treat every nested value
 * as read-only and must not retain it past the callback.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
public record EntityMappingEvent(EntityMetadata<?> metadata,
                                 Object entity,
                                 Map<String, Object> values) {

    public EntityMappingEvent {
        metadata = Objects.requireNonNull(metadata, "entity mapping metadata must not be null");
        entity = Objects.requireNonNull(entity, "mapped entity must not be null");
        values = snapshotValues(values);
    }

    @Override
    public Map<String, Object> values() {
        return values;
    }

    private static Map<String, Object> snapshotValues(Map<String, Object> source) {
        Map<String, Object> owned = new LinkedHashMap<>(Objects.requireNonNull(
                source, "entity mapping values must not be null"));
        return Collections.unmodifiableMap(owned);
    }
}
