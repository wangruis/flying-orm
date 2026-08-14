package com.flying.orm.rdb.mapping;

import com.fasterxml.jackson.databind.JsonNode;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 一次实体读写映射的只读现场。
 *
 * <p>写事件里的 {@code values} 是即将交给 Repository 的列值快照；读事件里则是数据库返回的原始行。
 * 事件会复制 Map、数组、ByteBuffer、标准容器图和 Jackson 树，因此监听器不能借这些受支持的可变值改写执行参数，
 * 也不会在响应式异步链里看到调用方随后做的修改。标准容器和 Jackson 树最多嵌套 64 层，非法循环容器图
 * 会在递归复制前稳定拒绝，避免耗尽 JVM 栈。其他自定义对象继续遵守映射层的受信任交接契约。</p>
 *
 * @param metadata 当前实体的只读元数据
 * @param entity   正在写入或刚完成构造的实体
 * @param values   本次映射使用的列值快照
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record EntityMappingEvent(EntityMetadata<?> metadata,
                                 Object entity,
                                 Map<String, Object> values) {

    private static final int MAX_VALUE_NESTING = 64;

    public EntityMappingEvent {
        metadata = Objects.requireNonNull(metadata, "entity mapping metadata must not be null");
        entity = Objects.requireNonNull(entity, "mapped entity must not be null");
        values = snapshotValues(values);
    }

    /**
     * @return 重新复制数组、ByteBuffer、标准容器图和 Jackson 树后的只读现场；其他自定义值仍遵守映射层交接契约
     */
    @Override
    public Map<String, Object> values() {
        return snapshotValues(values);
    }

    private static Map<String, Object> snapshotValues(Map<String, Object> source) {
        Map<String, Object> safeSource = Objects.requireNonNull(source, "entity mapping values must not be null");
        IdentityHashMap<Object, Object> snapshots = new IdentityHashMap<>();
        Map<String, Object> copy = new LinkedHashMap<>();
        Map<String, Object> immutable = Collections.unmodifiableMap(copy);
        Set<Object> active = Collections.newSetFromMap(new IdentityHashMap<>());
        snapshots.put(safeSource, immutable);
        active.add(safeSource);
        try {
            safeSource.forEach((name, value) -> copy.put(name, snapshotValue(value, snapshots, active, 0)));
        } finally {
            active.remove(safeSource);
        }
        return immutable;
    }

    private static Object snapshotValue(Object value,
                                        IdentityHashMap<Object, Object> snapshots,
                                        Set<Object> active,
                                        int depth) {
        if (value == null) {
            return value;
        }
        requireAcyclic(value, active);
        Object existing = snapshots.get(value);
        if (existing != null) {
            return existing;
        }
        if (value.getClass().isArray()) {
            requireSafeNesting(depth);
            return snapshotArray(value, snapshots, active, depth);
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = snapshotBuffer(buffer);
            snapshots.put(value, copy);
            return copy;
        }
        if (value instanceof JsonNode node) {
            requireSafeJsonTree(node, depth);
            JsonNode copy = node.deepCopy();
            snapshots.put(value, copy);
            return copy;
        }
        if (value instanceof Map<?, ?> map) {
            requireSafeNesting(depth);
            Map<Object, Object> copy = new LinkedHashMap<>();
            Map<Object, Object> immutable = Collections.unmodifiableMap(copy);
            snapshots.put(value, immutable);
            active.add(value);
            try {
                map.forEach((key, item) -> copy.put(
                        snapshotValue(key, snapshots, active, depth + 1),
                        snapshotValue(item, snapshots, active, depth + 1)));
            } finally {
                active.remove(value);
            }
            return immutable;
        }
        if (value instanceof List<?> list) {
            requireSafeNesting(depth);
            List<Object> copy = new ArrayList<>(list.size());
            List<Object> immutable = Collections.unmodifiableList(copy);
            snapshots.put(value, immutable);
            active.add(value);
            try {
                list.forEach(item -> copy.add(snapshotValue(item, snapshots, active, depth + 1)));
            } finally {
                active.remove(value);
            }
            return immutable;
        }
        if (value instanceof Set<?> set) {
            requireSafeNesting(depth);
            Set<Object> copy = new LinkedHashSet<>();
            Set<Object> immutable = Collections.unmodifiableSet(copy);
            snapshots.put(value, immutable);
            active.add(value);
            try {
                set.forEach(item -> copy.add(snapshotValue(item, snapshots, active, depth + 1)));
            } finally {
                active.remove(value);
            }
            return immutable;
        }
        if (value instanceof Collection<?> collection) {
            requireSafeNesting(depth);
            Collection<Object> copy = new ArrayList<>(collection.size());
            Collection<Object> immutable = Collections.unmodifiableCollection(copy);
            snapshots.put(value, immutable);
            active.add(value);
            try {
                collection.forEach(item -> copy.add(snapshotValue(item, snapshots, active, depth + 1)));
            } finally {
                active.remove(value);
            }
            return immutable;
        }
        return value;
    }

    private static Object snapshotArray(Object value,
                                        IdentityHashMap<Object, Object> snapshots,
                                        Set<Object> active,
                                        int depth) {
        int length = Array.getLength(value);
        Class<?> componentType = value.getClass().getComponentType();
        Class<?> snapshotType = componentType.isPrimitive()
                ? value.getClass()
                : snapshotArrayType(value, Collections.newSetFromMap(new IdentityHashMap<>()), depth);
        Object copy = Array.newInstance(snapshotType.getComponentType(), length);
        snapshots.put(value, copy);
        if (componentType.isPrimitive()) {
            System.arraycopy(value, 0, copy, 0, length);
            return copy;
        }
        active.add(value);
        try {
            for (int index = 0; index < length; index++) {
                Array.set(copy, index, snapshotValue(Array.get(value, index), snapshots, active, depth + 1));
            }
        } finally {
            active.remove(value);
        }
        return copy;
    }

    /**
     * 只在冻结元素不再兼容原组件类型时拓宽为 Object[]；普通值数组继续保留原运行时类型。
     * 身份集合只用于类型预判，循环数组会保守按 Object[] 判断，随后由实际快照阶段统一拒绝。
     */
    private static Class<?> snapshotArrayType(Object value, Set<Object> inspecting, int depth) {
        requireSafeNesting(depth);
        Class<?> valueType = value.getClass();
        Class<?> componentType = valueType.getComponentType();
        if (componentType.isPrimitive() || !inspecting.add(value)) {
            return componentType.isPrimitive() ? valueType : Object[].class;
        }
        try {
            int length = Array.getLength(value);
            for (int index = 0; index < length; index++) {
                if (!snapshotFits(componentType, Array.get(value, index), inspecting, depth + 1)) {
                    return Object[].class;
                }
            }
            return valueType;
        } finally {
            inspecting.remove(value);
        }
    }

    private static boolean snapshotFits(Class<?> componentType,
                                        Object value,
                                        Set<Object> inspecting,
                                        int depth) {
        if (value == null) {
            return true;
        }
        Class<?> snapshotType;
        if (value.getClass().isArray()) {
            snapshotType = snapshotArrayType(value, inspecting, depth);
        } else if (value instanceof ByteBuffer) {
            snapshotType = ByteBuffer.class;
        } else if (value instanceof Map<?, ?>) {
            snapshotType = Map.class;
        } else if (value instanceof List<?>) {
            snapshotType = List.class;
        } else if (value instanceof Set<?>) {
            snapshotType = Set.class;
        } else if (value instanceof Collection<?>) {
            snapshotType = Collection.class;
        } else {
            snapshotType = value.getClass();
        }
        return componentType.isAssignableFrom(snapshotType);
    }

    private static void requireSafeNesting(int depth) {
        if (depth > MAX_VALUE_NESTING) {
            throw new IllegalArgumentException(
                    "entity mapping value nesting exceeds " + MAX_VALUE_NESTING);
        }
    }

    private static void requireAcyclic(Object value, Set<Object> active) {
        if (active.contains(value)) {
            throw new IllegalArgumentException("entity mapping value graph must not contain cycles");
        }
    }

    /**
     * 在调用 Jackson 的递归 deepCopy 前以显式栈验证深度和循环，避免不可信树先耗尽 JVM 栈。
     */
    private static void requireSafeJsonTree(JsonNode root, int rootDepth) {
        Deque<JsonNode> nodes = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        Deque<Boolean> exits = new ArrayDeque<>();
        IdentityHashMap<JsonNode, Boolean> active = new IdentityHashMap<>();
        IdentityHashMap<JsonNode, Boolean> completed = new IdentityHashMap<>();
        nodes.push(root);
        depths.push(rootDepth);
        exits.push(false);
        while (!nodes.isEmpty()) {
            JsonNode node = nodes.pop();
            int depth = depths.pop();
            boolean exiting = exits.pop();
            if (exiting) {
                active.remove(node);
                completed.put(node, Boolean.TRUE);
                continue;
            }
            if (completed.containsKey(node)) {
                continue;
            }
            if (active.put(node, Boolean.TRUE) != null) {
                throw new IllegalArgumentException("entity mapping JSON tree must not contain cycles");
            }
            if (node.isContainerNode()) {
                requireSafeNesting(depth);
            }
            nodes.push(node);
            depths.push(depth);
            exits.push(true);
            node.elements().forEachRemaining(child -> {
                if (child != null) {
                    nodes.push(child);
                    depths.push(depth + 1);
                    exits.push(false);
                }
            });
        }
    }

    private static ByteBuffer snapshotBuffer(ByteBuffer source) {
        ByteBuffer readable = source.duplicate();
        int position = readable.position();
        int limit = readable.limit();
        readable.position(0);
        ByteBuffer copy = ByteBuffer.allocate(limit).order(source.order());
        copy.put(readable);
        copy.flip();
        copy.position(position);
        return copy;
    }
}
