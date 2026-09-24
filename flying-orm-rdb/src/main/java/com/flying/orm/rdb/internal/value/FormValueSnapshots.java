package com.flying.orm.rdb.internal.value;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.internal.InternalApi;
import tools.jackson.databind.JsonNode;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Owns raw JSON, SQL ARRAY and VECTOR values while their field meaning is still known.
 *
 * @author wangr
 * @version v3.1
 */
@InternalApi
public final class FormValueSnapshots {

    private FormValueSnapshots() {
    }

    public static Map<String, Object> snapshot(DynamicForm form, Map<String, Object> source) {
        return snapshot(form, source, field -> false);
    }

    /** 显式 codec 拥有领域值的解释；通用快照不能把其 Map/List 子类改成另一种 Java 类型。 */
    public static Map<String, Object> snapshot(DynamicForm form, Map<String, Object> source,
                                              Predicate<DynamicField> customValue) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeSource = Objects.requireNonNull(source, "form values must not be null");
        if (safeSource.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> snapshot = new LinkedHashMap<>(safeSource.size());
        Snapshotter snapshotter = null;
        for (Map.Entry<String, Object> entry : safeSource.entrySet()) {
            Object value = entry.getValue();
            DynamicField field = safeForm.findField(entry.getKey()).orElse(null);
            if (field != null && customValue.test(field)) {
                snapshot.put(entry.getKey(), BindableValueSnapshots.logicalValue(value));
                continue;
            }
            if (value == null || field == null) {
                snapshot.put(entry.getKey(), BindableValueSnapshots.logicalValue(value));
                continue;
            }
            LogicalType type = field.databaseType().logicalType();
            boolean sqlArray = field.databaseType().isArray();
            if (type == LogicalType.JSON || sqlArray || type == LogicalType.VECTOR) {
                if (snapshotter == null) {
                    snapshotter = new Snapshotter();
                }
                snapshot.put(entry.getKey(), snapshotter.snapshot(type, sqlArray, value));
            } else {
                snapshot.put(entry.getKey(), BindableValueSnapshots.logicalValue(value));
            }
        }
        return Collections.unmodifiableMap(snapshot);
    }

    private static final class Snapshotter {
        private IdentityHashMap<Object, Object> copies;
        private IdentityHashMap<Object, Boolean> active;

        private Object snapshot(LogicalType type, boolean sqlArray, Object value) {
            if (value == null) return null;
            if (type == LogicalType.JSON) return json(value);
            if (sqlArray || type == LogicalType.VECTOR) return sequence(value);
            return BindableValueSnapshots.logicalValue(value);
        }

        private Object json(Object value) {
            if (value == null) return null;
            if (active(value)) {
                throw new IllegalArgumentException("JSON form value must not contain cycles");
            }
            Object existing = copyOf(value);
            if (existing != null) return existing;
            if (value instanceof JsonNode node) {
                JsonNode copy = node.deepCopy();
                remember(value, copy);
                return copy;
            }
            if (value instanceof Map<?, ?> map) return jsonMap(map);
            if (value instanceof Collection<?> collection) return jsonList(collection);
            if (value.getClass().isArray()) return jsonArray(value);
            Object scalar = BindableValueSnapshots.logicalScalar(value);
            if (scalar != value) remember(value, scalar);
            return scalar;
        }

        private Object jsonMap(Map<?, ?> source) {
            Map<Object, Object> copy = new LinkedHashMap<>(source.size());
            Map<Object, Object> exposed = Collections.unmodifiableMap(copy);
            remember(source, exposed);
            enter(source);
            try {
                source.forEach((key, value) -> copy.put(key, json(value)));
            } finally {
                leave(source);
            }
            return exposed;
        }

        private Object jsonList(Collection<?> source) {
            List<Object> copy = new ArrayList<>(source.size());
            List<Object> exposed = Collections.unmodifiableList(copy);
            remember(source, exposed);
            enter(source);
            try {
                source.forEach(value -> copy.add(json(value)));
            } finally {
                leave(source);
            }
            return exposed;
        }

        private Object jsonArray(Object source) {
            int length = Array.getLength(source);
            if (source.getClass().getComponentType().isPrimitive()) {
                Object copy = Array.newInstance(source.getClass().getComponentType(), length);
                System.arraycopy(source, 0, copy, 0, length);
                remember(source, copy);
                return copy;
            }
            Object[] copy = new Object[length];
            remember(source, copy);
            enter(source);
            try {
                for (int index = 0; index < length; index++) copy[index] = json(Array.get(source, index));
            } finally {
                leave(source);
            }
            return copy;
        }

        private Object sequence(Object value) {
            if (value instanceof Collection<?> collection) {
                if (active(value)) {
                    throw new IllegalArgumentException("SQL ARRAY or VECTOR value must not contain cycles");
                }
                Object existing = copyOf(value);
                if (existing != null) return existing;
                List<Object> copy = new ArrayList<>(collection.size());
                List<Object> exposed = Collections.unmodifiableList(copy);
                remember(value, exposed);
                enter(value);
                try {
                    collection.forEach(item -> copy.add(sequenceItem(item)));
                } finally {
                    leave(value);
                }
                return exposed;
            }
            return BindableValueSnapshots.logicalValue(value);
        }

        private Object copyOf(Object source) {
            return copies == null ? null : copies.get(source);
        }

        private void remember(Object source, Object copy) {
            if (copies == null) {
                copies = new IdentityHashMap<>();
            }
            copies.put(source, copy);
        }

        private boolean active(Object source) {
            return active != null && active.containsKey(source);
        }

        private void enter(Object source) {
            if (active == null) {
                active = new IdentityHashMap<>();
            }
            active.put(source, Boolean.TRUE);
        }

        private void leave(Object source) {
            active.remove(source);
        }

        private static Object sequenceItem(Object value) {
            if (value instanceof Collection<?> || value != null && value.getClass().isArray()) {
                throw new IllegalArgumentException("nested SQL ARRAY or VECTOR values are not supported");
            }
            return BindableValueSnapshots.logicalValue(value);
        }
    }
}
