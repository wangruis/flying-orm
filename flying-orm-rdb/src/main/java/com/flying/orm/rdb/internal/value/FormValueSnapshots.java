package com.flying.orm.rdb.internal.value;

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
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeSource = Objects.requireNonNull(source, "form values must not be null");
        Snapshotter snapshotter = new Snapshotter();
        Map<String, Object> snapshot = new LinkedHashMap<>(safeSource.size());
        safeSource.forEach((name, value) -> snapshot.put(name, safeForm.findField(name)
                .map(field -> snapshotter.snapshot(field.databaseType().logicalType(),
                                                   field.databaseType().isArray(), value))
                .orElseGet(() -> BindableValueSnapshots.immutableValue(value))));
        return Collections.unmodifiableMap(snapshot);
    }

    private static final class Snapshotter {
        private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        private final IdentityHashMap<Object, Boolean> active = new IdentityHashMap<>();

        private Object snapshot(LogicalType type, boolean sqlArray, Object value) {
            if (value == null) return null;
            if (type == LogicalType.JSON) return json(value);
            if (sqlArray || type == LogicalType.VECTOR) return sequence(value);
            return BindableValueSnapshots.immutableValue(value);
        }

        private Object json(Object value) {
            if (value == null) return null;
            if (active.containsKey(value)) {
                throw new IllegalArgumentException("JSON form value must not contain cycles");
            }
            Object existing = copies.get(value);
            if (existing != null) return existing;
            if (value instanceof JsonNode node) {
                JsonNode copy = node.deepCopy();
                copies.put(value, copy);
                return copy;
            }
            if (value instanceof Map<?, ?> map) return jsonMap(map);
            if (value instanceof Collection<?> collection) return jsonList(collection);
            if (value.getClass().isArray()) return jsonArray(value);
            Object scalar = BindableValueSnapshots.immutableScalar(value, null);
            if (scalar != value) copies.put(value, scalar);
            return scalar;
        }

        private Object jsonMap(Map<?, ?> source) {
            Map<Object, Object> copy = new LinkedHashMap<>(source.size());
            Map<Object, Object> exposed = Collections.unmodifiableMap(copy);
            copies.put(source, exposed);
            active.put(source, Boolean.TRUE);
            try {
                source.forEach((key, value) -> copy.put(key, json(value)));
            } finally {
                active.remove(source);
            }
            return exposed;
        }

        private Object jsonList(Collection<?> source) {
            List<Object> copy = new ArrayList<>(source.size());
            List<Object> exposed = Collections.unmodifiableList(copy);
            copies.put(source, exposed);
            active.put(source, Boolean.TRUE);
            try {
                source.forEach(value -> copy.add(json(value)));
            } finally {
                active.remove(source);
            }
            return exposed;
        }

        private Object jsonArray(Object source) {
            int length = Array.getLength(source);
            if (source.getClass().getComponentType().isPrimitive()) {
                Object copy = Array.newInstance(source.getClass().getComponentType(), length);
                System.arraycopy(source, 0, copy, 0, length);
                copies.put(source, copy);
                return copy;
            }
            Object[] copy = new Object[length];
            copies.put(source, copy);
            active.put(source, Boolean.TRUE);
            try {
                for (int index = 0; index < length; index++) copy[index] = json(Array.get(source, index));
            } finally {
                active.remove(source);
            }
            return copy;
        }

        private Object sequence(Object value) {
            if (value instanceof Collection<?> collection) {
                if (active.containsKey(value)) {
                    throw new IllegalArgumentException("SQL ARRAY or VECTOR value must not contain cycles");
                }
                Object existing = copies.get(value);
                if (existing != null) return existing;
                List<Object> copy = new ArrayList<>(collection.size());
                List<Object> exposed = Collections.unmodifiableList(copy);
                copies.put(value, exposed);
                active.put(value, Boolean.TRUE);
                try {
                    collection.forEach(item -> copy.add(sequenceItem(item)));
                } finally {
                    active.remove(value);
                }
                return exposed;
            }
            return BindableValueSnapshots.immutableValue(value);
        }

        private static Object sequenceItem(Object value) {
            if (value instanceof Collection<?> || value != null && value.getClass().isArray()) {
                throw new IllegalArgumentException("nested SQL ARRAY or VECTOR values are not supported");
            }
            return BindableValueSnapshots.immutableValue(value);
        }
    }
}
