package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Internal ownership and snapshot rules for prepared protected write values. */
final class PreparedWriteValues {

    private PreparedWriteValues() {
    }

    static Map<String, Object> initialize(DynamicForm form, Map<String, Object> values) {
        return values instanceof OwnedValues owned ? owned.ownedMap() : snapshot(form, values);
    }

    static Map<String, Object> owned(Map<String, Object> values) {
        return new OwnedValues(values);
    }

    static Map<String, Object> snapshot(DynamicForm form, Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        Objects.requireNonNull(source, "protected write values must not be null")
               .forEach((name, value) -> copy.put(name, snapshotValue(form, name, value)));
        return Collections.unmodifiableMap(copy);
    }

    private static Object snapshotValue(DynamicForm form, String name, Object value) {
        return form.findField(name)
                   .<Object>map(field -> snapshotValue(field, value))
                   .orElse(value);
    }

    static Object snapshotValue(DynamicField field, Object value) {
        return field.databaseType().isBinary()
                ? BindableValueSnapshots.immutableValue(value) : value;
    }

    private static final class OwnedValues extends AbstractMap<String, Object> {

        private final Map<String, Object> values;

        private OwnedValues(Map<String, Object> values) {
            this.values = Collections.unmodifiableMap(Objects.requireNonNull(
                    values, "owned protected write values must not be null"));
        }

        private Map<String, Object> ownedMap() {
            return values;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return values.entrySet();
        }
    }
}
