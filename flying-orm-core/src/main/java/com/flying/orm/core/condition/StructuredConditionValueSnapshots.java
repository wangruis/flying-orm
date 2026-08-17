package com.flying.orm.core.condition;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 冻结前端结构化条件中的 JSON 形状值图。
 *
 * <p>只复制数组、Collection、Map、ByteBuffer 和可变文本；未知非容器标量保持身份，供可信扩展解释。
 * 复制使用对象身份保留共享关系与环，并以编译器允许的最大深度和原始引用数作为硬边界。</p>
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class StructuredConditionValueSnapshots {

    private static final int MAX_DEPTH = 64;
    private static final int MAX_REFERENCES = 20_000;

    private StructuredConditionValueSnapshots() {
    }

    static Object snapshot(Object value) {
        try {
            return new CopyContext().copy(value);
        } catch (StructuredConditionException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            StructuredConditionValueNormalizer.rethrowVirtualMachineError(failure);
            throw valueShapeNotAllowed();
        }
    }

    private static boolean isContainer(Object value) {
        return value.getClass().isArray() || value instanceof Collection<?> || value instanceof Map<?, ?>;
    }

    private static StructuredConditionException limitExceeded(StructuredConditionErrorCode code, String boundary) {
        return StructuredConditionException.of(code,
                                               "conditions.value",
                                               "structured condition value graph exceeds " + boundary);
    }

    private static StructuredConditionException valueShapeNotAllowed() {
        return StructuredConditionException.of(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                               "conditions.value",
                                               "structured condition value shape is not allowed at conditions.value");
    }

    private static final class CopyContext {

        private final IdentityHashMap<Object, Object> copies = new IdentityHashMap<>();
        private final ArrayDeque<CopyFrame> pending = new ArrayDeque<>();
        private int references;

        private Object copy(Object value) {
            Object result = copyValue(value, 1);
            while (!pending.isEmpty()) {
                copyFrame(pending.removeFirst());
            }
            return result;
        }

        private Object copyValue(Object value, int depth) {
            if (value instanceof ByteBuffer) {
                return BindableValueSnapshots.immutableValue(value);
            }
            if (value instanceof CharSequence text && !(text instanceof String)) {
                return text.toString();
            }
            if (value == null || !isContainer(value)) {
                return value;
            }
            Object existing = copies.get(value);
            if (existing != null) {
                return existing;
            }
            if (depth > MAX_DEPTH) {
                throw limitExceeded(StructuredConditionErrorCode.DEPTH_EXCEEDED, "depth limit");
            }
            if (value.getClass().isArray()) {
                return copyArray(value, depth);
            }
            if (value instanceof Map<?, ?>) {
                Map<Object, Object> target = new LinkedHashMap<>();
                Map<Object, Object> exposed = Collections.unmodifiableMap(target);
                copies.put(value, exposed);
                pending.addLast(new CopyFrame(value, target, depth));
                return exposed;
            }
            List<Object> target = new ArrayList<>();
            List<Object> exposed = Collections.unmodifiableList(target);
            copies.put(value, exposed);
            pending.addLast(new CopyFrame(value, target, depth));
            return exposed;
        }

        private Object copyArray(Object source, int depth) {
            int length = Array.getLength(source);
            Class<?> componentType = source.getClass().getComponentType();
            if (!componentType.isPrimitive()) {
                addReferences(length);
            }
            Object target = Array.newInstance(componentType, length);
            copies.put(source, target);
            if (componentType.isPrimitive()) {
                System.arraycopy(source, 0, target, 0, length);
            } else {
                pending.addLast(new CopyFrame(source, target, depth));
            }
            return target;
        }

        private void copyFrame(CopyFrame frame) {
            Object source = frame.source();
            if (source.getClass().isArray()) {
                copyObjectArray(source, frame.target(), frame.depth());
            } else if (source instanceof Map<?, ?> map) {
                copyMap(map, frame.target(), frame.depth());
            } else {
                copyCollection((Collection<?>) source, frame.target(), frame.depth());
            }
        }

        private void copyObjectArray(Object source, Object target, int depth) {
            int length = Array.getLength(source);
            Class<?> componentType = source.getClass().getComponentType();
            for (int index = 0; index < length; index++) {
                Object item = Array.get(source, index);
                Object itemCopy = item instanceof CharSequence text && !(text instanceof String)
                        ? copyTextForArray(text, componentType)
                        : copyValue(item, depth + 1);
                Array.set(target, index, itemCopy);
            }
        }

        @SuppressWarnings("unchecked")
        private void copyCollection(Collection<?> source, Object targetObject, int depth) {
            List<Object> target = (List<Object>) targetObject;
            for (Object item : source) {
                addReferences(1);
                target.add(copyValue(item, depth + 1));
            }
        }

        @SuppressWarnings("unchecked")
        private void copyMap(Map<?, ?> source, Object targetObject, int depth) {
            Map<Object, Object> target = (Map<Object, Object>) targetObject;
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                addReferences(2);
                if (entry.getKey() != null && isContainer(entry.getKey())) {
                    throw valueShapeNotAllowed();
                }
                Object key = copyValue(entry.getKey(), depth + 1);
                if (target.containsKey(key)) {
                    throw valueShapeNotAllowed();
                }
                target.put(key, copyValue(entry.getValue(), depth + 1));
            }
        }

        private void addReferences(int added) {
            if (added > MAX_REFERENCES - references) {
                throw limitExceeded(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, "reference limit");
            }
            references += added;
        }

        private static Object copyTextForArray(CharSequence value, Class<?> componentType) {
            if (componentType.isAssignableFrom(String.class)) {
                return value.toString();
            }
            if (componentType == StringBuilder.class) {
                return new StringBuilder(value);
            }
            if (componentType == StringBuffer.class) {
                return new StringBuffer(value);
            }
            if (componentType == CharBuffer.class) {
                return CharBuffer.wrap(value.toString()).asReadOnlyBuffer();
            }
            throw valueShapeNotAllowed();
        }
    }

    private record CopyFrame(Object source, Object target, int depth) {
    }
}
