package com.flying.orm.core.internal.condition;

import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 条件 AST 按需生成的结构摘要和共享参数源。
 *
 * @author wangr
 * @version v3.1
 */
public final class ConditionExecutionView {

    private final String shapeDigest;
    private final List<Object> parameterSources;
    private final long requiredStandardTermMask;
    private final boolean cacheable;

    ConditionExecutionView(String shapeDigest,
                           List<Object> ownedParameterSources,
                           long requiredStandardTermMask,
                           boolean cacheable) {
        this.shapeDigest = Objects.requireNonNull(shapeDigest, "condition shape digest must not be null");
        this.parameterSources = Objects.requireNonNull(
                ownedParameterSources, "condition parameter sources must not be null");
        this.requiredStandardTermMask = requiredStandardTermMask;
        this.cacheable = cacheable;
    }

    public String shapeDigest() {
        return shapeDigest;
    }

    List<Object> parameterSources() {
        return parameterSources;
    }

    public int parameterCount() {
        return parameterSources.size();
    }

    public boolean cacheable(long rendererStandardTermMask) {
        return cacheable
                && (rendererStandardTermMask & requiredStandardTermMask) == requiredStandardTermMask;
    }

    long requiredStandardTermMask() {
        return requiredStandardTermMask;
    }

    boolean structurallyCacheable() {
        return cacheable;
    }

    /** Takes ownership of the private directory; neither it nor its child segments may change after publication. */
    static List<Object> concatenate(List<List<Object>> segments) {
        return segments.size() == 1 ? segments.getFirst() : new ParameterSources(segments);
    }

    private static final class ParameterSources extends AbstractList<Object> {
        private final List<List<Object>> segments;
        private final int size;

        private ParameterSources(List<List<Object>> segments) {
            this.segments = segments;
            int count = 0;
            for (List<Object> segment : segments) {
                count = Math.addExact(count, segment.size());
            }
            this.size = count;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Object get(int index) {
            Objects.checkIndex(index, size);
            List<Object> current = this;
            while (current instanceof ParameterSources sources) {
                for (List<Object> segment : sources.segments) {
                    if (index < segment.size()) {
                        current = segment;
                        break;
                    }
                    index -= segment.size();
                }
            }
            return current.get(index);
        }

        @Override
        public void forEach(Consumer<? super Object> action) {
            Objects.requireNonNull(action, "parameter action must not be null");
            Iterator<List<Object>> current = segments.iterator();
            ArrayDeque<Iterator<List<Object>>> pending = null;
            while (true) {
                if (current.hasNext()) {
                    List<Object> segment = current.next();
                    if (segment instanceof ParameterSources sources) {
                        if (pending == null) pending = new ArrayDeque<>();
                        pending.push(current);
                        current = sources.segments.iterator();
                    } else {
                        segment.forEach(action);
                    }
                } else if (pending != null && !pending.isEmpty()) {
                    current = pending.pop();
                } else {
                    return;
                }
            }
        }

        @Override
        public Iterator<Object> iterator() {
            return new Iterator<>() {
                private final ArrayDeque<Iterator<List<Object>>> pending = new ArrayDeque<>();
                private Iterator<Object> leaf = List.of().iterator();
                {
                    pending.push(segments.iterator());
                }

                @Override
                public boolean hasNext() {
                    while (!leaf.hasNext() && !pending.isEmpty()) {
                        Iterator<List<Object>> current = pending.peek();
                        if (!current.hasNext()) {
                            pending.pop();
                        } else {
                            List<Object> segment = current.next();
                            if (segment instanceof ParameterSources sources) {
                                pending.push(sources.segments.iterator());
                            } else {
                                leaf = segment.iterator();
                            }
                        }
                    }
                    return leaf.hasNext();
                }

                @Override
                public Object next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    return leaf.next();
                }
            };
        }
    }
}
