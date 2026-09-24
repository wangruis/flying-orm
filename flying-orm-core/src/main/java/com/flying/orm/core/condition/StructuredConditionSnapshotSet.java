package com.flying.orm.core.condition;

import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Set ownership without invoking recursive JDK container hashing or equality during insertion. */
final class StructuredConditionSnapshotSet extends AbstractSet<Object> {

    private final Map<ElementKey, Object> elements = new LinkedHashMap<>();
    private int constructionHash;

    boolean addOwned(Object value, Hashes hashes) {
        int hash = hashes.hash(value);
        ElementKey key = new ElementKey(value, hash);
        int previousSize = elements.size();
        elements.putIfAbsent(key, value);
        if (elements.size() == previousSize) return false;
        constructionHash += hash;
        return true;
    }

    int constructionHash() {
        return constructionHash;
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public Iterator<Object> iterator() {
        return elements.values().iterator();
    }

    @Override
    public boolean contains(Object value) {
        int hash = container(value) ? new Hashes().hash(value) : Objects.hashCode(value);
        return elements.containsKey(new ElementKey(value, hash));
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        // Public snapshots may expose independently copied mutable scalar leaves (for example Date).
        return new Hashes().hash(this);
    }

    private record ElementKey(Object value, int hash) {
        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ElementKey key && hash == key.hash && equal(value, key.value);
        }
    }

    /** A construction-local memo; it must not outlive the snapshot operation. */
    static final class Hashes {
        private final IdentityHashMap<Object, Integer> completed = new IdentityHashMap<>();

        void remember(Object value, int hash) {
            completed.put(value, hash);
        }

        int hash(Object value) {
            if (!container(value)) return Objects.hashCode(value);
            Integer existing = completed.get(value);
            if (existing != null) return existing;
            ArrayDeque<HashFrame> pending = new ArrayDeque<>();
            IdentityHashMap<Object, Boolean> active = new IdentityHashMap<>();
            pending.push(new HashFrame(value));
            active.put(value, Boolean.TRUE);
            int result = 0;
            while (!pending.isEmpty()) {
                HashFrame frame = pending.peek();
                if (!frame.hasNext()) {
                    result = frame.hash;
                    completed.put(frame.source, result);
                    active.remove(frame.source);
                    pending.pop();
                    if (!pending.isEmpty()) pending.peek().accept(result);
                } else {
                    Object child = frame.next();
                    Integer known = completed.get(child);
                    if (known != null) {
                        frame.accept(known);
                    } else if (!container(child)) {
                        frame.accept(Objects.hashCode(child));
                    } else {
                        if (active.put(child, Boolean.TRUE) != null) {
                            throw new IllegalArgumentException("cyclic set member cannot be hashed");
                        }
                        pending.push(new HashFrame(child));
                    }
                }
            }
            return result;
        }
    }

    private static boolean container(Object value) {
        // Arrays and general Collection wrappers retain their ordinary identity semantics.
        return value instanceof List<?> || value instanceof Map<?, ?> || value instanceof Set<?>;
    }

    private static final class HashFrame {
        private final Object source;
        private final Iterator<?> iterator;
        private Map.Entry<?, ?> entry;
        private boolean valuePending;
        private int keyHash;
        private int hash;

        private HashFrame(Object source) {
            this.source = source;
            if (source instanceof Map<?, ?> map) {
                iterator = map.entrySet().iterator();
            } else {
                iterator = ((Iterable<?>) source).iterator();
                hash = source instanceof List<?> ? 1 : 0;
            }
        }

        private boolean hasNext() {
            return valuePending || iterator.hasNext();
        }

        private Object next() {
            if (!(source instanceof Map<?, ?>)) return iterator.next();
            if (valuePending) return entry.getValue();
            entry = (Map.Entry<?, ?>) iterator.next();
            return entry.getKey();
        }

        private void accept(int childHash) {
            if (source instanceof List<?>) {
                hash = 31 * hash + childHash;
            } else if (!(source instanceof Map<?, ?>)) {
                hash += childHash;
            } else if (valuePending) {
                hash += keyHash ^ childHash;
                valuePending = false;
            } else {
                keyHash = childHash;
                valuePending = true;
            }
        }
    }

    private static boolean equal(Object left, Object right) {
        if (left == right) return true;
        if (!container(left) || !container(right)) return Objects.equals(left, right);
        Hashes hashes = new Hashes();
        // Identity pairs are directional and local to this comparison; mutable leaves are not cached.
        IdentityHashMap<Object, IdentityHashMap<Object, Boolean>> completed = new IdentityHashMap<>();
        ArrayDeque<EqualityFrame> pending = new ArrayDeque<>();
        pending.push(new EqualityFrame(left, right, hashes));
        while (!pending.isEmpty()) {
            EqualityFrame frame = pending.peek();
            if (frame.done()) {
                boolean matched = !frame.failed;
                pending.pop();
                if (pending.isEmpty()) return matched;
                completed.computeIfAbsent(frame.first, ignored -> new IdentityHashMap<>())
                        .put(frame.second, matched);
                pending.peek().accept(matched);
            } else {
                Pair pair = frame.next(hashes);
                if (pair == null) continue;
                if (pair.left == pair.right) {
                    frame.accept(true);
                } else if (!container(pair.left) || !container(pair.right)) {
                    frame.accept(Objects.equals(pair.left, pair.right));
                } else {
                    IdentityHashMap<Object, Boolean> matches = completed.get(pair.left);
                    Boolean matched = matches == null ? null : matches.get(pair.right);
                    if (matched != null) {
                        frame.accept(matched);
                    } else {
                        pending.push(new EqualityFrame(pair.left, pair.right, hashes));
                    }
                }
            }
        }
        return true;
    }

    private record Pair(Object left, Object right) { }

    /** Set matching retries collisions in the same explicit stack as List and Map comparisons. */
    private static final class EqualityFrame {
        private final Object first;
        private final Object second;
        private final Iterator<?> left;
        private final Iterator<?> right;
        private final Map<?, ?> rightMap;
        private final Map<Integer, List<Object>> candidates;
        private boolean failed;
        private Object member;
        private List<Object> bucket;
        private int candidate;

        private EqualityFrame(Object first, Object second, Hashes hashes) {
            this.first = first;
            this.second = second;
            if (first == second) {
                left = null;
                right = null;
                rightMap = null;
                candidates = null;
            } else if (first instanceof List<?> firstList && second instanceof List<?> secondList) {
                left = firstList.iterator();
                right = secondList.iterator();
                rightMap = null;
                candidates = null;
                failed = firstList.size() != secondList.size();
            } else if (first instanceof Map<?, ?> firstMap && second instanceof Map<?, ?> secondMap) {
                left = firstMap.entrySet().iterator();
                right = null;
                rightMap = secondMap;
                candidates = null;
                failed = firstMap.size() != secondMap.size();
            } else if (first instanceof Set<?> firstSet && second instanceof Set<?> secondSet) {
                // Set.equals uses containsAll: the other set's element is the lookup argument.
                left = secondSet.iterator();
                right = null;
                rightMap = null;
                candidates = new HashMap<>();
                failed = firstSet.size() != secondSet.size();
                if (!failed) {
                    for (Object item : firstSet) {
                        candidates.computeIfAbsent(hashes.hash(item), ignored -> new ArrayList<>()).add(item);
                    }
                }
            } else {
                left = null;
                right = null;
                rightMap = null;
                candidates = null;
                failed = !Objects.equals(first, second);
            }
        }

        private boolean done() {
            return failed || left == null || bucket == null && !left.hasNext();
        }

        private Pair next(Hashes hashes) {
            if (right != null) return new Pair(left.next(), right.next());
            if (rightMap != null) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) left.next();
                Object value = rightMap.get(entry.getKey());
                if (value == null && !rightMap.containsKey(entry.getKey())) {
                    failed = true;
                    return null;
                }
                return new Pair(entry.getValue(), value);
            }
            if (bucket == null) {
                member = left.next();
                bucket = candidates.get(hashes.hash(member));
                candidate = 0;
                if (bucket == null) {
                    failed = true;
                    return null;
                }
            }
            return new Pair(member, bucket.get(candidate++));
        }

        private void accept(boolean matched) {
            if (candidates == null) {
                failed = !matched;
            } else if (matched) {
                bucket = null;
                member = null;
            } else if (candidate == bucket.size()) {
                failed = true;
            }
        }
    }
}
