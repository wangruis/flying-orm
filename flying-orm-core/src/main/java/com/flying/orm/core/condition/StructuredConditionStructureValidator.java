package com.flying.orm.core.condition;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 在扩展适配器执行前限制条件树以及外部容器值，避免包装或序列化绕过统一预算。 */
final class StructuredConditionStructureValidator {

    /** 扩展值可以合法超过普通 IN 上限，但前端原始图仍不能要求无界遍历。 */
    private static final int MAX_RAW_VALUE_REFERENCES = 20_000;

    private StructuredConditionStructureValidator() {
    }

    static void validate(StructuredConditionInput input, StructuredConditionPolicy policy) {
        StructuredConditionInput safeInput = Objects.requireNonNull(input,
                                                                     "structured condition input must not be null");
        StructuredConditionPolicy safePolicy = Objects.requireNonNull(policy,
                                                                       "structured condition policy must not be null");
        validateNode(safeInput,
                     safePolicy,
                     new ConditionCompilationBudget(),
                     1,
                     ConditionCompilationBudget.ROOT_PATH);
    }

    private static void validateNode(StructuredConditionInput input,
                                     StructuredConditionPolicy policy,
                                     ConditionCompilationBudget budget,
                                     int depth,
                                     String path) {
        budget.checkNode(depth, policy, path);
        String valuePath = path + ".value";
        try {
            validateRawValue(input.stableValue(),
                             policy,
                             valuePath,
                             isStandardScalarArray(input, policy),
                             usesStandardValueSemantics(input, policy));
        } catch (StructuredConditionException error) {
            throw error;
        } catch (RuntimeException failure) {
            StructuredConditionValueNormalizer.rethrowVirtualMachineError(failure);
            throw StructuredConditionException.of(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                                  valuePath,
                                                  "structured condition value shape is not allowed at " + valuePath);
        }
        List<StructuredConditionInput> children = input.terms();
        for (int index = 0; index < children.size(); index++) {
            String childPath = ConditionCompilationBudget.childConditionPath(path, index);
            StructuredConditionInput child = children.get(index);
            if (child == null) {
                throw StructuredConditionException.of(StructuredConditionErrorCode.INVALID_NODE_SHAPE,
                                                      childPath,
                                                      "structured condition child must not be null at " + childPath);
            }
            // 策略把最大深度封顶在 64。递归深度固定有界，同时不会先为超大同级节点分配一整块栈内存。
            validateNode(child, policy, budget, depth + 1, childPath);
        }
    }

    /**
     * 只遍历前端常见的数组、Collection 和 Map；一次性 Iterable 留给值归一化器消费一次。
     * 使用对象身份维护当前活动路径，既拒绝循环容器，又允许共享但无环的普通值图。
     */
    private static void validateRawValue(Object value,
                                         StructuredConditionPolicy policy,
                                         String path,
                                         boolean preserveScalarArray,
                                         boolean enforceCollectionSize) {
        ArrayDeque<ValueNode> pending = new ArrayDeque<>();
        IdentityHashMap<Object, Boolean> active = new IdentityHashMap<>();
        int nodes = 0;
        int references = 0;
        if (value != null) {
            pending.addLast(new ValueNode(value, path, 1, false));
        }
        while (!pending.isEmpty()) {
            ValueNode node = pending.removeLast();
            Object current = node.value();
            if (node.exit()) {
                active.remove(current);
                continue;
            }
            if (current instanceof CharSequence text) {
                if (text.length() > policy.maxStringLength()) {
                    throw valueLimit(StructuredConditionErrorCode.VALUE_TOO_LONG,
                                     node.path(),
                                     "structured condition string value exceeds limit at ");
                }
                continue;
            }
            if (preserveScalarArray && current == value) {
                // byte[] 和 SQL Array 的 Java 数组是一个标量；元素数量不是 IN/扩展集合的容量预算。
                continue;
            }
            if (!isContainer(current)) {
                continue;
            }
            if (active.put(current, Boolean.TRUE) != null) {
                throw valueLimit(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                 node.path(),
                                 "structured condition value graph must not contain a cycle at ");
            }
            if (node.depth() > policy.maxDepth()) {
                throw valueLimit(StructuredConditionErrorCode.DEPTH_EXCEEDED,
                                 node.path(),
                                 "structured condition value depth exceeds limit at ");
            }
            nodes++;
            if (nodes > policy.maxNodes()) {
                throw valueLimit(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED,
                                 node.path(),
                                 "structured condition value node count exceeds limit at ");
            }
            pending.addLast(new ValueNode(current, node.path(), node.depth(), true));
            if (current.getClass().isArray()) {
                int length = Array.getLength(current);
                if (enforceCollectionSize) {
                    requireCollectionSize(length, policy, node.path());
                }
                for (int index = length - 1; index >= 0; index--) {
                    references = checkReferenceCount(references, node.path());
                    addValue(pending,
                             Array.get(current, index),
                             valuePath(node.path(), index),
                             node.depth() + 1);
                }
            } else if (current instanceof Collection<?> collection) {
                if (enforceCollectionSize) {
                    requireCollectionSize(collection.size(), policy, node.path());
                }
                int index = 0;
                for (Object item : collection) {
                    references = checkReferenceCount(references, node.path());
                    if (enforceCollectionSize) {
                        requireCollectionSize(index + 1, policy, node.path());
                    }
                    addValue(pending,
                             item,
                             valuePath(node.path(), index),
                             node.depth() + 1);
                    index++;
                }
            } else if (current instanceof Map<?, ?> map) {
                if (enforceCollectionSize) {
                    requireCollectionSize(map.size(), policy, node.path());
                }
                int index = 0;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    references = checkReferenceCount(references, node.path());
                    references = checkReferenceCount(references, node.path());
                    if (enforceCollectionSize) {
                        requireCollectionSize(index + 1, policy, node.path());
                    }
                    addValue(pending,
                             entry.getValue(),
                             valuePath(node.path(), index),
                             node.depth() + 1);
                    addValue(pending,
                             entry.getKey(),
                             valuePath(node.path(), index) + ".key",
                             node.depth() + 1);
                    index++;
                }
            }
        }
    }

    private static int checkReferenceCount(int references, String path) {
        int next = references + 1;
        if (next > MAX_RAW_VALUE_REFERENCES) {
            throw valueLimit(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED,
                             path,
                             "structured condition value reference count exceeds limit at ");
        }
        return next;
    }

    private static boolean isStandardScalarArray(StructuredConditionInput input,
                                                 StructuredConditionPolicy policy) {
        Object value = input.stableValue();
        if (value == null || !value.getClass().isArray() || input.operator() == null) {
            return false;
        }
        return policy.resolveOperator(input.operator())
                     .filter(policy::usesFieldValue)
                     .filter(operator -> policy.valueShape(operator) == ConditionValueShape.SCALAR)
                     .isPresent();
    }

    private static boolean usesStandardValueSemantics(StructuredConditionInput input,
                                                       StructuredConditionPolicy policy) {
        if (input.operator() == null) {
            return false;
        }
        return policy.resolveOperator(input.operator()).filter(policy::usesFieldValue).isPresent();
    }

    private static boolean isContainer(Object value) {
        return value.getClass().isArray() || value instanceof Collection<?> || value instanceof Map<?, ?>;
    }

    private static void addValue(ArrayDeque<ValueNode> pending, Object value, String path, int depth) {
        if (value != null) {
            pending.addLast(new ValueNode(value, path, depth, false));
        }
    }

    private static String valuePath(String path, int index) {
        return path + '[' + index + ']';
    }

    private static void requireCollectionSize(int size, StructuredConditionPolicy policy, String path) {
        if (size > policy.maxCollectionSize()) {
            throw valueLimit(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE,
                             path,
                             "structured condition collection value exceeds limit at ");
        }
    }

    private static StructuredConditionException valueLimit(StructuredConditionErrorCode code,
                                                            String path,
                                                            String message) {
        return StructuredConditionException.of(code, path, message + path);
    }

    private record ValueNode(Object value, String path, int depth, boolean exit) {
    }
}
