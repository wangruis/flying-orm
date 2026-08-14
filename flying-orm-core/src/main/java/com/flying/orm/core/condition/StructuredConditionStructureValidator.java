package com.flying.orm.core.condition;

import java.util.List;
import java.util.Objects;

/** 只负责条件树的资源保护，不读取字段、操作符或 value。 */
final class StructuredConditionStructureValidator {

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
}
