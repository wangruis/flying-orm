package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 在一次遍历中把前端节点的结构预算、策略授权和值规范化编译为内部条件 AST。
 * 字段和操作符始终先经过策略校验，再进入值转换，未授权输入不会流向 SQL 渲染层。
 */
final class StructuredConditionAstCompiler {

    private final StructuredConditionValueNormalizer valueNormalizer;

    StructuredConditionAstCompiler(ValueCodecRegistry valueCodecs) {
        this.valueNormalizer = new StructuredConditionValueNormalizer(valueCodecs);
    }

    ConditionGroup compile(DynamicForm form, StructuredConditionInput input, StructuredConditionPolicy policy) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        StructuredConditionInput safeInput = Objects.requireNonNull(input, "structured condition input must not be null");
        StructuredConditionPolicy safePolicy = Objects.requireNonNull(policy,
                                                                       "structured condition policy must not be null");
        ConditionNode node = compileNode(safeForm,
                                         safeInput,
                                         safePolicy,
                                         new ConditionCompilationBudget(),
                                         1,
                                         ConditionCompilationBudget.ROOT_PATH);
        return node instanceof ConditionGroup group
                ? group : ConditionGroup.owned(LogicalOperator.AND, List.of(node));
    }

    private ConditionNode compileNode(DynamicForm form,
                                      StructuredConditionInput input,
                                      StructuredConditionPolicy policy,
                                      ConditionCompilationBudget budget,
                                      int depth,
                                      String path) {
        budget.checkNode(depth, policy, path);
        boolean hasTermShape = hasText(input.field()) || hasText(input.operator());
        boolean hasGroupShape = hasText(input.logic()) || !input.terms().isEmpty();
        if (hasTermShape == hasGroupShape) {
            throw StructuredConditionException.of(StructuredConditionErrorCode.INVALID_NODE_SHAPE,
                                                  path,
                                                  "structured condition node must be either term or group at " + path);
        }
        return hasTermShape
                ? compileTerm(form, input, policy, path)
                : compileGroup(form, input, policy, budget, depth, path);
    }

    private TermCondition compileTerm(DynamicForm form,
                                      StructuredConditionInput input,
                                      StructuredConditionPolicy policy,
                                      String path) {
        String externalField = requireText(input.field(),
                                           StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                                           ConditionCompilationBudget.propertyPath(path, "field"),
                                           "structured condition field at " + path,
                                           policy.maxStringLength());
        String externalOperator = requireText(input.operator(),
                                               StructuredConditionErrorCode.OPERATOR_NOT_ALLOWED,
                                               ConditionCompilationBudget.propertyPath(path, "operator"),
                                               "structured condition operator at " + path,
                                               policy.maxStringLength());
        FieldIdentity externalIdentity = FieldIdentity.of(externalField);
        if (!policy.allowsField(externalIdentity)) {
            throw StructuredConditionException.field(StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                                                     ConditionCompilationBudget.propertyPath(path, "field"),
                                                     externalField,
                                                     "structured condition field is not allowed at " + path);
        }
        if (!policy.allowsFieldOperator(externalIdentity, externalOperator)) {
            throw StructuredConditionException.term(StructuredConditionErrorCode.FIELD_OPERATOR_NOT_ALLOWED,
                                                    ConditionCompilationBudget.propertyPath(path, "operator"),
                                                    externalField,
                                                    externalOperator,
                                                    "structured condition operator is not allowed for field at "
                                                            + path);
        }
        DynamicField field = form.findField(externalField)
                                 .orElseThrow(() -> StructuredConditionException.field(
                                         StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                                         ConditionCompilationBudget.propertyPath(path, "field"),
                                         externalField,
                                         "structured condition field does not exist at " + path));
        String operator = policy.resolveOperator(externalOperator)
                                .orElseThrow(() -> StructuredConditionException.term(
                                        StructuredConditionErrorCode.OPERATOR_NOT_ALLOWED,
                                        ConditionCompilationBudget.propertyPath(path, "operator"),
                                        externalField,
                                        externalOperator,
                                        "structured condition operator is not allowed at " + path));
        Object value = valueNormalizer.normalize(input.stableValue(),
                                                 field,
                                                 policy,
                                                 ConditionCompilationBudget.propertyPath(path, "value"),
                                                 operator);
        return TermCondition.owned(field.identity(), operator, value);
    }

    private ConditionGroup compileGroup(DynamicForm form,
                                        StructuredConditionInput input,
                                        StructuredConditionPolicy policy,
                                        ConditionCompilationBudget budget,
                                        int depth,
                                        String path) {
        LogicalOperator operator = parseLogic(input.logic(), path, policy.maxStringLength());
        if (input.terms().isEmpty()) {
            throw StructuredConditionException.of(StructuredConditionErrorCode.EMPTY_GROUP,
                                                  path,
                                                  "structured condition group must not be empty at " + path);
        }
        List<ConditionNode> children = new ArrayList<>(input.terms().size());
        for (int index = 0; index < input.terms().size(); index++) {
            String childPath = ConditionCompilationBudget.childConditionPath(path, index);
            StructuredConditionInput child = input.terms().get(index);
            if (child == null) {
                throw StructuredConditionException.of(StructuredConditionErrorCode.INVALID_NODE_SHAPE,
                                                      childPath,
                                                      "structured condition child must not be null at " + childPath);
            }
            children.add(compileNode(form, child, policy, budget, depth + 1, childPath));
        }
        return ConditionGroup.owned(operator, children);
    }

    private LogicalOperator parseLogic(String logic, String path, int maxStringLength) {
        String logicPath = ConditionCompilationBudget.propertyPath(path, "logic");
        String normalizedLogic = requireText(logic,
                                             StructuredConditionErrorCode.LOGIC_NOT_ALLOWED,
                                             logicPath,
                                             "structured condition logic at " + path,
                                             maxStringLength).toLowerCase(Locale.ROOT);
        return switch (normalizedLogic) {
            case "and" -> LogicalOperator.AND;
            case "or" -> LogicalOperator.OR;
            default -> throw StructuredConditionException.of(StructuredConditionErrorCode.LOGIC_NOT_ALLOWED,
                                                             logicPath,
                                                             "structured condition logic is not allowed at " + path);
        };
    }

    private static String requireText(String value,
                                      StructuredConditionErrorCode code,
                                      String path,
                                      String name,
                                      int maxStringLength) {
        if (value == null) {
            throw StructuredConditionException.of(code, path, name + " must not be blank");
        }
        if (value.length() > maxStringLength) {
            throw StructuredConditionException.of(code, path, name + " exceeds the configured string limit");
        }
        if (value.isBlank()) {
            throw StructuredConditionException.of(code, path, name + " must not be blank");
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
