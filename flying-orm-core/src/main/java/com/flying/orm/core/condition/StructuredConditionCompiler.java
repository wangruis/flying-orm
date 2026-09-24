package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 前端结构化条件进入 ORM 的唯一编译入口。
 *
 * <p>编译器在一次遍历中完成树预算、策略授权和 AST 构造，字段值转换仍由专用归一化器负责。
 * 前端只能传数据结构，不能传 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class StructuredConditionCompiler {

    private final StructuredConditionValueNormalizer valueNormalizer;

    private StructuredConditionCompiler(ValueCodecRegistry valueCodecs) {
        this.valueNormalizer = new StructuredConditionValueNormalizer(
                Objects.requireNonNull(valueCodecs, "value codec registry must not be null"));
    }

    /** 创建使用内置值转换规则的条件编译器。 */
    public static StructuredConditionCompiler create() {
        return create(ValueCodecRegistry.standard());
    }

    /**
     * 创建使用应用级 codec 注册表的条件编译器。注册表构造完成后只读，可以被并发请求共享。
     *
     * @param valueCodecs 条件值转换使用的 codec 注册表
     * @return 编译器
     */
    public static StructuredConditionCompiler create(ValueCodecRegistry valueCodecs) {
        return new StructuredConditionCompiler(valueCodecs);
    }

    /**
     * 在任何输入适配器运行前，检查树预算以及外部 Map、Collection 和数组的有界值图。
     * 这里只执行资源边界，不按表单解释字段值，因此深层或超大输入不会先进入扩展序列化。
     */
    public static void validateStructure(StructuredConditionInput input, StructuredConditionPolicy policy) {
        StructuredConditionStructureValidator.validate(input, policy);
    }

    /** 使用默认策略编译前端条件。 */
    public ConditionGroup compile(DynamicForm form, StructuredConditionInput input) {
        return compile(form, input, StructuredConditionPolicy.defaults());
    }

    /**
     * 校验并编译前端条件为内部 AST。字段、操作符和值都保留原有的错误码和前端路径。
     */
    public ConditionGroup compile(DynamicForm form, StructuredConditionInput input, StructuredConditionPolicy policy) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        StructuredConditionInput safeInput = Objects.requireNonNull(
                input, "structured condition input must not be null");
        StructuredConditionPolicy safePolicy = Objects.requireNonNull(policy,
                "structured condition policy must not be null");
        ConditionCompilationBudget budget = new ConditionCompilationBudget();
        ArrayDeque<CompileFrame> pending = new ArrayDeque<>();
        pending.addLast(new CompileFrame(safeInput, ConditionCompilationBudget.Path.ROOT));
        while (true) {
            CompileFrame frame = pending.getLast();
            ConditionNode completed;
            if (frame.children == null) {
                budget.checkNode(frame.path, safePolicy);
                boolean hasTermShape = hasText(frame.input.field()) || hasText(frame.input.operator());
                boolean hasGroupShape = hasText(frame.input.logic()) || !frame.input.terms().isEmpty();
                if (hasTermShape == hasGroupShape) {
                    String path = frame.path.text();
                    throw StructuredConditionException.of(StructuredConditionErrorCode.INVALID_NODE_SHAPE,
                            path, "structured condition node must be either term or group at " + path);
                }
                if (hasTermShape) {
                    completed = compileTerm(safeForm, frame.input, safePolicy, budget, frame.path);
                } else {
                    frame.operator = parseLogic(frame.input.logic(), frame.path, safePolicy.maxStringLength());
                    if (frame.input.terms().isEmpty()) {
                        String path = frame.path.text();
                        throw StructuredConditionException.of(StructuredConditionErrorCode.EMPTY_GROUP,
                                path, "structured condition group must not be empty at " + path);
                    }
                    frame.children = new ArrayList<>(frame.input.terms().size());
                    continue;
                }
            } else if (frame.children.size() < frame.input.terms().size()) {
                int index = frame.children.size();
                StructuredConditionInput child = frame.input.terms().get(index);
                ConditionCompilationBudget.Path childPath = frame.path.child(index);
                if (child == null) {
                    String path = childPath.text();
                    throw StructuredConditionException.of(StructuredConditionErrorCode.INVALID_NODE_SHAPE,
                            path, "structured condition child must not be null at " + path);
                }
                pending.addLast(new CompileFrame(child, childPath));
                continue;
            } else {
                completed = ConditionGroup.owned(frame.operator, frame.children);
            }
            pending.removeLast();
            if (pending.isEmpty()) {
                return completed instanceof ConditionGroup group
                        ? group : ConditionGroup.owned(LogicalOperator.AND, List.of(completed));
            }
            pending.getLast().children.add(completed);
        }
    }

    private TermCondition compileTerm(DynamicForm form,
                                      StructuredConditionInput input,
                                      StructuredConditionPolicy policy,
                                      ConditionCompilationBudget budget,
                                      ConditionCompilationBudget.Path path) {
        String externalField = requireText(input.field(),
                StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                path, "field", "structured condition field",
                policy.maxStringLength());
        String externalOperator = requireText(input.operator(),
                StructuredConditionErrorCode.OPERATOR_NOT_ALLOWED,
                path, "operator", "structured condition operator",
                policy.maxStringLength());
        FieldIdentity externalIdentity = FieldIdentity.of(externalField);
        if (!policy.allowsField(externalIdentity)) {
            throw StructuredConditionException.field(StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                    path.property("field").text(),
                    externalField,
                    "structured condition field is not allowed at " + path.text());
        }
        if (!policy.allowsFieldOperator(externalIdentity, externalOperator)) {
            throw StructuredConditionException.term(StructuredConditionErrorCode.FIELD_OPERATOR_NOT_ALLOWED,
                    path.property("operator").text(),
                    externalField,
                    externalOperator,
                    "structured condition operator is not allowed for field at " + path.text());
        }
        DynamicField field = form.findField(externalField)
                .orElseThrow(() -> StructuredConditionException.field(
                        StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                        path.property("field").text(),
                        externalField,
                        "structured condition field does not exist at " + path.text()));
        String operator = policy.resolveOperator(externalOperator)
                .orElseThrow(() -> StructuredConditionException.term(
                        StructuredConditionErrorCode.OPERATOR_NOT_ALLOWED,
                        path.property("operator").text(),
                        externalField,
                        externalOperator,
                        "structured condition operator is not allowed at " + path.text()));
        budget.checkTerm(operator, policy, path);
        Object value = valueNormalizer.normalize(input.stableValue(),
                field,
                policy,
                path.property("value"),
                operator);
        return TermCondition.owned(field.identity(), operator, value);
    }

    private LogicalOperator parseLogic(String logic, ConditionCompilationBudget.Path path, int maxStringLength) {
        if (logic == null || logic.length() > maxStringLength || logic.isBlank()) {
            logic = requireText(logic, StructuredConditionErrorCode.LOGIC_NOT_ALLOWED,
                    path, "logic", "structured condition logic", maxStringLength);
        }
        String normalizedLogic = logic.trim().toLowerCase(Locale.ROOT);
        return switch (normalizedLogic) {
            case "and" -> LogicalOperator.AND;
            case "or" -> LogicalOperator.OR;
            default -> {
                String location = path.text();
                throw StructuredConditionException.of(StructuredConditionErrorCode.LOGIC_NOT_ALLOWED,
                        path.property("logic").text(),
                        "structured condition logic is not allowed at " + location);
            }
        };
    }

    private static final class CompileFrame {
        private final StructuredConditionInput input;
        private final ConditionCompilationBudget.Path path;
        private LogicalOperator operator;
        private List<ConditionNode> children;

        private CompileFrame(StructuredConditionInput input, ConditionCompilationBudget.Path path) {
            this.input = input;
            this.path = path;
        }
    }

    private static String requireText(String value,
                                      StructuredConditionErrorCode code,
                                      ConditionCompilationBudget.Path path,
                                      String property,
                                      String name,
                                      int maxStringLength) {
        if (value == null || value.length() > maxStringLength || value.isBlank()) {
            String reason = value != null && value.length() > maxStringLength
                    ? " exceeds the configured string limit" : " must not be blank";
            throw StructuredConditionException.of(code, path.property(property).text(),
                    name + " at " + path.text() + reason);
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
