package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicForm;

import java.util.Objects;

/**
 * 前端结构化条件进入 ORM 的唯一编译入口。
 *
 * <p>这个门面只保留公开 API 和一次调用的入口校验。树形 AST 编译和值归一化分别交给包内协作者，
 * 因而不会把字段授权、节点预算和类型转换揉在同一个大类里。前端仍然只能传数据结构，不能传 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class StructuredConditionCompiler {

    private final StructuredConditionAstCompiler astCompiler;

    private StructuredConditionCompiler(ValueCodecRegistry valueCodecs) {
        this.astCompiler = new StructuredConditionAstCompiler(
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
        return astCompiler.compile(form, input, policy);
    }
}
