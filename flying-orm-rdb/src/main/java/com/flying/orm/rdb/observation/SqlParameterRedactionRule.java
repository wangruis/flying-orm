package com.flying.orm.rdb.observation;

/**
 * 在内置参数脱敏之外，再指定哪些参数必须完全隐藏。
 *
 * <p>规则只会拿到参数位置和 Java 类型，不会拿到参数正文。返回 {@code true} 时日志固定写成
 * {@code <masked>}；返回 {@code false} 时仍会继续执行 flying-orm 的内置脱敏和长度限制，
 * 所以上层扩展只能把保护做得更严，不能把密码、二进制内容或超长文本重新放进日志。</p>
 *
 * <p>参数位置从 0 开始。参数为 {@code null} 时类型传入 {@link Object}，上层仍可按位置隐藏它。</p>
 *
 * @author wangr
 * @version v1.0
 */
@FunctionalInterface
public interface SqlParameterRedactionRule {

    /**
     * 判断指定参数是否必须完全隐藏。
     *
     * @param parameterIndex 参数在 {@code SqlRequest.parameters()} 中的位置，从 0 开始
     * @param valueType 参数运行时类型；参数为 null 时是 {@link Object}
     * @return true 表示完全隐藏，false 表示交给内置脱敏继续处理
     */
    boolean fullyMask(int parameterIndex, Class<?> valueType);

    /** 默认不追加特殊规则，所有参数仍受内置脱敏保护。 */
    static SqlParameterRedactionRule none() {
        return (parameterIndex, valueType) -> false;
    }
}
