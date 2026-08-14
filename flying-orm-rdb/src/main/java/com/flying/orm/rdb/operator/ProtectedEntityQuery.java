package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.protection.ProtectedConditions;

/**
 * 实体 Lambda 查询共享的轻量保护搜索与显示控制。
 *
 * <p>这些方法只构造受控条件 AST；字段是否声明相应搜索模式仍由执行前的保护元数据校验。</p>
 *
 * @param <T> 实体类型
 * @param <SELF> 具体查询门面
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
public interface ProtectedEntityQuery<T, SELF> {

    /** 使用已注册条件运算符追加一个 AND 条件。 */
    SELF and(EntityProperty<T, ?> property, String operator, Object value);

    /** 设置本次查询对已声明脱敏字段的显示方式。 */
    SELF sensitiveDisplay(SensitiveDisplayMode mode);

    /** 精确匹配声明了 EXACT 的加密字段。 */
    default SELF exactEncrypted(EntityProperty<T, ?> property, Object value) {
        return and(property, ProtectedConditions.EXACT, value);
    }

    /** 后缀匹配声明了对应固定长度的加密字段。 */
    default SELF suffixEncrypted(EntityProperty<T, ?> property, Object value) {
        return and(property, ProtectedConditions.SUFFIX, value);
    }

    /** 包含匹配声明了 CONTAINS 的加密字段。 */
    default SELF containsEncrypted(EntityProperty<T, ?> property, Object value) {
        return and(property, ProtectedConditions.CONTAINS, value);
    }

    /** 使用字段声明的默认显示方式。 */
    default SELF declaredDisplay() {
        return sensitiveDisplay(SensitiveDisplayMode.DECLARED);
    }

    /** 强制对已声明脱敏字段返回脱敏值。 */
    default SELF masked() {
        return sensitiveDisplay(SensitiveDisplayMode.MASKED);
    }

    /** 对可信调用显式返回完整值；日志、异常和观测仍保持脱敏。 */
    default SELF showSensitive() {
        return sensitiveDisplay(SensitiveDisplayMode.FULL);
    }
}
