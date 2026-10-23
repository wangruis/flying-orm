package com.flying.orm.core.lambda;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * 已绑定实体的直接 Getter 或 record 访问器，例如 {@code user::getName}。
 *
 * @author wangr
 * @version v4.1.2
 */
@FunctionalInterface
public interface BoundEntityProperty<R> extends Supplier<R>, Serializable {
}
