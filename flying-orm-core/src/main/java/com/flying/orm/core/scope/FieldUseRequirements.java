package com.flying.orm.core.scope;

import com.flying.orm.core.field.FieldIdentity;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SQL 规划阶段得到的、与具体调用方授权无关的字段用途要求。
 *
 * <p>对象只保存结构事实，可以跟随受治理的形状计划复用；批准结果不在这里缓存。每次调用仍要用
 * 当前 {@link FieldUsePolicy} 和 {@link FieldScope} 生成新的 {@link FieldUseSnapshot}。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class FieldUseRequirements {

    private static final FieldUseRequirements EMPTY = new FieldUseRequirements(List.of());

    private final List<Requirement> requirements;

    private FieldUseRequirements(List<Requirement> requirements) {
        this.requirements = List.copyOf(requirements);
    }

    public static FieldUseRequirements empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Requirement> requirements() {
        return requirements;
    }

    public boolean isEmpty() {
        return requirements.isEmpty();
    }

    /** 一项规范化后的字段用途事实。 */
    public record Requirement(String field, FieldUse use, FieldUseOrigin origin) {

        public Requirement {
            field = FieldIdentity.of(field).key();
            use = Objects.requireNonNull(use, "field use must not be null");
            origin = Objects.requireNonNull(origin, "field use origin must not be null");
        }
    }

    /** 构建器只在一次规划遍历中使用；build 后的对象与后续修改完全隔离。 */
    public static final class Builder {

        private final Set<Requirement> requirements = new LinkedHashSet<>();

        public Builder require(String field, FieldUse use) {
            return require(field, use, FieldUseOrigin.CALLER);
        }

        public Builder require(String field, FieldUse use, FieldUseOrigin origin) {
            requirements.add(new Requirement(field, use, origin));
            return this;
        }

        public FieldUseRequirements build() {
            return requirements.isEmpty()
                    ? EMPTY
                    : new FieldUseRequirements(new ArrayList<>(requirements));
        }
    }
}
