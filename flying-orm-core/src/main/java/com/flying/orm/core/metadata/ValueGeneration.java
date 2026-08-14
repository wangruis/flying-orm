package com.flying.orm.core.metadata;

import java.util.Objects;

/**
 * 描述列值由谁生成。NONE 表示由调用方正常传值；IDENTITY 交给数据库标识列；SEQUENCE 使用明确命名的序列。
 *
 * <p>这里只保存数据库都能理解的最小参数，不直接拼 SQL。具体是 Oracle 的
 * {@code generated ... as identity}、{@code sequence.nextval}，还是 SQL Server 的 {@code identity(seed, increment)}，
 * 由 RDB 方言在 DDL 阶段决定。对象构造后只读，可以跟随表单元数据安全缓存。</p>
 *
 * @param strategy     生成方式
 * @param sequenceName SEQUENCE 使用的名字，其他方式必须为空
 * @param startWith    第一个值
 * @param incrementBy  每次增长多少，不能为零
 * @param cacheSize    数据库预取数量；0 表示不显式声明缓存参数
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record ValueGeneration(Strategy strategy,
                              String sequenceName,
                              long startWith,
                              long incrementBy,
                              int cacheSize) {

    /** 默认缓存一百个值，减少高并发主键生成时对序列元数据的争用。 */
    private static final int DEFAULT_CACHE_SIZE = 100;

    public ValueGeneration {
        strategy = Objects.requireNonNull(strategy, "value generation strategy must not be null");
        if (incrementBy == 0) {
            throw new IllegalArgumentException("value generation increment must not be zero");
        }
        if (cacheSize < 0) {
            throw new IllegalArgumentException("value generation cache size must not be negative");
        }
        if (strategy == Strategy.SEQUENCE) {
            sequenceName = MetadataNames.requireText(sequenceName, "value generation sequence name");
        } else if (sequenceName != null) {
            throw new IllegalArgumentException("only sequence generation accepts a sequence name");
        }
        if (strategy == Strategy.NONE && (startWith != 1 || incrementBy != 1 || cacheSize != 0)) {
            throw new IllegalArgumentException("NONE value generation cannot carry sequence options");
        }
    }

    /** @return 普通调用方赋值方式 */
    public static ValueGeneration none() {
        return new ValueGeneration(Strategy.NONE, null, 1, 1, 0);
    }

    /** @return 从 1 开始、步长为 1 的标识列 */
    public static ValueGeneration identity() {
        return identity(1, 1, DEFAULT_CACHE_SIZE);
    }

    /** @return 带明确起点、步长和缓存大小的标识列 */
    public static ValueGeneration identity(long startWith, long incrementBy, int cacheSize) {
        return new ValueGeneration(Strategy.IDENTITY, null, startWith, incrementBy, cacheSize);
    }

    /** @return 从 1 开始、步长为 1 的命名序列 */
    public static ValueGeneration sequence(String name) {
        return sequence(name, 1, 1, DEFAULT_CACHE_SIZE);
    }

    /** @return 带明确起点、步长和缓存大小的命名序列 */
    public static ValueGeneration sequence(String name, long startWith, long incrementBy, int cacheSize) {
        return new ValueGeneration(Strategy.SEQUENCE, name, startWith, incrementBy, cacheSize);
    }

    /** @return 是否由数据库生成，而不是普通业务参数 */
    public boolean generated() {
        return strategy != Strategy.NONE;
    }

    public enum Strategy {
        NONE,
        IDENTITY,
        SEQUENCE
    }
}
