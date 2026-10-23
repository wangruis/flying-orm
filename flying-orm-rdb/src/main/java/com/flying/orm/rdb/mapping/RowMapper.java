package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.internal.InternalApi;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * 把数据库行映射成业务对象的稳定扩展接口。
 *
 * <p>自定义映射可以直接实现这个函数式接口；常规 record/Bean 使用 {@link #of(Class)}。默认工厂会在内部
 * 编译可复用的反射计划，客户端的 {@link EntityModelRegistry} 按配置缓存计划；静态工厂不做全局缓存。
 * 具体计划类型不属于公共 API。实现对象可能被多个响应式订阅并发调用，自定义 mapper
 * 不应把某一行的临时状态保存在共享字段里。</p>
 *
 * @param <T> 目标对象类型
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
@FunctionalInterface
public interface RowMapper<T> {

    /**
     * 把一行列名到驱动值的 Map 转成目标对象。
     *
     * @param row 当前数据库行；调用期间只读
     * @return 映射后的业务对象
     * @throws MappingException 构造对象、写字段或值转换失败时抛出
     */
    T map(DynamicRow row);

    /**
     * 执行器按当前结果布局绑定一次；返回 null 表示继续使用普通行映射。
     * 绑定器仅使用列布局，返回的函数同步消费当前行的索引值，不得保留值读取器。
     * 函数属于本次结果消费，按行串行调用，不跨查询或线程共享。
     */
    @InternalApi
    default Function<IntFunction<Object>, T> bindColumns(DynamicRow columns) {
        return null;
    }

    /**
     * 读取 COUNT 标量为精确 long；优先使用 total/TOTAL 别名，否则读取首列。
     * 小数、非有限数或溢出值报错，不截断为错误的计数。
     */
    static RowMapper<Long> count() {
        return RowMapper::countValue;
    }

    private static Long countValue(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "count row must not be null");
        Object value = safeRow.get("total");
        if (value == null) {
            value = safeRow.get("TOTAL");
        }
        if (value == null && !safeRow.isEmpty()) {
            value = safeRow.value(0);
        }
        Long count = exactCount(value);
        if (count == null) {
            throw new IllegalArgumentException("count result must not be null");
        }
        return count;
    }

    /** 将驱动 COUNT 值无损转换为 long，保留 SQL NULL；拒绝小数、非有限数和溢出。 */
    static Long exactCount(Object value) {
        if (value == null) return null;
        if (value instanceof Long count) return count;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).longValue();
        }
        try {
            if (value instanceof BigInteger integer) return integer.longValueExact();
            if (value instanceof BigDecimal decimal) return decimal.longValueExact();
            if (value instanceof Number number) return new BigDecimal(number.toString()).longValueExact();
            if (value instanceof CharSequence text) return Long.parseLong(text.toString());
        } catch (ArithmeticException | NumberFormatException error) {
            throw new IllegalArgumentException("count result must be an exact long integer", error);
        }
        throw new IllegalArgumentException("count result must be an exact long integer");
    }

    /**
     * 把调用方自己准备的普通 Map 显式压缩后再映射。
     *
     * <p>数据库查询热路径始终直接调用 {@link #map(DynamicRow)}，不会经过这里。保留这个重载是为了让
     * 单元测试、自定义数据导入和手工构造行仍然好用，同时把普通 Map 的一次性转换边界写清楚。</p>
     */
    default T map(Map<String, Object> row) {
        return map(DynamicRow.copyOf(Objects.requireNonNull(row, "row must not be null")));
    }

    /**
     * 给数据库或原生 SQL 返回的列别名做显式映射。Map 的 key 是结果列标签，value 是实体字段名或列名。
     * 包装器按共享结果布局只做一次线性改名，不修改驱动返回的原 Map，也不会把别名规则放进全局缓存。
     */
    default RowMapper<T> withAliases(Map<String, String> aliases) {
        Map<String, String> safeAliases = indexAliases(aliases);
        if (safeAliases.isEmpty()) {
            return this;
        }
        RowMapper<T> delegate = this;
        UnaryOperator<String> renamer = name -> findAlias(safeAliases, name);
        return new RowMapper<>() {
            @Override
            public T map(DynamicRow row) {
                DynamicRow source = Objects.requireNonNull(row, "row must not be null");
                return delegate.map(source.renameColumnsBound(renamer));
            }

            @Override
            public Function<IntFunction<Object>, T> bindColumns(DynamicRow columns) {
                return delegate.bindColumns(columns.renameColumns(renamer));
            }
        };
    }

    private static String findAlias(Map<String, String> aliases, String name) {
        String safeName = Objects.requireNonNull(name, "row column label must not be null");
        return aliases.getOrDefault(FieldIdentity.of(safeName).key(), safeName);
    }

    private static Map<String, String> indexAliases(Map<String, String> aliases) {
        Map<String, String> source = Objects.requireNonNull(aliases, "row aliases must not be null");
        Map<String, String> indexed = new HashMap<>(source.size());
        for (Map.Entry<String, String> entry : source.entrySet()) {
            String key = FieldIdentity.of(entry.getKey()).key();
            String target = requireText(entry.getValue(), "row alias target");
            String previous = indexed.putIfAbsent(key, target);
            if (previous != null && !previous.equals(target)) {
                throw new IllegalArgumentException("row aliases contain conflicting column");
            }
        }
        return Map.copyOf(indexed);
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

    /**
     * 使用标准业务类型规则创建可并发复用的默认 mapper。
     *
     * @param type 目标 record 或带无参构造器的 Bean 类型
     * @param <T> 目标对象类型
     * @return 预编译、可复用的行映射器
     */
    static <T> RowMapper<T> of(Class<T> type) {
        return MappingPlan.of(type);
    }

    /**
     * 使用应用级 codec 注册表创建对象映射器，保证实体回读和写入参数采用同一套业务类型规则。
     *
     * @param type 目标对象类型
     * @param valueCodecs 应用级 codec 注册表
     * @param <T> 目标对象类型
     * @return 可并发复用的行映射器
     */
    static <T> RowMapper<T> of(Class<T> type, ValueCodecRegistry valueCodecs) {
        return MappingPlan.of(type, valueCodecs);
    }

    /**
     * 按点分列名组装嵌套 Map，例如 {@code department.id}；保留列名大小写、顺序和 NULL。
     * 路径各段不能为空，同一路径不能同时是值和子对象。默认查询仍返回平铺的紧凑行。
     */
    static RowMapper<Map<String, Object>> nested() {
        return MappingPlan.nestedMap();
    }

    /**
     * 按点分属性路径映射单行嵌套 Bean/record，例如 {@code department.id}。
     * 子对象已映射的列全部为 SQL NULL 时返回 null；未投影属性仍沿用普通映射规则。
     * 本入口只组装当前行，不发起关联查询，也不聚合多行集合。
     */
    static <T> RowMapper<T> nested(Class<T> type) {
        return nested(type, ValueCodecRegistry.standard());
    }

    /** 使用应用 codec 创建可复用的嵌套行映射器；路径与字段绑定仅在结果布局变化时创建。 */
    static <T> RowMapper<T> nested(Class<T> type, ValueCodecRegistry valueCodecs) {
        return MappingPlan.nested(type, valueCodecs);
    }

    /**
     * 创建带读后事件的默认映射器。监听器只是包在不可变缓存计划外层，不会进入全局缓存键，
     * 因而不同客户端安装不同监听器时不会互相串事件。
     *
     * @param type 目标对象类型
     * @param listener 应用级线程安全监听器
     * @param <T> 目标对象类型
     * @return 带当前监听器的映射器
     */
    static <T> RowMapper<T> of(Class<T> type, EntityMappingListener listener) {
        return observed(type, MappingPlan.of(type), listener);
    }

    /** 使用自定义 codec 和监听器创建映射器。 */
    static <T> RowMapper<T> of(Class<T> type,
                               ValueCodecRegistry valueCodecs,
                               EntityMappingListener listener) {
        return observed(type, MappingPlan.of(type, valueCodecs), listener);
    }

    private static <T> RowMapper<T> observed(Class<T> type,
                                              RowMapper<T> delegate,
                                              EntityMappingListener listener) {
        EntityMappingListener safeListener = java.util.Objects.requireNonNull(
                listener, "entity mapping listener must not be null");
        if (safeListener == EntityMappingListener.NONE) {
            return delegate;
        }
        EntityMetadata<T> metadata = EntityMetadataResolver.createUncached(type);
        return row -> {
            T entity = delegate.map(row);
            safeListener.afterRead(new EntityMappingEvent(metadata, entity, row));
            return entity;
        };
    }
}
