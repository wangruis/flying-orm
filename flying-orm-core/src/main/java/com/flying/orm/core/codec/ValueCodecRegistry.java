package com.flying.orm.core.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * 值转换的统一入口。动态表单、条件参数和实体映射共享同一份只读注册表，避免同一个值在不同链路被解释成不同类型。
 *
 * <p>注册顺序就是优先级。这个类只负责组合、查找和驱动值解包；具体数值、时间、文本等转换由独立 codec 负责，
 * 既保留热路径上的直接匹配，也让每类转换规则有清楚的归属。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class ValueCodecRegistry {

    private static final ValueCodecRegistry STANDARD = new ValueCodecRegistry(StandardValueCodecs.create());

    private final List<ValueCodec> codecs;

    private final List<DriverValueAdapter> driverAdapters;

    /** 创建按声明顺序匹配的只读 codec 注册表。 */
    public ValueCodecRegistry(List<ValueCodec> codecs) {
        this(codecs, List.of());
    }

    private ValueCodecRegistry(List<ValueCodec> codecs, List<DriverValueAdapter> driverAdapters) {
        this.codecs = List.copyOf(Objects.requireNonNull(codecs, "value codecs must not be null"));
        this.driverAdapters = List.copyOf(Objects.requireNonNull(driverAdapters,
                                                                  "driver value adapters must not be null"));
    }

    /** 返回框架内置的共享注册表，常规场景无需为每次查询重复创建。 */
    public static ValueCodecRegistry standard() {
        return STANDARD;
    }

    /**
     * 把业务 codec 放在最前面并返回新注册表。第一个匹配项生效，原注册表保持不变，可安全并发共享。
     */
    public ValueCodecRegistry withFirst(ValueCodec codec) {
        ValueCodec safeCodec = Objects.requireNonNull(codec, "value codec must not be null");
        List<ValueCodec> combined = new ArrayList<>(codecs.size() + 1);
        combined.add(safeCodec);
        combined.addAll(codecs);
        return new ValueCodecRegistry(combined, driverAdapters);
    }

    /** 在标准转换前增加驱动值解包器。新的 adapter 优先匹配，原注册表不受影响。 */
    public ValueCodecRegistry withDriverAdapter(DriverValueAdapter adapter) {
        DriverValueAdapter safeAdapter = Objects.requireNonNull(adapter, "driver value adapter must not be null");
        List<DriverValueAdapter> combined = new ArrayList<>(driverAdapters.size() + 1);
        combined.add(safeAdapter);
        combined.addAll(driverAdapters);
        return new ValueCodecRegistry(codecs, combined);
    }

    /** 把 Java 值整理为数据库驱动可绑定的形态，空值保持为空。 */
    public Object write(Object value) {
        return value == null ? null : find(value.getClass()).write(value);
    }

    /**
     * 把驱动返回值转成调用方要求的 Java 类型。目标类型已经接住值时直接返回，避免多余解析和对象分配。
     */
    public <T> T read(Object value, Class<T> targetType) {
        return read(value, targetType, null);
    }

    /**
     * 允许上层在最终选中内置 Java-time codec 时提供驱动值 fallback。
     * 已注册的 driver adapter、目标类型快路和业务 codec 始终先于 fallback，Core 因而无需依赖具体驱动类型。
     */
    public <T> T read(Object value,
                      Class<T> targetType,
                      BiFunction<Object, Class<?>, Object> standardJavaTimeFallback) {
        Class<T> safeTargetType = ValueCodecTypeSupport.boxed(
                Objects.requireNonNull(targetType, "target type must not be null"));
        if (value == null) {
            return null;
        }
        Object adapted = adapt(value);
        if (safeTargetType.isInstance(adapted)) {
            return safeTargetType.cast(adapted);
        }
        ValueCodec codec = find(safeTargetType);
        Object codecValue = adapted;
        if (standardJavaTimeFallback != null && codec instanceof JavaTimeValueCodec) {
            codecValue = Objects.requireNonNull(
                    standardJavaTimeFallback.apply(adapted, safeTargetType),
                    "standard Java time fallback must not return null for non-null input");
            if (safeTargetType.isInstance(codecValue)) {
                return safeTargetType.cast(codecValue);
            }
        }
        return readWithCodec(codec, codecValue, safeTargetType);
    }

    private Object adapt(Object value) {
        for (DriverValueAdapter adapter : driverAdapters) {
            if (adapter.supports(value)) {
                return Objects.requireNonNull(adapter.unwrap(value),
                                              "driver value adapter must not return null for non-null input");
            }
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T> T readWithCodec(ValueCodec codec, Object value, Class<T> targetType) {
        return (T) codec.read(value, targetType);
    }

    private ValueCodec find(Class<?> targetType) {
        // 第一个匹配项获胜，扩展注册表时具体类型必须排在通用类型前。
        for (ValueCodec codec : codecs) {
            if (codec.supports(targetType)) {
                return codec;
            }
        }
        throw new IllegalArgumentException("no value codec for " + targetType.getName());
    }

}
