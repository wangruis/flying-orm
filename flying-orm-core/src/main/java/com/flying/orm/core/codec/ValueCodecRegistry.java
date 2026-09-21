package com.flying.orm.core.codec;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * 值转换的统一入口。动态表单、条件参数和实体映射共享同一份只读注册表，避免同一个值在不同链路被解释成不同类型。
 *
 * <p>注册顺序就是优先级。注册表统一拥有内建 codec 目录、查找和驱动值解包；具体转换由私有嵌套实现负责，
 * 既保留热路径上的直接匹配，也让内建规则只有一个所有者。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class ValueCodecRegistry {

    private static final StableDigest.Domain DESCRIPTOR_FINGERPRINT_DOMAIN =
            StableDigest.domain("value-codec-registry-descriptors/v1");

    private static final DescriptorState NO_DESCRIPTORS = new DescriptorState(
            false,
            StableDigest.sha256(DESCRIPTOR_FINGERPRINT_DOMAIN)
                        .integer("DESCRIPTOR_COUNT", 0)
                        .finishHex());

    private static final ValueCodecRegistry STANDARD = new ValueCodecRegistry(List.of(
            new EnumValueCodec(),
            new BooleanValueCodec(),
            new NumberValueCodec(),
            new JavaTimeValueCodec(),
            new UuidValueCodec(),
            new BinaryValueCodec(),
            new TextValueCodec(),
            new ArrayIdentityValueCodec(),
            new IdentityValueCodec()));

    private final List<ValueCodec> codecs;

    private final List<DriverValueAdapter> driverAdapters;

    private final boolean hasDescriptors;

    private final String descriptorFingerprint;

    /** 创建按声明顺序匹配的只读 codec 注册表。 */
    public ValueCodecRegistry(List<ValueCodec> codecs) {
        this(codecs, List.of());
    }

    private ValueCodecRegistry(List<ValueCodec> codecs, List<DriverValueAdapter> driverAdapters) {
        this.codecs = List.copyOf(Objects.requireNonNull(codecs, "value codecs must not be null"));
        this.driverAdapters = List.copyOf(Objects.requireNonNull(driverAdapters,
                                                                  "driver value adapters must not be null"));
        DescriptorState descriptorState = descriptorState(this.codecs);
        this.hasDescriptors = descriptorState.present();
        this.descriptorFingerprint = descriptorState.fingerprint();
    }

    /** 返回框架内置的共享注册表，常规场景无需为每次查询重复创建。 */
    public static ValueCodecRegistry standard() {
        return STANDARD;
    }

    /** @return 当前注册表是否包含可配置装配所需的显式 codec 描述器 */
    public boolean hasDescriptors() {
        return hasDescriptors;
    }

    /**
     * 返回按 codec 优先级冻结的描述器指纹。旧 codec 仍由注册表对象身份隔离，不伪造稳定契约。
     */
    public String descriptorFingerprint() {
        return descriptorFingerprint;
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
     * 允许上层为内置 Java-time 转换或尚未注册的目标类型提供 fallback。
     * 已注册的 driver adapter、目标类型快路和业务 codec 始终先于 fallback，Core 因而无需依赖具体驱动类型。
     */
    public <T> T read(Object value,
                      Class<T> targetType,
                      BiFunction<Object, Class<?>, Object> standardFallback) {
        Class<T> safeTargetType = ValueCodecTypeSupport.boxed(
                Objects.requireNonNull(targetType, "target type must not be null"));
        if (value == null) {
            return null;
        }
        Object adapted = adapt(value);
        if (safeTargetType.isInstance(adapted)) {
            return safeTargetType.cast(adapted);
        }
        ValueCodec codec = findOrNull(safeTargetType);
        Object codecValue = adapted;
        if (standardFallback != null && (codec == null || codec instanceof JavaTimeValueCodec)) {
            codecValue = Objects.requireNonNull(
                    standardFallback.apply(adapted, safeTargetType),
                    "standard value fallback must not return null for non-null input");
            if (safeTargetType.isInstance(codecValue)) {
                return safeTargetType.cast(codecValue);
            }
        }
        if (codec == null) {
            throw new IllegalArgumentException("no value codec for " + safeTargetType.getName());
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
        ValueCodec codec = findOrNull(targetType);
        if (codec == null) {
            throw new IllegalArgumentException("no value codec for " + targetType.getName());
        }
        return codec;
    }

    private ValueCodec findOrNull(Class<?> targetType) {
        // 第一个匹配项获胜，扩展注册表时具体类型必须排在通用类型前。
        for (ValueCodec codec : codecs) {
            if (codec.supports(targetType)) {
                return codec;
            }
        }
        return null;
    }

    private static DescriptorState descriptorState(List<ValueCodec> codecs) {
        List<ValueCodecDescriptor> descriptors = new ArrayList<>();
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        for (ValueCodec codec : codecs) {
            Optional<ValueCodecDescriptor> descriptor = Objects.requireNonNull(
                    Objects.requireNonNull(codec, "value codec must not be null").descriptor(),
                    "value codec descriptor lookup must not return null");
            if (descriptor.isEmpty()) {
                continue;
            }
            ValueCodecDescriptor value = descriptor.orElseThrow();
            if (!ids.add(value.id())) {
                throw new IllegalArgumentException("duplicate value codec descriptor id: " + value.id());
            }
            descriptors.add(value);
        }
        if (descriptors.isEmpty()) {
            return NO_DESCRIPTORS;
        }
        StableEncoder encoder = StableDigest.sha256(DESCRIPTOR_FINGERPRINT_DOMAIN);
        encoder.integer("DESCRIPTOR_COUNT", descriptors.size());
        for (int index = 0; index < descriptors.size(); index++) {
            encoder.integer("PRIORITY", index)
                   .text("DESCRIPTOR", descriptors.get(index).fingerprint());
        }
        return new DescriptorState(!descriptors.isEmpty(), encoder.finishHex());
    }

    private record DescriptorState(boolean present, String fingerprint) {
    }

    /** 枚举按常量名持久化，避免 ordinal 随声明顺序变化而破坏历史数据。 */
    private static final class EnumValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            // 带独立方法实现的枚举常量，其运行时类型是枚举的子类，仍按声明名称绑定。
            return Enum.class.isAssignableFrom(targetType);
        }

        @Override
        public Object write(Object value) {
            return ((Enum<?>) value).name();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Object read(Object value, Class<?> targetType) {
            try {
                return Enum.valueOf((Class<? extends Enum>) targetType, ValueCodecTypeSupport.text(value));
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException("value cannot be converted to enum");
            }
        }
    }

    /** 兼容常见数据库布尔表示，同时拒绝含义不明确的文本。 */
    private static final class BooleanValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Boolean.class;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (value instanceof Number number) {
                // 不能借 intValue() 判断真假，大整数低位为 0 或小数会被错误解释。
                return ValueCodecTypeSupport.decimal(number).compareTo(BigDecimal.ZERO) != 0;
            }
            return switch (ValueCodecTypeSupport.text(value).toLowerCase(Locale.ROOT)) {
                case "true", "1", "yes", "y", "on" -> true;
                case "false", "0", "no", "n", "off" -> false;
                default -> throw new IllegalArgumentException("boolean value is not supported");
            };
        }
    }

    /** 数值先收口到 BigDecimal，再做精确整数或显式浮点转换。 */
    private static final class NumberValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return Number.class.isAssignableFrom(targetType);
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            BigDecimal number = ValueCodecTypeSupport.decimal(value);
            if (targetType == Integer.class) {
                return ValueCodecTypeSupport.exactInteger(number);
            }
            if (targetType == Long.class) {
                return ValueCodecTypeSupport.exactLong(number);
            }
            if (targetType == BigDecimal.class) {
                return number;
            }
            if (targetType == BigInteger.class) {
                return ValueCodecTypeSupport.exactBigInteger(number);
            }
            if (targetType == Double.class) {
                double converted = number.doubleValue();
                if (!Double.isFinite(converted)) {
                    throw new IllegalArgumentException("number is out of double range");
                }
                return converted;
            }
            if (targetType == Float.class) {
                float converted = number.floatValue();
                if (!Float.isFinite(converted)) {
                    throw new IllegalArgumentException("number is out of float range");
                }
                return converted;
            }
            if (targetType == Short.class) {
                int integer = ValueCodecTypeSupport.exactInteger(number);
                if (integer < Short.MIN_VALUE || integer > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("number is out of short range");
                }
                return (short) integer;
            }
            if (targetType == Byte.class) {
                int integer = ValueCodecTypeSupport.exactInteger(number);
                if (integer < Byte.MIN_VALUE || integer > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("number is out of byte range");
                }
                return (byte) integer;
            }
            throw new IllegalArgumentException("number type is not supported: " + targetType.getName());
        }
    }

    /**
     * 统一处理 java.time 对象和 ISO 文本。数据库驱动专有时间对象由上层适配器先转换成稳定的 java.time 载体。
     */
    private static final class JavaTimeValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == LocalDate.class
                    || targetType == LocalDateTime.class
                    || targetType == LocalTime.class
                    || targetType == Instant.class
                    || targetType == OffsetDateTime.class
                    || targetType == OffsetTime.class;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            try {
                if (targetType == LocalDate.class) {
                    return localDate(value);
                }
                if (targetType == LocalDateTime.class) {
                    return localDateTime(value);
                }
                if (targetType == LocalTime.class) {
                    return localTime(value);
                }
                if (targetType == Instant.class) {
                    return instant(value);
                }
                if (targetType == OffsetDateTime.class) {
                    return offsetDateTime(value);
                }
                if (targetType == OffsetTime.class) {
                    return offsetTime(value);
                }
            } catch (DateTimeException failure) {
                throw new IllegalArgumentException("value cannot be converted to java time type");
            }
            throw new IllegalArgumentException("java time type is not supported: " + targetType.getName());
        }

        private static LocalDate localDate(Object value) {
            if (value instanceof LocalDateTime dateTime) {
                return dateTime.toLocalDate();
            }
            if (value instanceof OffsetDateTime dateTime) {
                return dateTime.toLocalDate();
            }
            return LocalDate.parse(ValueCodecTypeSupport.text(value));
        }

        private static LocalDateTime localDateTime(Object value) {
            if (value instanceof LocalDate date) {
                return date.atStartOfDay();
            }
            if (value instanceof OffsetDateTime dateTime) {
                return dateTime.toLocalDateTime();
            }
            return LocalDateTime.parse(ValueCodecTypeSupport.text(value));
        }

        private static LocalTime localTime(Object value) {
            if (value instanceof LocalDateTime dateTime) {
                return dateTime.toLocalTime();
            }
            if (value instanceof OffsetDateTime dateTime) {
                return dateTime.toLocalTime();
            }
            return LocalTime.parse(ValueCodecTypeSupport.text(value));
        }

        private static Instant instant(Object value) {
            if (value instanceof OffsetDateTime dateTime) {
                return dateTime.toInstant();
            }
            return Instant.parse(ValueCodecTypeSupport.text(value));
        }

        private static OffsetDateTime offsetDateTime(Object value) {
            if (value instanceof Instant instant) {
                return instant.atOffset(ZoneOffset.UTC);
            }
            return OffsetDateTime.parse(ValueCodecTypeSupport.text(value));
        }

        private static OffsetTime offsetTime(Object value) {
            if (value instanceof OffsetDateTime dateTime) {
                return dateTime.toOffsetTime();
            }
            return OffsetTime.parse(ValueCodecTypeSupport.text(value));
        }
    }

    /** UUID 本身可被常见驱动直接绑定，文本读取时再解析。 */
    private static final class UuidValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == UUID.class;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            try {
                return UUID.fromString(ValueCodecTypeSupport.text(value));
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException("value cannot be converted to UUID");
            }
        }
    }

    /** 二进制读取只复制 ByteBuffer 当前可读区间，并且不移动原 buffer 的 position。 */
    private static final class BinaryValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            // 写入按运行时实现类查找 codec，必须接受 HeapByteBuffer 等子类；读取仍在下方只承诺精确 ByteBuffer。
            return targetType == byte[].class || targetType == Byte[].class
                    || ByteBuffer.class.isAssignableFrom(targetType);
        }

        @Override
        public Object write(Object value) {
            return value instanceof Byte[] boxed ? toPrimitive(boxed) : value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (targetType == byte[].class && value instanceof ByteBuffer buffer) {
                ByteBuffer readable = buffer.duplicate();
                byte[] bytes = new byte[readable.remaining()];
                readable.get(bytes);
                return bytes;
            }
            if (targetType == byte[].class && value instanceof Byte[] boxed) {
                return toPrimitive(boxed);
            }
            if (targetType == Byte[].class && value instanceof byte[] bytes) {
                return toBoxed(bytes);
            }
            if (targetType == Byte[].class && value instanceof ByteBuffer buffer) {
                return toBoxed((byte[]) read(buffer, byte[].class));
            }
            if (targetType == ByteBuffer.class && value instanceof byte[] bytes) {
                return ByteBuffer.wrap(bytes.clone());
            }
            if (targetType == ByteBuffer.class && value instanceof Byte[] boxed) {
                return ByteBuffer.wrap(toPrimitive(boxed));
            }
            throw new IllegalArgumentException("binary value cannot be converted from " + value.getClass().getName()
                                                       + " to " + targetType.getName());
        }

        /** boxed 二进制不能包含数据库无法表达的元素级 null。 */
        private static byte[] toPrimitive(Byte[] source) {
            byte[] target = new byte[source.length];
            for (int index = 0; index < source.length; index++) {
                Byte value = source[index];
                if (value == null) {
                    throw new IllegalArgumentException("boxed binary value must not contain null");
                }
                target[index] = value;
            }
            return target;
        }

        private static Byte[] toBoxed(byte[] source) {
            Byte[] target = new Byte[source.length];
            for (int index = 0; index < source.length; index++) {
                target[index] = source[index];
            }
            return target;
        }
    }

    /** 大文本和普通文本都保留空白，只将可变字符序列收口为最通用的 String。 */
    private static final class TextValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class
                    || CharSequence.class.isAssignableFrom(targetType)
                    || targetType == Character.class
                    || targetType == char[].class;
        }

        @Override
        public Object write(Object value) {
            return value instanceof char[] characters ? new String(characters) : value.toString();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            String text = value instanceof char[] characters ? new String(characters) : value.toString();
            if (targetType == String.class || targetType == CharSequence.class) {
                return text;
            }
            if (targetType == StringBuilder.class) {
                return new StringBuilder(text);
            }
            if (targetType == StringBuffer.class) {
                return new StringBuffer(text);
            }
            if (targetType == CharBuffer.class) {
                return CharBuffer.wrap(text);
            }
            if (targetType == Character.class) {
                if (text.length() != 1) {
                    throw new IllegalArgumentException("character value must contain exactly one character");
                }
                return text.charAt(0);
            }
            if (targetType == char[].class) {
                return text.toCharArray();
            }
            throw new IllegalArgumentException("text value cannot be converted to " + targetType.getName());
        }
    }

    /** 已被字段 codec 强类型化的数组直接交给驱动，跨类型数组转换必须使用字段感知 codec。 */
    private static final class ArrayIdentityValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType.isArray();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (targetType.isInstance(value)) {
                return value;
            }
            throw new IllegalArgumentException("array conversion requires a field-aware codec");
        }
    }

    /** String、Object 和 Character 的通用兜底转换。 */
    private static final class IdentityValueCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class || targetType == Object.class || targetType == Character.class;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            if (targetType == String.class) {
                return value.toString();
            }
            if (targetType == Character.class) {
                String text = ValueCodecTypeSupport.text(value);
                if (text.length() == 1) {
                    return text.charAt(0);
                }
                throw new IllegalArgumentException("character value must contain exactly one character");
            }
            return value;
        }
    }

}
