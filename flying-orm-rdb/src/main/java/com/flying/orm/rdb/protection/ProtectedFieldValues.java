package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.TenantScope;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.codec.JdbcLegacyTemporalAdapter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * 字段保护内部共享的值编码与租户派生边界。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class ProtectedFieldValues {

    private ProtectedFieldValues() {
    }

    /** 按表单、字段和可信租户值构造密钥派生上下文。 */
    static ProtectedFieldContext context(DynamicForm form, DynamicField field, String tenant) {
        return new ProtectedFieldContext(form.id(), field.name(), tenant);
    }

    /** 把有效 DataScope 中的租户值编码为稳定且不歧义的派生身份。 */
    static String tenantIdentity(DynamicForm form, DataScope scope, ValueCodecRegistry codecs) {
        try {
            return tenantIdentityValue(form, scope, codecs);
        } catch (RuntimeException failure) {
            // codec、adapter 或自定义值的 toString 都可能携带租户原值，公共边界只保留固定分类。
            throw new IllegalArgumentException("tenant value does not have a stable protected identity");
        }
    }

    private static String tenantIdentityValue(DynamicForm form, DataScope scope, ValueCodecRegistry codecs) {
        if (form.tenant().isEmpty()) {
            return "";
        }
        String field = form.tenant().orElseThrow().fieldName();
        DynamicField tenantField = form.field(field);
        TenantScope tenant = Objects.requireNonNull(scope, "data scope must not be null")
                                    .tenantScope(field)
                                    .orElseThrow(() -> new IllegalArgumentException(
                                            "tenant scope is required for protected field"));
        Class<? extends TemporalAccessor> temporalType = temporalIdentityType(tenantField.databaseType());
        Object identityValue = tenant.value();
        if (temporalType != null) {
            identityValue = JdbcLegacyTemporalAdapter.read(codecs, identityValue, temporalType);
        }
        Object encoded = Objects.requireNonNull(
                codecs.write(identityValue), "tenant codec result must not be null");
        requireRepresentableTemporalPrecision(tenantField, encoded, temporalType, codecs);
        if (encoded instanceof byte[] bytes) {
            return binaryIdentity(bytes);
        }
        if (encoded instanceof ByteBuffer buffer) {
            return binaryIdentity(binary(buffer));
        }
        if (encoded instanceof CharSequence text) {
            return "text:" + text;
        }
        if (encoded instanceof Character character) {
            return "text:" + character;
        }
        if (encoded instanceof Boolean flag) {
            return "boolean:" + flag;
        }
        if (encoded instanceof Number number) {
            return numericIdentity(number);
        }
        if (encoded instanceof UUID uuid) {
            return "uuid:" + uuid;
        }
        if (encoded instanceof TemporalAccessor temporal) {
            return temporalIdentity(temporal);
        }
        throw new IllegalArgumentException("tenant value does not have a stable protected identity");
    }

    /** 按数据库等值语义选择稳定 java.time 类型；推荐 java.time 类型的旧身份保持字节级兼容。 */
    private static Class<? extends TemporalAccessor> temporalIdentityType(DatabaseType dataType) {
        return switch (Objects.requireNonNull(dataType, "tenant data type must not be null").logicalType()) {
            case OFFSET_TIMESTAMP -> Instant.class;
            case DATE -> LocalDate.class;
            case TIME -> LocalTime.class;
            case OFFSET_TIME -> OffsetTime.class;
            case TIMESTAMP -> LocalDateTime.class;
            default -> null;
        };
    }

    private static String temporalIdentity(TemporalAccessor temporal) {
        return "time:" + temporal.getClass().getName() + ':' + temporal;
    }

    private static void requireRepresentableTemporalPrecision(DynamicField field,
                                                               Object encoded,
                                                               Class<? extends TemporalAccessor> temporalType,
                                                               ValueCodecRegistry codecs) {
        if (temporalType == null) {
            return;
        }
        Integer precision = temporalPrecision(field);
        Object precisionValue = encoded instanceof TemporalAccessor
                ? encoded
                : JdbcLegacyTemporalAdapter.read(codecs, encoded, temporalType);
        if (!(precisionValue instanceof TemporalAccessor temporal)
                || !temporal.isSupported(ChronoField.NANO_OF_SECOND)) {
            return;
        }
        int nanos = temporal.get(ChronoField.NANO_OF_SECOND);
        if (isSmallDateTime(field.databaseType())) {
            if (!temporal.isSupported(ChronoField.SECOND_OF_MINUTE)
                    || temporal.get(ChronoField.SECOND_OF_MINUTE) != 0
                    || nanos != 0) {
                throw new IllegalArgumentException("temporal tenant value is not aligned to SMALLDATETIME minutes");
            }
            return;
        }
        if (isOracleDate(field.databaseType())) {
            if (nanos != 0) {
                throw new IllegalArgumentException("temporal tenant value exceeds ORACLE_DATE second precision");
            }
            return;
        }
        if (isSqlServerDateTime(field.databaseType())) {
            int milliseconds = nanos / 1_000_000;
            int lastDigit = milliseconds % 10;
            if (nanos % 1_000_000 != 0 || (lastDigit != 0 && lastDigit != 3 && lastDigit != 7)) {
                throw new IllegalArgumentException(
                        "temporal tenant value is not aligned to SQLSERVER_DATETIME storage increments");
            }
            return;
        }
        if (precision == null) {
            if (nanos != 0) {
                // 未声明时各方言默认精度不同；只接受所有方言都能无损保存的整秒值。
                throw new IllegalArgumentException(
                        "fractional temporal tenant value requires an explicit storage precision");
            }
            return;
        }
        if (precision >= 9) {
            return;
        }
        int unit = 1;
        for (int digit = precision; digit < 9; digit++) {
            unit *= 10;
        }
        if (nanos % unit != 0) {
            // 不猜测各数据库的舍入规则；先失败，避免写入后租户值变化导致保护密钥漂移。
            throw new IllegalArgumentException("temporal tenant value exceeds declared storage precision");
        }
    }

    private static boolean isSmallDateTime(DatabaseType dataType) {
        return "SMALLDATETIME".equals(dataType.baseName())
                || "SQLSERVER_SMALLDATETIME".equals(dataType.baseName());
    }

    private static boolean isOracleDate(DatabaseType dataType) {
        return "ORACLE_DATE".equals(dataType.baseName());
    }

    private static boolean isSqlServerDateTime(DatabaseType dataType) {
        return "SQLSERVER_DATETIME".equals(dataType.baseName());
    }

    private static Integer temporalPrecision(DynamicField field) {
        Integer declared = field.precision();
        if (field.databaseType().arguments().isEmpty()) {
            return declared;
        }
        String argument = field.databaseType().arguments().getFirst();
        if (argument.isEmpty() || !argument.chars().allMatch(Character::isDigit)) {
            return declared;
        }
        int inline;
        try {
            inline = Integer.parseInt(argument);
        } catch (NumberFormatException ignored) {
            return declared;
        }
        if (declared != null && declared != inline) {
            throw new IllegalArgumentException("conflicting temporal tenant precision values");
        }
        return inline;
    }

    /** 受保护字段只接受业务 codec 编码后的文本值。 */
    static String encodedText(ValueCodecRegistry codecs, Object value) {
        Object snapshot = BindableValueSnapshots.immutableValue(
                Objects.requireNonNull(value, "protected field value must not be null"));
        return encodeText(codecs, snapshot);
    }

    /**
     * 编码由条件访问器刚生成的一次性值。数组已经由访问器复制；这里只补齐访问器不拥有的可变标量，
     * 不再遍历集合或重复复制数组。
     */
    static String encodedOwnedText(ValueCodecRegistry codecs, Object value) {
        Object owned = Objects.requireNonNull(value, "protected field value must not be null");
        Object codecValue = owned.getClass().isArray()
                ? owned : BindableValueSnapshots.immutableScalar(owned, null);
        return encodeText(codecs, codecValue);
    }

    private static String encodeText(ValueCodecRegistry codecs, Object value) {
        try {
            Object encoded = Objects.requireNonNull(codecs, "value codec registry must not be null")
                                    .write(value);
            if (!(encoded instanceof CharSequence text)) {
                throw new IllegalArgumentException("encrypted field value must encode as text");
            }
            return text.toString();
        } catch (RuntimeException failure) {
            // 自定义 codec 和 CharSequence 实现不能把待加密明文复制到异常消息或 cause。
            throw new IllegalArgumentException("encrypted field value cannot be encoded");
        }
    }

    /** 把驱动返回的二进制值物化为独立 byte 数组视图。 */
    static byte[] binary(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            return binary(buffer);
        }
        throw new ProtectedFieldException();
    }

    private static byte[] binary(ByteBuffer value) {
        ByteBuffer copy = value.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static String binaryIdentity(byte[] value) {
        return "bytes:" + Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static String numericIdentity(Number value) {
        BigDecimal decimal;
        if (value instanceof BigDecimal bigDecimal) {
            decimal = bigDecimal;
        } else if (value instanceof BigInteger bigInteger) {
            decimal = new BigDecimal(bigInteger);
        } else if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            decimal = BigDecimal.valueOf(value.longValue());
        } else if (value instanceof Float || value instanceof Double) {
            double floating = value.doubleValue();
            if (!Double.isFinite(floating)) {
                throw new IllegalArgumentException("tenant value does not have a stable protected identity");
            }
            decimal = BigDecimal.valueOf(floating);
        } else {
            try {
                decimal = new BigDecimal(value.toString());
            } catch (NumberFormatException ignored) {
                throw new IllegalArgumentException("tenant value does not have a stable protected identity");
            }
        }
        BigDecimal canonical = decimal.compareTo(BigDecimal.ZERO) == 0
                ? BigDecimal.ZERO : decimal.stripTrailingZeros();
        // toString 在需要时使用科学计数法，避免很小的 BigDecimal 对象因负 scale 膨胀成无界派生文本。
        return "number:" + canonical;
    }
}
