package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.TenantScope;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (RuntimeException | Error failure) {
            VirtualMachineError fatal = ProtectedFailureSupport.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // codec、adapter 或自定义值的 toString 都可能携带租户原值，公共边界只保留固定分类。
            throw new IllegalArgumentException("tenant value does not have a stable protected identity");
        }
    }

    private static String tenantIdentityValue(DynamicForm form, DataScope scope, ValueCodecRegistry codecs) {
        if (form.tenant().isEmpty()) {
            return "";
        }
        String field = form.tenant().orElseThrow().fieldName();
        TenantScope tenant = Objects.requireNonNull(scope, "data scope must not be null")
                                    .tenantScope(field)
                                    .orElseThrow(() -> new IllegalArgumentException(
                                            "tenant scope is required for protected field"));
        Object encoded = Objects.requireNonNull(
                codecs.write(tenant.value()), "tenant codec result must not be null");
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
            return "time:" + encoded.getClass().getName() + ':' + temporal;
        }
        throw new IllegalArgumentException("tenant value does not have a stable protected identity");
    }

    /** 受保护字段只接受业务 codec 编码后的文本值。 */
    static String encodedText(ValueCodecRegistry codecs, Object value) {
        try {
            Object encoded = Objects.requireNonNull(codecs, "value codec registry must not be null")
                                    .write(Objects.requireNonNull(
                                            value, "protected field value must not be null"));
            if (!(encoded instanceof CharSequence text)) {
                throw new IllegalArgumentException("encrypted field value must encode as text");
            }
            return text.toString();
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (RuntimeException | Error failure) {
            VirtualMachineError fatal = ProtectedFailureSupport.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
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
