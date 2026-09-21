package com.flying.orm.rdb.dialect;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.Locale;
import java.util.Objects;

/**
 * 一次数据库识别得到的、可以安全进入审核计划的稳定身份。
 *
 * <p>描述只包含数据库产品、产品版本、方言 ID 和能力快照指纹。URL、账号、凭据、连接池以及
 * 物理数据源都不属于 ORM 计划身份，也不会被这个对象保存或带入日志。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class DatabaseDescriptor {

    private static final StableDigest.Domain FINGERPRINT_DOMAIN =
            StableDigest.domain("database-descriptor/v1");

    private final String product;
    private final String version;
    private final String dialectId;
    private final DialectCapabilities capabilities;
    private final String capabilityFingerprint;
    private final String fingerprint;

    private DatabaseDescriptor(String product,
                               String version,
                               String dialectId,
                               DialectCapabilities capabilities) {
        this.product = requireText(product, "database product");
        this.version = requireText(version, "database version");
        this.dialectId = requireText(dialectId, "database dialect id").toLowerCase(Locale.ROOT);
        this.capabilities = Objects.requireNonNull(
                capabilities, "dialect capabilities must not be null");
        this.capabilityFingerprint = this.capabilities.fingerprint();
        this.fingerprint = computeFingerprint();
    }

    /**
     * 使用已经解析好的方言构造数据库描述。连接信息不会进入返回对象。
     */
    public static DatabaseDescriptor of(String product, String version, RdbDialect dialect) {
        RdbDialect source = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        return new DatabaseDescriptor(product, version, source.name(), source.capabilities());
    }

    /**
     * 使用明确的方言 ID 和能力快照构造描述，适合无连接的静态规划或测试。
     */
    public static DatabaseDescriptor of(String product,
                                        String version,
                                        String dialectId,
                                        DialectCapabilities capabilities) {
        return new DatabaseDescriptor(product, version, dialectId, capabilities);
    }

    /** @return 数据库产品名，不包含 URL 或拓扑信息 */
    public String product() {
        return product;
    }

    /** @return 数据库产品版本 */
    public String version() {
        return version;
    }

    /** @return 已解析并规范化为小写的方言 ID */
    public String dialectId() {
        return dialectId;
    }

    /** @return 构造时冻结的方言能力指纹 */
    public String capabilityFingerprint() {
        return capabilityFingerprint;
    }

    /** @return 识别数据库时冻结的不可变方言能力事实 */
    public DialectCapabilities capabilities() {
        return capabilities;
    }

    /** @return 覆盖四项数据库身份事实的稳定 SHA-256 指纹 */
    public String fingerprint() {
        return fingerprint;
    }

    private String computeFingerprint() {
        StableEncoder encoder = StableDigest.sha256(FINGERPRINT_DOMAIN)
                                            .text("PRODUCT", product)
                                            .text("VERSION", version)
                                            .text("DIALECT_ID", dialectId)
                                            .text("CAPABILITY_FINGERPRINT", capabilityFingerprint);
        return encoder.finishHex();
    }

    private static String requireText(String value, String fieldName) {
        String safeValue = Objects.requireNonNull(value, fieldName + " must not be null").trim();
        if (safeValue.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return safeValue;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DatabaseDescriptor that)) {
            return false;
        }
        return product.equals(that.product)
                && version.equals(that.version)
                && dialectId.equals(that.dialectId)
                && capabilityFingerprint.equals(that.capabilityFingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(product, version, dialectId, capabilityFingerprint);
    }

    @Override
    public String toString() {
        return "DatabaseDescriptor[product=" + product
                + ", version=" + version
                + ", dialectId=" + dialectId
                + ", capabilityFingerprint=" + capabilityFingerprint + ']';
    }
}
