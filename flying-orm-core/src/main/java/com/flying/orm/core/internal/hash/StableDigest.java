package com.flying.orm.core.internal.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * 创建带用途域隔离的稳定 SHA-256 编码器。
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
public final class StableDigest {

    private StableDigest() {
    }

    /** 预初始化一个可安全并发复用的用途域，避免在摘要热路径重复编码固定文本。 */
    public static Domain domain(String name) {
        return new Domain(name);
    }

    /**
     * 创建一次性摘要编码器。相同数据用于不同用途时，域名会让摘要保持隔离。
     *
     * @param domain 稳定、非空的用途和协议版本，例如 {@code batch-payload/v1}
     * @return 新编码器
     */
    public static StableEncoder sha256(String domain) {
        return sha256(domain(domain));
    }

    /** 使用预初始化用途域创建一次性编码器。 */
    public static StableEncoder sha256(Domain domain) {
        return new StableEncoder(Objects.requireNonNull(domain, "stable digest domain must not be null")
                                        .newDigest());
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is required by Java 21", error);
        }
    }

    /** 已写入用途域的 SHA-256 初始状态。 */
    public static final class Domain {

        private final String name;
        private final MessageDigest seed;

        private Domain(String name) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("stable digest domain must not be blank");
            }
            this.name = name;
            this.seed = newSha256();
            new StableEncoder(seed).text("DOMAIN", name);
        }

        private MessageDigest newDigest() {
            try {
                return (MessageDigest) seed.clone();
            } catch (CloneNotSupportedException ignored) {
                MessageDigest digest = newSha256();
                new StableEncoder(digest).text("DOMAIN", name);
                return digest;
            }
        }
    }
}
