package com.flying.orm.rdb.protection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 保存一个 current 主密钥和最多三个只读旧版本。
 *
 * <p>构造时复制上层提供的密钥，关闭时清零 ORM 持有的副本。该类型不负责读取配置或连接外部密钥系统。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class ProtectedFieldKeyRing implements AutoCloseable {

    private static final int MASTER_KEY_LENGTH = 32;
    private static final int MAX_READABLE_KEYS = 4;
    private static final Pattern VERSION = Pattern.compile("[A-Za-z0-9._-]{1,16}");

    private final String currentVersion;
    private final Map<String, byte[]> keys;
    private final byte[] uniqueSearchKey;
    private boolean closed;

    private ProtectedFieldKeyRing(String currentVersion,
                                  Map<String, byte[]> source,
                                  byte[] configuredUniqueSearchKey) {
        this.currentVersion = currentVersion;
        Map<String, byte[]> copied = new LinkedHashMap<>();
        source.forEach((version, key) -> copied.put(version, key.clone()));
        this.keys = copied;
        this.uniqueSearchKey = configuredUniqueSearchKey == null && copied.size() == 1
                ? copied.get(currentVersion).clone()
                : configuredUniqueSearchKey == null ? null : configuredUniqueSearchKey.clone();
    }

    /** @param version 当前密钥版本；@param masterKey 32 字节主密钥；@return 单版本密钥环 */
    public static ProtectedFieldKeyRing single(String version, byte[] masterKey) {
        return builder().current(version, masterKey).build();
    }

    /** @return 新密钥环构建器 */
    public static Builder builder() {
        return new Builder();
    }

    /** @return 当前写入版本 */
    public String currentVersion() {
        return currentVersion;
    }

    /** @return 当前和旧版本的不可修改集合，不暴露密钥内容 */
    public synchronized Set<String> readableVersions() {
        requireOpen();
        return Set.copyOf(keys.keySet());
    }

    synchronized byte[] masterKey(String version) {
        requireOpen();
        byte[] key = keys.get(version);
        if (key == null) {
            throw new ProtectedFieldException();
        }
        return key.clone();
    }

    synchronized List<String> versionsInSearchOrder() {
        requireOpen();
        List<String> versions = new ArrayList<>(keys.size());
        versions.add(currentVersion);
        keys.keySet().stream()
            .filter(version -> !version.equals(currentVersion))
            .forEach(versions::add);
        return List.copyOf(versions);
    }

    /**
     * 返回跨加密密钥轮换保持稳定的身份密钥副本。
     *
     * <p>单密钥配置自动复用当前主密钥以保持既有 token；同时配置多个可读加密密钥时必须由上层显式提供稳定密钥，
     * 否则唯一列和受保护批量回执在轮换窗口中会产生不同身份。</p>
     */
    synchronized byte[] uniqueSearchKey() {
        requireOpen();
        if (uniqueSearchKey == null) {
            throw new IllegalStateException("stable unique search key is required during key rotation");
        }
        return uniqueSearchKey.clone();
    }

    /** 清零 ORM 持有的全部主密钥副本；重复关闭安全。 */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        keys.values().forEach(key -> Arrays.fill(key, (byte) 0));
        keys.clear();
        if (uniqueSearchKey != null) {
            Arrays.fill(uniqueSearchKey, (byte) 0);
        }
        closed = true;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("protected field key ring is closed");
        }
    }

    private static String version(String value) {
        if (value == null || !VERSION.matcher(value).matches()) {
            throw new IllegalArgumentException("protected field key version is invalid");
        }
        return value;
    }

    private static byte[] key(byte[] value) {
        Objects.requireNonNull(value, "protected field master key must not be null");
        if (value.length != MASTER_KEY_LENGTH) {
            throw new IllegalArgumentException("protected field master key must contain 32 bytes");
        }
        return value.clone();
    }

    /** 构建版本化主密钥环；构建后不能复用。 */
    public static final class Builder {

        private final Map<String, byte[]> keys = new LinkedHashMap<>();
        private String currentVersion;
        private byte[] uniqueSearchKey;
        private boolean built;

        private Builder() {
        }

        /** @return 当前构建器 */
        public Builder current(String version, byte[] masterKey) {
            requireMutable();
            if (currentVersion != null) {
                throw new IllegalStateException("protected field current key is already configured");
            }
            currentVersion = add(version, masterKey);
            return this;
        }

        /** @return 当前构建器 */
        public Builder readable(String version, byte[] masterKey) {
            requireMutable();
            add(version, masterKey);
            return this;
        }

        /**
         * 配置不随加密密钥版本变化的 32 字节唯一搜索密钥。
         *
         * @param searchKey 上层长期保存并独立轮换治理的唯一搜索密钥
         * @return 当前构建器
         */
        public Builder uniqueSearchKey(byte[] searchKey) {
            requireMutable();
            if (uniqueSearchKey != null) {
                throw new IllegalStateException("protected unique search key is already configured");
            }
            uniqueSearchKey = key(searchKey);
            return this;
        }

        /** @return 可并发共享的密钥环 */
        public ProtectedFieldKeyRing build() {
            requireMutable();
            built = true;
            try {
                if (currentVersion == null) {
                    throw new IllegalStateException("protected field current key is required");
                }
                if (keys.size() > MAX_READABLE_KEYS) {
                    throw new IllegalArgumentException("protected field key ring contains too many versions");
                }
                return new ProtectedFieldKeyRing(currentVersion, keys, uniqueSearchKey);
            } finally {
                keys.values().forEach(value -> Arrays.fill(value, (byte) 0));
                keys.clear();
                if (uniqueSearchKey != null) {
                    Arrays.fill(uniqueSearchKey, (byte) 0);
                    uniqueSearchKey = null;
                }
            }
        }

        private String add(String rawVersion, byte[] masterKey) {
            String safeVersion = version(rawVersion);
            byte[] safeKey = key(masterKey);
            if (keys.putIfAbsent(safeVersion, safeKey) != null) {
                Arrays.fill(safeKey, (byte) 0);
                throw new IllegalArgumentException("duplicate protected field key version");
            }
            return safeVersion;
        }

        private void requireMutable() {
            if (built) {
                throw new IllegalStateException("protected field key ring builder is already used");
            }
        }
    }
}
