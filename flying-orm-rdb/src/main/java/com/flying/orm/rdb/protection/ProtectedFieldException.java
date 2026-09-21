package com.flying.orm.rdb.protection;

/**
 * 表示受保护字段的信封、密钥版本或认证校验失败。
 *
 * <p>公开消息固定且不包含密文、密钥版本或底层密码实现信息。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class ProtectedFieldException extends RuntimeException {

    private static final String SAFE_MESSAGE = "protected field value cannot be decrypted";

    ProtectedFieldException(Throwable cause) {
        super(SAFE_MESSAGE, cause);
    }

    ProtectedFieldException() {
        super(SAFE_MESSAGE);
    }
}
