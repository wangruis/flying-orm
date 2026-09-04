package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 生成受方言长度约束的关系对象名。
 *
 * <p>表名和列名属于公开关系身份，绝不静默改名；只有框架生成的约束、索引和序列名允许使用
 * 稳定摘要截断。实例没有可变状态，同一输入永远得到同一输出。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationalObjectNameGenerator {

    private static final StableDigest.Domain DOMAIN = StableDigest.domain("relational-object-name/v1");
    private static final int DIGEST_LENGTH = 12;

    public enum Kind {
        PRIMARY_KEY("pk"),
        UNIQUE("uk"),
        INDEX("idx"),
        FOREIGN_KEY("fk"),
        CHECK("ck"),
        SEQUENCE("seq");

        private final String prefix;

        Kind(String prefix) {
            this.prefix = prefix;
        }
    }

    private final int maximumLength;

    /** 0 表示当前数据库没有提供可信上限；生成或校验名称时会 fail closed。 */
    public RelationalObjectNameGenerator(int maximumLength) {
        if (maximumLength < 0) {
            throw new IllegalArgumentException("maximum identifier length must not be negative");
        }
        this.maximumLength = maximumLength;
    }

    public static RelationalObjectNameGenerator forDialect(RdbDialect dialect) {
        return new RelationalObjectNameGenerator(Objects.requireNonNull(
                dialect, "RDB dialect must not be null").maxIdentifierLength());
    }

    public int maximumLength() {
        return maximumLength;
    }

    public String table(String name) {
        return exact(name, "table name");
    }

    public String column(String name) {
        return exact(name, "column name");
    }

    /** 审阅器在冷路径校验显式约束、索引和序列名，不改变调用方声明。 */
    String object(String name) {
        return exact(name, "relational object name");
    }

    /** 根据稳定、有序的名称片段生成约束、索引或序列名。 */
    public String generate(Kind kind, List<String> parts) {
        Kind safeKind = Objects.requireNonNull(kind, "relational object kind must not be null");
        List<String> safeParts = Objects.requireNonNull(parts, "relational object name parts must not be null")
                .stream()
                .map(part -> Names.requireText(part, "relational object name part"))
                .toList();
        if (safeParts.isEmpty()) {
            throw new IllegalArgumentException("relational object name parts must not be empty");
        }
        requireKnownLimit();
        boolean encoded = safeParts.stream().anyMatch(part -> part.indexOf('.') >= 0);
        String full = safeKind.prefix + '_' + safeParts.stream()
                .map(part -> part.replace('.', '_'))
                .collect(Collectors.joining("_"));
        if (!encoded && length(full) <= maximumLength) {
            return full;
        }
        if (maximumLength <= DIGEST_LENGTH + 1) {
            throw new IllegalArgumentException("maximum identifier length is too small for stable truncation");
        }
        String digest = digest(safeKind, safeParts).substring(0, DIGEST_LENGTH);
        int prefixLength = Math.min(length(full), maximumLength - DIGEST_LENGTH - 1);
        return prefix(full, prefixLength) + '_' + digest;
    }

    public String generate(Kind kind, String... parts) {
        return generate(kind, List.of(parts));
    }

    /**
     * 在整份计划生成完后一次检查名称碰撞。数据库通常折叠未引用名称的大小写，因此这里保守地
     * 使用小写键；宁可要求调用方显式改名，也不让迁移执行到一半才撞库。
     */
    public void requireNoCollisions(Collection<String> names) {
        Set<String> seen = new HashSet<>();
        for (String name : Objects.requireNonNull(names, "relational object names must not be null")) {
            String safeName = exact(name, "relational object name");
            if (!seen.add(safeName.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("relational object names collide after database normalization");
            }
        }
    }

    private String exact(String value, String role) {
        String name = Names.requireText(value, role);
        requireKnownLimit();
        if (length(name) > maximumLength) {
            throw new IllegalArgumentException(role + " exceeds the database identifier limit");
        }
        return name;
    }

    private void requireKnownLimit() {
        if (maximumLength == 0) {
            throw new IllegalStateException("database identifier length is unknown");
        }
    }

    private static String digest(Kind kind, List<String> parts) {
        StableEncoder encoder = StableDigest.sha256(DOMAIN).text("KIND", kind.name())
                .integer("PART_COUNT", parts.size());
        parts.forEach(part -> encoder.text("PART", part));
        return encoder.finishHex();
    }

    private static int length(String value) {
        return value.codePointCount(0, value.length());
    }

    private static String prefix(String value, int codePoints) {
        return value.substring(0, value.offsetByCodePoints(0, codePoints));
    }
}
