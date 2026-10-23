package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.internal.Names;

import java.util.Locale;
import java.util.Objects;

/**
 * Canonical Java-member/database-column matching for entity metadata and mapped result labels.
 *
 * <p>Dynamic-form field identity remains stricter and case-insensitive only. Entity mapping additionally treats
 * camelCase, snake_case and kebab-case as the same member name; result labels may also be qualified or quoted.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
public final class EntityFieldNames {

    private EntityFieldNames() {
    }

    /** 默认表名和列名共用的 snake_case 规则；显式注解名称不经过此转换。 */
    static String snakeCase(String propertyName) {
        String text = Objects.requireNonNull(propertyName, "property name must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("property name must not be blank");
        }
        StringBuilder name = new StringBuilder(text.length() + 8);
        for (int i = 0; i < text.length(); i++) {
            char current = text.charAt(i);
            if (Character.isUpperCase(current)) {
                if (i > 0 && name.charAt(name.length() - 1) != '_') {
                    name.append('_');
                }
                name.append(Character.toLowerCase(current));
            } else if (current == '-' || current == ' ') {
                if (!name.isEmpty() && name.charAt(name.length() - 1) != '_') {
                    name.append('_');
                }
            } else {
                name.append(current);
            }
        }
        return name.toString();
    }

    /** @return the canonical entity member/column lookup key */
    public static String key(String value) {
        return compact(Names.requireText(value, "entity field name"), false);
    }

    /** @return whether two entity member/column names resolve to the same key */
    public static boolean matches(String left, String right) {
        return key(left).equals(key(right));
    }

    /** @return the canonical key for a possibly qualified or quoted driver result label */
    public static String resultKey(String value) {
        String text = Names.requireText(value, "result column label");
        int qualifier = text.lastIndexOf('.');
        if (qualifier >= 0 && qualifier < text.length() - 1) {
            text = text.substring(qualifier + 1);
        }
        if (text.length() >= 2 && ((text.startsWith("\"") && text.endsWith("\""))
                || (text.startsWith("`") && text.endsWith("`"))
                || (text.startsWith("[") && text.endsWith("]")))) {
            text = text.substring(1, text.length() - 1);
        }
        return compact(text, true);
    }

    private static String compact(String value, boolean removeSpaces) {
        StringBuilder key = null;
        boolean nonAscii = false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            boolean retained = current != '_' && current != '-' && (!removeSpaces || current != ' ');
            char normalized = current >= 'A' && current <= 'Z' ? (char) (current + ('a' - 'A')) : current;
            nonAscii |= retained && current > 0x7f;
            if (key == null) {
                if (retained && normalized == current) {
                    continue;
                }
                key = new StringBuilder(value.length()).append(value, 0, index);
            }
            if (retained) {
                key.append(normalized);
            }
        }
        String normalized = key == null ? value : key.toString();
        return nonAscii ? normalized.toLowerCase(Locale.ROOT) : normalized;
    }
}
