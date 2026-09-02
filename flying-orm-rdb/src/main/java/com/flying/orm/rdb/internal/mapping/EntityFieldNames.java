package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.internal.Names;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Locale;

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
@InternalApi
public final class EntityFieldNames {

    private EntityFieldNames() {
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
        StringBuilder key = new StringBuilder(value.length());
        boolean nonAscii = false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current != '_' && current != '-' && (!removeSpaces || current != ' ')) {
                nonAscii |= current > 0x7f;
                key.append(current >= 'A' && current <= 'Z' ? (char) (current + ('a' - 'A')) : current);
            }
        }
        String normalized = key.toString();
        return nonAscii ? normalized.toLowerCase(Locale.ROOT) : normalized;
    }
}
