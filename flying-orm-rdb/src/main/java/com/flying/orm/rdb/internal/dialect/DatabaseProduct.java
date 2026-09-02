package com.flying.orm.rdb.internal.dialect;

import com.flying.orm.rdb.internal.InternalApi;

/**
 * Canonical identity of the database products supported by flying-orm.
 *
 * <p>{@link #fromName(String)} is strict enough for user configuration; {@link #detect(String)} additionally
 * accepts standard driver and metadata descriptions. Low-level adapters use this value once during initialization
 * instead of repeatedly interpreting product-name strings on execution paths.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
@InternalApi
public enum DatabaseProduct {
    H2,
    MYSQL,
    POSTGRESQL,
    ORACLE,
    SQL_SERVER,
    UNKNOWN;

    /** Resolve a supported configuration, URL-driver or provider alias without substring guessing. */
    public static DatabaseProduct fromName(String name) {
        return switch (normalize(name)) {
            case "h2", "h2database", "h2databaseengine" -> H2;
            case "mysql", "mariadb", "mysqlconnectionfactoryprovider",
                    "mariadbconnectionfactoryprovider" -> MYSQL;
            case "postgresql", "postgres", "postgresqlconnectionfactoryprovider" -> POSTGRESQL;
            case "oracle", "oracledatabase", "oracleconnectionfactoryprovider" -> ORACLE;
            case "sqlserver", "mssql", "microsoftsqlserver",
                    "mssqlconnectionfactoryprovider" -> SQL_SERVER;
            default -> UNKNOWN;
        };
    }

    /** Detect a standard driver or metadata description while keeping unknown products explicit. */
    public static DatabaseProduct detect(String description) {
        DatabaseProduct exact = fromName(description);
        if (exact != UNKNOWN) {
            return exact;
        }
        String name = normalize(description);
        if (name.contains("mariadb") || name.contains("mysql")) {
            return MYSQL;
        }
        if (name.contains("postgres")) {
            return POSTGRESQL;
        }
        if (name.contains("oracle")) {
            return ORACLE;
        }
        if (name.contains("sqlserver") || name.contains("mssql")) {
            return SQL_SERVER;
        }
        return name.contains("h2database") ? H2 : UNKNOWN;
    }

    private static String normalize(String name) {
        if (name == null || name.isBlank()) {
            return "";
        }
        StringBuilder normalized = new StringBuilder(name.length());
        for (int index = 0; index < name.length(); index++) {
            char character = name.charAt(index);
            if (Character.isLetterOrDigit(character)) {
                normalized.append(Character.toLowerCase(character));
            }
        }
        return normalized.toString();
    }
}
