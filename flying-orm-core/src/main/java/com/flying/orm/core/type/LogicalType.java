package com.flying.orm.core.type;

/**
 * Database-independent meaning of a declared column type.
 *
 * <p>The enum is deliberately small. Vendor spelling, precision and modifiers stay in
 * {@link DatabaseType}; this type only answers decisions shared by SQL validation, value conversion and
 * result decoding.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public enum LogicalType {

    SMALL_INTEGER(true, false, false, false),
    INTEGER(true, false, false, false),
    BIG_INTEGER(true, false, false, false),
    DECIMAL(true, false, false, false),
    FLOAT(true, false, false, false),
    BOOLEAN(false, false, false, false),
    TEXT(false, true, false, false),
    BINARY(false, false, true, false),
    DATE(false, false, false, true),
    TIME(false, false, false, true),
    OFFSET_TIME(false, false, false, true),
    TIMESTAMP(false, false, false, true),
    OFFSET_TIMESTAMP(false, false, false, true),
    JSON(false, false, false, false),
    UUID(false, false, false, false),
    XML(false, false, false, false),
    VECTOR(false, false, false, false),
    INTERVAL(false, false, false, false),
    OTHER(false, false, false, false);

    private final boolean numeric;
    private final boolean textual;
    private final boolean binary;
    private final boolean temporal;

    LogicalType(boolean numeric, boolean textual, boolean binary, boolean temporal) {
        this.numeric = numeric;
        this.textual = textual;
        this.binary = binary;
        this.temporal = temporal;
    }

    /** @return whether arithmetic operations have a stable cross-dialect meaning */
    public boolean numeric() {
        return numeric;
    }

    /** @return whether the database type is plain character storage */
    public boolean textual() {
        return textual;
    }

    /** @return whether the database type is binary storage */
    public boolean binary() {
        return binary;
    }

    /** @return whether the database type represents a date or time value */
    public boolean temporal() {
        return temporal;
    }
}
