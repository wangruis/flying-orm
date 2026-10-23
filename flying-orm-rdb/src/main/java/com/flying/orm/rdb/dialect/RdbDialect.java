package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * 聚合一个数据库在 DDL、分页、upsert 和 JSON 上的 SQL 能力。业务渲染器只依赖这个组合对象，
 * 不在各处用数据库名称写分支。
 *
 * <p>内置实例覆盖 H2、MySQL、PostgreSQL、Oracle 和 SQL Server；没有 OpenGauss 专用方言。
 * 方言始终由上层应用显式选择后传入，ORM 不探测连接或驱动。</p>
 *
 * @author wangr
 * @version v1.0
 */
public final class RdbDialect {

    private final String name;
    private final SchemaDialect schema;
    private final PaginationDialect pagination;
    private final UpsertDialect upsert;
    private final JsonDialect json;
    private final LockingReadDialect lockingRead;
    private final String version;
    private final DialectCapabilities capabilities;
    private final int maxIdentifierLength;
    private final Function<RdbException, RuntimeException> exceptionTranslator;

    private RdbDialect(String name,
                       SchemaDialect schema,
                       PaginationDialect pagination,
                       UpsertDialect upsert,
                       JsonDialect json,
                       LockingReadDialect lockingRead,
                       String version,
                       DialectCapabilities capabilities,
                       int maxIdentifierLength,
                       Function<RdbException, RuntimeException> exceptionTranslator) {
        this.name = requireText(name, "dialect name").toLowerCase(Locale.ROOT);
        this.schema = Objects.requireNonNull(schema, "schema dialect must not be null");
        this.pagination = Objects.requireNonNull(pagination, "pagination dialect must not be null");
        this.upsert = Objects.requireNonNull(upsert, "upsert dialect must not be null");
        this.json = Objects.requireNonNull(json, "json dialect must not be null");
        this.lockingRead = Objects.requireNonNull(lockingRead, "locking read dialect must not be null");
        this.version = requireText(version, "dialect version");
        this.capabilities = Objects.requireNonNull(capabilities, "dialect capabilities must not be null");
        if (maxIdentifierLength < 0) {
            throw new IllegalArgumentException("maximum identifier length must not be negative");
        }
        this.maxIdentifierLength = maxIdentifierLength;
        this.exceptionTranslator = exceptionTranslator;
    }

    /**
     * 创建 H2 方言。
     *
     * @return H2 方言
     */
    public static RdbDialect h2() {
        return BuiltInRdbDialects.h2();
    }

    /**
     * 创建 MySQL 方言。
     *
     * <p>MySQL 没有携带时区的时间戳类型。逻辑 {@code TIMESTAMPTZ} 使用原生 {@code TIMESTAMP}，
     * JDBC/R2DBC 连接必须把会话时区固定为 UTC；flying-orm 在参数边界使用 UTC {@code LocalDateTime}，
     * 不在 SQL 热路径增加会话探测。普通 {@code TIMESTAMP} 仍映射 {@code DATETIME}，保持本地时间语义。</p>
     *
     * @return MySQL 方言
     */
    public static RdbDialect mysql() {
        return BuiltInRdbDialects.mysql();
    }

    /**
     * 创建 PostgreSQL 方言。
     *
     * @return PostgreSQL 方言
     */
    public static RdbDialect postgresql() {
        return BuiltInRdbDialects.postgresql();
    }

    /**
     * 创建 Oracle 方言。
     *
     * @return Oracle 方言
     */
    public static RdbDialect oracle() {
        return oracle(OracleVersion.V19C);
    }

    /**
     * 按明确版本创建 Oracle 方言。默认 factory 仍使用稳定的 19c 基线；只有调用方声明 21c/23ai，
     * 才会启用原生 JSON 或 SQL BOOLEAN，避免在旧库上生成无法执行的 DDL。
     */
    public static RdbDialect oracle(OracleVersion version) {
        return BuiltInRdbDialects.oracle(version);
    }

    /**
     * 创建 SQL Server 方言。
     *
     * @return SQL Server 方言
     */
    public static RdbDialect sqlServer() {
        return sqlServer(SqlServerVersion.V2022);
    }

    /**
     * 按明确版本创建 SQL Server 方言。2012 是当前最低代码契约，2016 起额外声明 JSON 函数能力。
     */
    public static RdbDialect sqlServer(SqlServerVersion version) {
        return BuiltInRdbDialects.sqlServer(version);
    }

    /**
     * 自己组装一个数据库方言，并指定 upsert 写法。
     *
     * @param name       名字，比如 mysql
     * @param schema     建表、改表这类 SQL 的写法
     * @param pagination 分页 SQL 的写法
     * @param upsert     upsert SQL 的写法
     * @return 自定义 RDB 方言
     */
    public static RdbDialect of(String name, SchemaDialect schema, PaginationDialect pagination, UpsertDialect upsert) {
        return of(name, schema, pagination, upsert, JsonDialect.plain());
    }

    /**
     * 自己组装完整方言。数据库对 JSON 参数有特殊类型要求时，用 json 参数把写法交代清楚。
     */
    public static RdbDialect of(String name,
                                SchemaDialect schema,
                                PaginationDialect pagination,
                                UpsertDialect upsert,
                                JsonDialect json) {
        return new RdbDialect(name,
                              schema,
                              pagination,
                              upsert,
                              json,
                              LockingReadDialect.unsupported(),
                              "unspecified",
                              DialectCapabilities.empty(),
                              0, null);
    }

    /**
     * 显式组装带能力事实的方言。
     *
     * <p>能力直接使用调用方声明的快照，标识符上限为 0 表示未知。</p>
     */
    public static RdbDialect of(String name,
                                                SchemaDialect schema,
                                                PaginationDialect pagination,
                                                UpsertDialect upsert,
                                                JsonDialect json,
                                                String version,
                                                DialectCapabilities capabilities,
                                                int maxIdentifierLength) {
        return new RdbDialect(name,
                              schema,
                              pagination,
                              upsert,
                              json,
                              LockingReadDialect.unsupported(),
                              version,
                              capabilities,
                              maxIdentifierLength, null);
    }

    static RdbDialect builtIn(String name,
                              SchemaDialect schema,
                              PaginationDialect pagination,
                              UpsertDialect upsert,
                              JsonDialect json,
                              String version,
                              Set<DialectCapabilityId> capabilities,
                              int maxIdentifierLength) {
        return new RdbDialect(name,
                              schema,
                              pagination,
                              upsert,
                              json,
                              builtInLockingRead(name),
                              version,
                              DialectCapabilities.copyOf(capabilities),
                              maxIdentifierLength, null);
    }

    /** @return 当前数据库方言名称 */
    public String name() {
        return name;
    }

    /** @return 建表、改表等结构 SQL 方言 */
    public SchemaDialect schema() {
        return schema;
    }

    /**
     * 定制当前数据库的 DDL 写法，保留其他渲染器、版本和能力声明，不修改原实例。
     * 这是同一数据库的扩展入口；切换数据库应使用对应 factory 或显式组装方言。
     */
    public RdbDialect withSchema(SchemaDialect schema) {
        return new RdbDialect(name, schema, pagination, upsert, json, lockingRead,
                              version, capabilities, maxIdentifierLength, exceptionTranslator);
    }

    /** 显式转换数据库分类异常；不转换业务异常、生成键结果或批量证据载体，不增加重试。 */
    public RdbDialect withExceptionTranslator(Function<RdbException, RuntimeException> translator) {
        return new RdbDialect(name, schema, pagination, upsert, json, lockingRead,
                version, capabilities, maxIdentifierLength, Objects.requireNonNull(translator, "exception translator"));
    }

    /** 未配置时执行器不为复合操作增加异常映射节点。 */
    public boolean hasExceptionTranslator() {
        return exceptionTranslator != null;
    }

    /** 保留默认数据库分类与原始 cause，再按应用声明转换；结果事实由执行所有者保留。 */
    public RuntimeException translateException(Throwable error) {
        RuntimeException translated = RdbExceptionTranslator.translate(error);
        return exceptionTranslator != null && translated instanceof RdbException database
                ? Objects.requireNonNull(exceptionTranslator.apply(database), "translated exception") : translated;
    }

    /** 批量先记录原始执行事实，再转换对外 cause；未配置或未命中时保持原异常身份。 */
    public Throwable mapException(Throwable error) {
        if (exceptionTranslator == null || error instanceof Error) return error;
        RuntimeException normalized = RdbExceptionTranslator.translate(error);
        if (!(normalized instanceof RdbException database)) return error;
        RuntimeException mapped = Objects.requireNonNull(exceptionTranslator.apply(database), "translated exception");
        return mapped == normalized ? error : mapped;
    }

    /** @return 分页 SQL 方言 */
    public PaginationDialect pagination() {
        return pagination;
    }

    /** @return insert 主键冲突时的 upsert SQL 方言 */
    public UpsertDialect upsert() {
        return upsert;
    }

    /**
     * JSON 参数在当前数据库里的绑定表达式。
     */
    public JsonDialect json() {
        return json;
    }

    /**
     * @return 当前方言的锁定读取渲染器；未提供实现时明确报告不支持
     */
    public LockingReadDialect lockingReadDialect() {
        return lockingRead;
    }

    /** @return 当前方言按哪个数据库版本边界生成 SQL */
    public String version() {
        return version;
    }

    /** @return 构造时冻结的只读方言能力；不会在 SQL 热路径重新推断 */
    public DialectCapabilities capabilities() {
        return capabilities;
    }

    /**
     * @return 当前数据库普通对象名上限；0 表示版本或自定义方言没有提供可信上限
     */
    public int maxIdentifierLength() {
        return maxIdentifierLength;
    }

    /**
     * 判断当前版本配置是否明确支持某项能力。返回 false 表示 flying-orm 不会承诺生成可执行 SQL，
     * 不等于数据库厂商从未提供该功能。
     */
    public boolean supports(DialectCapabilityId capability) {
        return capabilities.supports(capability);
    }

    private static LockingReadDialect builtInLockingRead(String name) {
        return switch (requireText(name, "dialect name").toLowerCase(Locale.ROOT)) {
            case "h2", "mysql", "postgresql" -> LockingReadDialect.forUpdateSuffix();
            case "oracle" -> LockingReadDialect.forUpdateSuffixWithoutPagination();
            case "sqlserver" -> LockingReadDialect.sqlServerTableHint();
            default -> LockingReadDialect.unsupported();
        };
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
