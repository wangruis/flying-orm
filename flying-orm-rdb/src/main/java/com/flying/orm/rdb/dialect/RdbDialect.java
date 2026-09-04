package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.schema.SchemaDialect;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 聚合一个数据库在 DDL、分页、upsert 和 JSON 上的 SQL 能力。业务渲染器只依赖这个组合对象，
 * 不在各处用数据库名称写分支。
 *
 * <p>内置实例覆盖 H2、MySQL、PostgreSQL、Oracle 和 SQL Server；没有 OpenGauss 专用方言。
 * 上层应用可以根据连接工厂元数据或配置自动选择，独立使用时也允许显式传入。</p>
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
    private final Set<DialectFeature> features;

    private RdbDialect(String name,
                       SchemaDialect schema,
                       PaginationDialect pagination,
                       UpsertDialect upsert,
                       JsonDialect json,
                       LockingReadDialect lockingRead,
                       String version,
                       DialectCapabilities capabilities,
                       int maxIdentifierLength) {
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
        this.features = legacyFeatures(capabilities);
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
                              0);
    }

    /**
     * 显式组装带能力事实的方言。
     *
     * <p>这个方法使用独立名称，不与历史 {@code of(..., JsonDialect)} 形成同参数位重载；
     * 因此旧代码把 {@code null} 传给 JSON 参数时仍能按原签名编译。标识符上限为 0 表示未知，
     * 调用方必须在需要生成受限对象名的路径上 fail closed。</p>
     */
    public static RdbDialect ofWithCapabilities(String name,
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
                              maxIdentifierLength);
    }

    static RdbDialect builtIn(String name,
                              SchemaDialect schema,
                              PaginationDialect pagination,
                              UpsertDialect upsert,
                              JsonDialect json,
                              String version,
                              Set<DialectFeature> features,
                              int maxIdentifierLength) {
        return new RdbDialect(name,
                              schema,
                              pagination,
                              upsert,
                              json,
                              builtInLockingRead(name),
                              version,
                              DialectCapabilities.from(features),
                              maxIdentifierLength);
    }

    /** @return 当前数据库方言名称 */
    public String name() {
        return name;
    }

    /** @return 建表、改表等结构 SQL 方言 */
    public SchemaDialect schema() {
        return schema;
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
     * @return 已确认版本的受控锁定读取渲染器；旧自定义方言默认 fail closed
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
    public boolean supports(DialectFeature feature) {
        return features.contains(Objects.requireNonNull(feature, "dialect feature must not be null"));
    }

    private static Set<DialectFeature> legacyFeatures(DialectCapabilities capabilities) {
        EnumSet<DialectFeature> features = EnumSet.noneOf(DialectFeature.class);
        for (DialectFeature feature : DialectFeature.values()) {
            if (capabilities.supports(DialectCapabilityId.from(feature))) {
                features.add(feature);
            }
        }
        return Set.copyOf(features);
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
