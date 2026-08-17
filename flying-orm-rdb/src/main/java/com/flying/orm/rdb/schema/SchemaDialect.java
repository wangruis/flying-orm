package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 描述一套数据库 DDL 写法。
 *
 * <p>这个类是稳定门面，负责保持公开 API；实际的类型安全、DDL 语法和生成值规则分别由包内协作者
 * 完成。{@link #standard()} 表示较通用的 SQL 标准写法，不是某个叫 ANSI 的数据库。</p>
 *
 * <p>构建完成后的对象只读，可以并发共享。构建器只建议在启动阶段使用；类型映射和所有不能绑定的
 * DDL 片段都会经过白名单校验。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SchemaDialect {
    public enum ColumnCommentStyle {
        NONE,
        INLINE,
        COMMENT_ON,
        SQL_SERVER_EXTENDED_PROPERTY
    }

    public enum DropIndexStyle {
        NAME_ONLY,
        ON_TABLE
    }

    public enum RenameColumnStyle {
        ALTER_TABLE,
        SQL_SERVER_SP_RENAME
    }

    public enum GeneratedValueStyle {
        NONE,
        H2,
        MYSQL,
        POSTGRESQL,
        ORACLE,
        SQL_SERVER
    }

    public enum ColumnChangeStyle {
        STANDARD,
        ORACLE,
        SQL_SERVER
    }

    private final SchemaDialectTypeSupport types;
    private final SchemaDialectDdlSupport ddl;
    private final SchemaDialectGenerationSupport generation;
    private final GeneratedValueStyle generatedValueStyle;
    private final SchemaOnlineDdlSupport onlineDdlSupport;
    private SchemaDialect(String quoteOpen,
                          String quoteClose,
                          Map<String, String> typeMappings,
                          ColumnCommentStyle columnCommentStyle,
                          DropIndexStyle dropIndexStyle,
                          RenameColumnStyle renameColumnStyle,
                          GeneratedValueStyle generatedValueStyle,
                          ColumnChangeStyle columnChangeStyle,
                          SchemaOnlineDdlSupport onlineDdlSupport,
                          SchemaLockTimeoutStyle lockTimeoutStyle) {
        this.types = new SchemaDialectTypeSupport(quoteOpen, quoteClose, typeMappings, generatedValueStyle);
        this.ddl = new SchemaDialectDdlSupport(types,
                                               columnCommentStyle,
                                               dropIndexStyle,
                                               renameColumnStyle,
                                               generatedValueStyle,
                                               columnChangeStyle,
                                               onlineDdlSupport,
                                               lockTimeoutStyle);
        this.generation = new SchemaDialectGenerationSupport(types, generatedValueStyle);
        this.generatedValueStyle = Objects.requireNonNull(generatedValueStyle,
                                                          "generated value style must not be null");
        this.onlineDdlSupport = Objects.requireNonNull(onlineDdlSupport,
                                                       "online DDL support must not be null");
    }

    /** 创建名字不加引号、类型原样输出的通用 DDL 写法。 */
    public static SchemaDialect standard() {
        return builder().build();
    }

    /** 创建可以配置标识符引号、类型映射和数据库特殊 DDL 的构建器。 */
    public static Builder builder() {
        return new Builder();
    }

    /** 包内建表校验读取明确样式，不把数据库分支泄露到业务 API。 */
    GeneratedValueStyle generatedValueStyle() {
        return generatedValueStyle;
    }

    public String identifier(String value) {
        return types.identifier(value);
    }

    public String dataType(String value) {
        return types.dataType(value);
    }

    public String dataType(String value, Integer length, Integer precision, Integer scale) {
        return types.dataType(value, length, precision, scale);
    }

    public boolean inlineColumnComment() {
        return ddl.inlineColumnComment();
    }

    public Optional<String> columnCommentSql(String table, String column, String comment) {
        return ddl.columnCommentSql(table, column, comment);
    }

    Optional<String> columnCommentChangeSql(String table,
                                            String column,
                                            String previousComment,
                                            String targetComment) {
        return ddl.columnCommentChangeSql(table, column, previousComment, targetComment);
    }

    public String dropIndexSql(String table, String index) {
        return ddl.dropIndexSql(table, index);
    }

    public SchemaOnlineDdlSupport onlineDdlSupport() {
        return onlineDdlSupport;
    }

    public SqlRequest preferOnline(SqlRequest request) {
        return ddl.preferOnline(request);
    }

    SchemaDdlSessionGuard lockTimeoutGuard(Duration timeout) {
        return ddl.lockTimeoutGuard(timeout);
    }

    public String renameColumnSql(String table, String oldName, String newName) {
        return ddl.renameColumnSql(table, oldName, newName);
    }

    public String addColumnSql(String table, String columnDefinition) {
        return ddl.addColumnSql(table, columnDefinition);
    }

    /**
     * 生成只依赖目标类型的列类型变更 SQL。
     *
     * <p>MySQL 和 SQL Server 修改类型时还需要完整列定义来保留可空性、生成策略和其他列属性，
     * 因此这两个内置方言会拒绝此低信息入口；表单迁移器使用包内完整定义入口。</p>
     *
     * @param table        表名
     * @param column       字段名
     * @param databaseType 目标数据库类型
     * @return 可安全执行的 DDL
     * @throws IllegalArgumentException 当前方言不能仅凭目标类型安全生成 DDL
     */
    public String alterColumnTypeSql(String table, String column, String databaseType) {
        return ddl.alterColumnTypeSql(table, column, databaseType);
    }

    /** MySQL 修改列类型时必须重放完整列定义，避免隐式丢失非空、生成策略或注释。 */
    String alterColumnTypeSql(String table,
                              String column,
                              String databaseType,
                              String columnDefinition) {
        return ddl.alterColumnTypeSql(table, column, databaseType, columnDefinition);
    }

    /** 供迁移计划判断内联注释能否随完整列定义一起安全重放。 */
    boolean rewritesFullColumnDefinition() {
        return ddl.rewritesFullColumnDefinition();
    }
    /** nullable 变更只给包内迁移计划使用；MySQL 需要完整字段定义来保留列属性。 */
    String alterColumnNullabilitySql(String table, String column,
                                     String databaseType, String columnDefinition,
                                     boolean nullable) {
        return ddl.alterColumnNullabilitySql(table, column, databaseType, columnDefinition, nullable);
    }
    public String generatedValueClause(ValueGeneration generation, String databaseType) {
        return this.generation.generatedValueClause(generation, databaseType);
    }

    String generatedTableClause(ValueGeneration generation) {
        return this.generation.generatedTableClause(generation);
    }

    public Optional<String> createSequenceSql(ValueGeneration generation, String databaseType) {
        return this.generation.createSequenceSql(generation, databaseType);
    }

    Optional<String> dropSequenceSql(ValueGeneration generation) {
        return this.generation.dropSequenceSql(generation);
    }
    public String quoteLiteral(String value) {
        return types.quoteLiteral(value);
    }

    /**
     * 构建器只负责收集配置，build 时一次性创建不可变方言对象。
     * 公开方法名称保持原样，避免使用方因为内部拆分被迫改代码。
     */
    public static final class Builder {

        private String quoteOpen;
        private String quoteClose;
        private final Map<String, String> typeMappings = new LinkedHashMap<>();
        private ColumnCommentStyle columnCommentStyle = ColumnCommentStyle.NONE;
        private DropIndexStyle dropIndexStyle = DropIndexStyle.NAME_ONLY;
        private RenameColumnStyle renameColumnStyle = RenameColumnStyle.ALTER_TABLE;
        private GeneratedValueStyle generatedValueStyle = GeneratedValueStyle.NONE;
        private ColumnChangeStyle columnChangeStyle = ColumnChangeStyle.STANDARD;
        private SchemaOnlineDdlSupport onlineDdlSupport = SchemaOnlineDdlSupport.NONE;
        private SchemaLockTimeoutStyle lockTimeoutStyle = SchemaLockTimeoutStyle.NONE;

        private Builder() {
        }

        /** 设置一对相同的标识符边界，比如 MySQL 的反引号。 */
        public Builder quoteIdentifiers(char quote) {
            return quoteIdentifiers(quote, quote);
        }

        /** 设置一对标识符边界，比如 SQL Server 的方括号。 */
        public Builder quoteIdentifiers(char open, char close) {
            this.quoteOpen = String.valueOf(open);
            this.quoteClose = String.valueOf(close);
            return this;
        }

        /** 把业务类型映射成数据库类型；两边都按安全类型语法校验。 */
        public Builder mapType(String logicalType, String databaseType) {
            typeMappings.put(SchemaDialectTypeSupport.normalize(
                                     SchemaDialectTypeSupport.requireDataType(logicalType, "logical type")),
                             SchemaDialectTypeSupport.requireDataType(databaseType, "database type"));
            return this;
        }

        public Builder inlineColumnComment() {
            columnCommentStyle = ColumnCommentStyle.INLINE;
            return this;
        }

        public Builder commentOnColumn() {
            columnCommentStyle = ColumnCommentStyle.COMMENT_ON;
            return this;
        }

        public Builder sqlServerExtendedPropertyComment() {
            columnCommentStyle = ColumnCommentStyle.SQL_SERVER_EXTENDED_PROPERTY;
            return this;
        }

        public Builder dropIndexOnTable() {
            dropIndexStyle = DropIndexStyle.ON_TABLE;
            return this;
        }

        public Builder sqlServerRenameColumn() {
            renameColumnStyle = RenameColumnStyle.SQL_SERVER_SP_RENAME;
            return this;
        }

        /** 选择当前数据库的标识列和序列语法；具体 SQL 仍由生成值支持类集中渲染。 */
        public Builder generatedValues(GeneratedValueStyle style) {
            generatedValueStyle = Objects.requireNonNull(style, "generated value style must not be null");
            return this;
        }

        /** Oracle 方言的易读快捷配置，和 {@link #generatedValues(GeneratedValueStyle)} 使用同一实现。 */
        public Builder oracleGeneratedValues() {
            return generatedValues(GeneratedValueStyle.ORACLE);
        }

        /** SQL Server 方言的易读快捷配置，和 {@link #generatedValues(GeneratedValueStyle)} 使用同一实现。 */
        public Builder sqlServerGeneratedValues() {
            return generatedValues(GeneratedValueStyle.SQL_SERVER);
        }

        public Builder oracleColumnChanges() {
            columnChangeStyle = ColumnChangeStyle.ORACLE;
            return this;
        }

        public Builder sqlServerColumnChanges() {
            columnChangeStyle = ColumnChangeStyle.SQL_SERVER;
            return this;
        }

        /** PostgreSQL 明确支持的并发建索引能力。 */
        public Builder concurrentIndexOnlineDdl() {
            onlineDdlSupport = SchemaOnlineDdlSupport.CONCURRENT_INDEX;
            return this;
        }

        /** MySQL 在线 alter 依赖具体动作和存储引擎，只记录能力提示。 */
        public Builder operationDependentOnlineDdl() {
            onlineDdlSupport = SchemaOnlineDdlSupport.OPERATION_DEPENDENT;
            return this;
        }

        /** Oracle、SQL Server 的在线能力受版本或授权影响，不默认打开。 */
        public Builder licenseOrEditionDependentOnlineDdl() {
            onlineDdlSupport = SchemaOnlineDdlSupport.LICENSE_OR_EDITION_DEPENDENT;
            return this;
        }

        /** 使用 MySQL session lock_wait_timeout，单位为向上取整后的秒。 */
        public Builder mysqlLockTimeout() {
            lockTimeoutStyle = SchemaLockTimeoutStyle.MYSQL;
            return this;
        }

        /** 使用 PostgreSQL lock_timeout，保留毫秒精度。 */
        public Builder postgresqlLockTimeout() {
            lockTimeoutStyle = SchemaLockTimeoutStyle.POSTGRESQL;
            return this;
        }

        /** 使用 Oracle ddl_lock_timeout，单位为向上取整后的秒。 */
        public Builder oracleLockTimeout() {
            lockTimeoutStyle = SchemaLockTimeoutStyle.ORACLE;
            return this;
        }

        /** 使用 SQL Server LOCK_TIMEOUT，单位为毫秒。 */
        public Builder sqlServerLockTimeout() {
            lockTimeoutStyle = SchemaLockTimeoutStyle.SQL_SERVER;
            return this;
        }

        public SchemaDialect build() {
            if ((quoteOpen == null) != (quoteClose == null)) {
                throw new IllegalStateException("identifier quote boundaries must be configured together");
            }
            return new SchemaDialect(quoteOpen,
                                     quoteClose,
                                     typeMappings,
                                     columnCommentStyle,
                                     dropIndexStyle,
                                     renameColumnStyle,
                                     generatedValueStyle,
                                     columnChangeStyle,
                                     onlineDdlSupport,
                                     lockTimeoutStyle);
        }
    }
}
