package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * 描述一套数据库 DDL 写法。
 *
 * <p>类型映射、DDL 语法和生成值规则分别由包内协作者
 * 完成。{@link #standard()} 表示较通用的 SQL 标准写法，不是某个叫 ANSI 的数据库。</p>
 *
 * <p>构建完成后的对象只读，可以并发共享。构建器只建议在启动阶段使用；类型映射和所有不能绑定的
 * 普通 DDL 片段都会经过白名单校验；显式原生默认表达式属于可信后端 SQL，只校验词法边界。</p>
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

    public enum TableCommentStyle {
        NONE,
        MYSQL_OPTION,
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
    private final boolean concurrentIndexesSupported;
    private final SchemaOperationRenderer operationRenderer;
    private final FormSchemaDdlRenderer formRenderer;
    private SchemaDialect(String quoteOpen,
                          String quoteClose,
                          Map<String, DatabaseType> typeMappings,
                          Map<String, Function<DatabaseType, String>> typeRenderers,
                          ColumnCommentStyle columnCommentStyle,
                          TableCommentStyle tableCommentStyle,
                          DropIndexStyle dropIndexStyle,
                          RenameColumnStyle renameColumnStyle,
                          GeneratedValueStyle generatedValueStyle,
                          ColumnChangeStyle columnChangeStyle,
                          boolean concurrentIndexesSupported,
                          SchemaOperationRenderer operationRenderer,
                          FormSchemaDdlRenderer formRenderer) {
        this.types = new SchemaDialectTypeSupport(
                quoteOpen, quoteClose, typeMappings, typeRenderers, generatedValueStyle);
        this.ddl = new SchemaDialectDdlSupport(types,
                                               columnCommentStyle,
                                               tableCommentStyle,
                                               dropIndexStyle,
                                               renameColumnStyle,
                                               generatedValueStyle,
                                               columnChangeStyle,
                                               concurrentIndexesSupported);
        this.generation = new SchemaDialectGenerationSupport(types, generatedValueStyle);
        this.generatedValueStyle = Objects.requireNonNull(generatedValueStyle,
                                                          "generated value style must not be null");
        this.operationRenderer = operationRenderer;
        this.formRenderer = formRenderer;
        this.concurrentIndexesSupported = concurrentIndexesSupported;
    }

    /** 创建名字不加引号、类型原样输出的通用 DDL 写法。 */
    public static SchemaDialect standard() {
        return builder().build();
    }

    /** 创建可以配置标识符引号、类型映射和数据库特殊 DDL 的构建器。 */
    public static Builder builder() {
        return new Builder();
    }

    private SchemaDialect(SchemaDialect source, SchemaOperationRenderer renderer, FormSchemaDdlRenderer formRenderer) {
        this.types = source.types;
        this.ddl = source.ddl;
        this.generation = source.generation;
        this.generatedValueStyle = source.generatedValueStyle;
        this.concurrentIndexesSupported = source.concurrentIndexesSupported;
        this.operationRenderer = renderer;
        this.formRenderer = formRenderer;
    }

    /** 替换完整关系模型的 DDL 渲染扩展，保留类型、引号、生成值及在线 DDL 配置，不修改原实例。 */
    public SchemaDialect withOperationRenderer(SchemaOperationRenderer renderer) {
        return new SchemaDialect(this, Objects.requireNonNull(renderer,
                "schema operation renderer must not be null"), formRenderer);
    }

    /** 替换动态表单 DDL 渲染扩展，保留其他配置，不修改原实例。 */
    public SchemaDialect withFormRenderer(FormSchemaDdlRenderer renderer) {
        return new SchemaDialect(this, operationRenderer, Objects.requireNonNull(renderer,
                "form schema renderer must not be null"));
    }

    FormSchemaDdlRenderer formRenderer() {
        return formRenderer;
    }

    /** 包内建表校验读取明确样式，不把数据库分支泄露到业务 API。 */
    GeneratedValueStyle generatedValueStyle() {
        return generatedValueStyle;
    }

    SchemaOperationRenderer operationRenderer() {
        return operationRenderer;
    }

    public String identifier(String value) {
        return types.identifier(value);
    }

    StringBuilder appendIdentifier(StringBuilder sql, String value) {
        return types.appendIdentifier(sql, value);
    }

    /**
     * 逐段渲染关系身份。每一段中的点号都属于名称本身，不会被再次拆分。
     */
    public String relationIdentifier(RelationIdentity value) {
        return types.identifier(value);
    }

    String schemaObjectIdentifier(RelationIdentity relation, String objectName) {
        return types.schemaObjectIdentifier(relation, objectName);
    }

    public String dataType(String value) {
        return types.dataType(value);
    }

    public String dataType(String value, Integer length, Integer precision, Integer scale) {
        return types.dataType(DatabaseType.of(value), length, precision, scale);
    }

    String mappedDataType(DatabaseType value, Integer length, Integer precision, Integer scale) {
        return types.dataType(value, length, precision, scale);
    }

    String physicalDataType(String value, Integer length, Integer precision, Integer scale) {
        return types.physicalDataType(value, length, precision, scale);
    }

    String metadataDataType(String value, Integer length, Integer precision, Integer scale) {
        return types.metadataDataType(value, length, precision, scale);
    }

    boolean sameDataType(String left, String right) {
        return types.sameDataType(left, right);
    }

    public boolean inlineColumnComment() {
        return ddl.inlineColumnComment();
    }

    public Optional<String> columnCommentSql(String table, String column, String comment) {
        return columnCommentSql(RelationIdentity.parse(table), column, comment);
    }

    public Optional<String> columnCommentSql(RelationIdentity table, String column, String comment) {
        return ddl.columnCommentSql(table, column, comment);
    }

    String inlineTableCommentClause(String comment) {
        return ddl.inlineTableCommentClause(comment);
    }

    Optional<String> tableCommentSql(RelationIdentity table, String comment) {
        return ddl.tableCommentSql(table, comment);
    }

    Optional<String> columnCommentChangeSql(RelationIdentity table,
                                            String column,
                                            String previousComment,
                                            String targetComment) {
        return ddl.columnCommentChangeSql(table, column, previousComment, targetComment);
    }

    public String dropIndexSql(String table, String index) {
        return dropIndexSql(RelationIdentity.parse(table), index);
    }

    public String dropIndexSql(RelationIdentity table, String index) {
        return ddl.dropIndexSql(table, index);
    }

    String dropConstraintSql(RelationIdentity table, String constraint) {
        return ddl.dropConstraintSql(table, constraint);
    }

    String dropConstraintSql(RelationIdentity table, String constraint, SchemaOperation.Kind kind) {
        return ddl.dropConstraintSql(table, constraint, kind);
    }

    String changeConstraintSql(RelationIdentity table,
                               String actualConstraint,
                               String desiredDefinition,
                               SchemaOperation.Kind kind) {
        return ddl.changeConstraintSql(table, actualConstraint, desiredDefinition, kind);
    }

    String dropColumnSql(RelationIdentity table, String column) {
        return ddl.dropColumnSql(table, column);
    }

    String renameConstraintSql(RelationIdentity table, String actual, String desired) {
        return ddl.renameConstraintSql(table, actual, desired);
    }

    String renameIndexSql(RelationIdentity table, String actual, String desired) {
        return ddl.renameIndexSql(table, actual, desired);
    }

    String dropTableSql(RelationIdentity table) {
        return ddl.dropTableSql(table);
    }

    String alterColumnDefaultSql(RelationIdentity table,
                                 String column,
                                 String defaultExpression) {
        return ddl.alterColumnDefaultSql(table, column, defaultExpression);
    }

    public boolean concurrentIndexesSupported() {
        return concurrentIndexesSupported;
    }

    public SqlRequest preferConcurrentIndex(SqlRequest request) {
        return ddl.preferConcurrentIndex(request);
    }

    public String renameColumnSql(String table, String oldName, String newName) {
        return renameColumnSql(RelationIdentity.parse(table), oldName, newName);
    }

    public String renameColumnSql(RelationIdentity table, String oldName, String newName) {
        return ddl.renameColumnSql(table, oldName, newName);
    }

    public String addColumnSql(String table, String columnDefinition) {
        return ddl.addColumnSql(RelationIdentity.parse(table), columnDefinition);
    }

    String addColumnSql(RelationIdentity table, String columnDefinition, ValueGeneration generation) {
        return addGeneratedTableOption(ddl.addColumnSql(table, columnDefinition), generation);
    }

    private String addGeneratedTableOption(String sql, ValueGeneration generation) {
        String option = generatedTableClause(generation);
        return option.isEmpty() ? sql : sql + "," + option;
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
        return ddl.alterColumnTypeSql(RelationIdentity.parse(table), column, databaseType);
    }

    String alterColumnTypeSql(RelationIdentity table, String column, String databaseType,
                              String columnDefinition, boolean nullable, String collation) {
        return ddl.alterColumnTypeSql(table, column, databaseType, columnDefinition, nullable, collation);
    }

    /** 供迁移计划判断内联注释能否随完整列定义一起安全重放。 */
    boolean rewritesFullColumnDefinition() {
        return ddl.rewritesFullColumnDefinition();
    }
    String alterColumnNullabilitySql(RelationIdentity table, String column,
                                     String databaseType, String columnDefinition,
                                     boolean nullable, String collation) {
        return ddl.alterColumnNullabilitySql(table, column, databaseType, columnDefinition, nullable, collation);
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

    public String quoteLiteral(String value) {
        return types.quoteLiteral(value);
    }

    String commentLiteral(String value) {
        return ddl.commentLiteral(value);
    }

    String valueLiteral(String value) {
        return types.valueLiteral(value);
    }

    /**
     * 构建器只负责收集配置，build 时一次性创建不可变方言对象。
     */
    public static final class Builder {

        private String quoteOpen;
        private String quoteClose;
        private final Map<String, DatabaseType> typeMappings = new LinkedHashMap<>();
        private final Map<String, Function<DatabaseType, String>> typeRenderers = new LinkedHashMap<>();
        private ColumnCommentStyle columnCommentStyle = ColumnCommentStyle.NONE;
        private TableCommentStyle tableCommentStyle = TableCommentStyle.NONE;
        private DropIndexStyle dropIndexStyle = DropIndexStyle.NAME_ONLY;
        private RenameColumnStyle renameColumnStyle = RenameColumnStyle.ALTER_TABLE;
        private GeneratedValueStyle generatedValueStyle = GeneratedValueStyle.NONE;
        private ColumnChangeStyle columnChangeStyle = ColumnChangeStyle.STANDARD;
        private boolean concurrentIndexesSupported = false;
        private SchemaOperationRenderer operationRenderer;
        private FormSchemaDdlRenderer formRenderer;

        /** 定制动态表单已规划的 DDL，不替换差异规划或执行。 */
        public Builder formRenderer(FormSchemaDdlRenderer renderer) {
            formRenderer = Objects.requireNonNull(renderer, "form schema renderer must not be null");
            return this;
        }

        private Builder() {
        }

        /** 定制完整关系 Schema 的操作渲染，不替换差异规划或执行。 */
        public Builder operationRenderer(SchemaOperationRenderer renderer) {
            operationRenderer = Objects.requireNonNull(renderer, "schema operation renderer must not be null");
            return this;
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
            String key = SchemaDialectTypeSupport.canonicalDataType(logicalType);
            typeMappings.put(key, DatabaseType.of(databaseType).requireSafe("database type"));
            typeRenderers.remove(key);
            return this;
        }

        /**
         * 按列尺寸生成完整数据库类型声明；回调由后端注册，必须可并发调用。
         *
         * <p>回调接收合并 length/precision/scale 后的类型，返回值仍经过类型语法校验。
         * 优先匹配完整声明，再匹配去掉尺寸的声明；数组和修饰符不自动继承标量映射。
         * 同一个键最后注册的固定映射或回调生效，已构建方言不受后续注册影响。</p>
         *
         * @param logicalType 逻辑类型或带尺寸的精确声明
         * @param renderer 无状态类型渲染函数，返回完整类型声明
         * @return 当前构建器
         */
        public Builder mapType(String logicalType, Function<DatabaseType, String> renderer) {
            String key = SchemaDialectTypeSupport.canonicalDataType(logicalType);
            typeRenderers.put(key, Objects.requireNonNull(renderer, "type renderer must not be null"));
            typeMappings.remove(key);
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

        /** MySQL 把表注释放在 CREATE TABLE 的表选项中。 */
        public Builder mysqlTableComment() {
            tableCommentStyle = TableCommentStyle.MYSQL_OPTION;
            return this;
        }

        /** H2、PostgreSQL 和 Oracle 使用独立的 COMMENT ON TABLE 语句。 */
        public Builder commentOnTable() {
            tableCommentStyle = TableCommentStyle.COMMENT_ON;
            return this;
        }

        /** SQL Server 使用表级扩展属性保存注释。 */
        public Builder sqlServerExtendedPropertyTableComment() {
            tableCommentStyle = TableCommentStyle.SQL_SERVER_EXTENDED_PROPERTY;
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
        public Builder concurrentIndexes() {
            concurrentIndexesSupported = true;
            return this;
        }



        public SchemaDialect build() {
            return new SchemaDialect(quoteOpen,
                                     quoteClose,
                                     typeMappings,
                                     typeRenderers,
                                     columnCommentStyle,
                                     tableCommentStyle,
                                     dropIndexStyle,
                                     renameColumnStyle,
                                     generatedValueStyle,
                                     columnChangeStyle,
                                     concurrentIndexesSupported,
                                     operationRenderer,
                                     formRenderer);
        }
    }
}
