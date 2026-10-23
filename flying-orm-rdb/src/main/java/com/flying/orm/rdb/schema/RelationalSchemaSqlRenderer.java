package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.internal.template.SchemaDefaultExpressions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 把规范关系 operation 编译成确定顺序、零业务参数的 DDL 请求。
 *
 * <p>本类只消费已经校验过的关系对象，不读取数据库，也不从说明文字反向解析 SQL。当前方言不能
 * 无损表达的操作会明确抛出 {@link UnsupportedOperationException}，直接向调用方传播。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationalSchemaSqlRenderer {

    private static final StableDigest.Domain ADDED_DEFAULT_NAME_DOMAIN =
            StableDigest.domain("legacy-added-column-default/v1");

    private final SchemaDialect dialect;
    private final RelationalSchemaEvolutionSqlRenderer evolution;
    private RelationalSchemaSqlRenderer(SchemaDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        evolution = new RelationalSchemaEvolutionSqlRenderer(dialect, this);
    }

    public static RelationalSchemaSqlRenderer create(SchemaDialect dialect) {
        return new RelationalSchemaSqlRenderer(dialect);
    }
    /** 渲染一项结构化 operation；返回顺序就是审核和执行顺序。 */
    public List<SqlRequest> render(SchemaOperation operation) {
        return render(operation, new LinkedHashMap<>());
    }

    List<SqlRequest> render(SchemaOperation operation, Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences) {
        return render(operation, existingSequences, true);
    }

    List<SqlRequest> render(SchemaOperation operation,
                           Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                           boolean physicalActualTypes) {
        return render(operation, existingSequences, physicalActualTypes, null);
    }

    List<SqlRequest> render(SchemaOperation operation,
                           Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                           boolean physicalActualTypes,
                           DatabaseType observedActualType) {
        SchemaOperation source = Objects.requireNonNull(operation, "schema operation must not be null");
        return renderOperation(source, existingSequences, prepared -> renderBuiltIn(
                source, existingSequences, prepared, physicalActualTypes, observedActualType));
    }

    private List<SqlRequest> renderOperation(SchemaOperation operation,
                                            Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                                            Function<Map<String, SchemaSequenceSqlRenderer.Definition>, List<SqlRequest>> builtin) {
        if (operation.kind() == SchemaOperation.Kind.VERIFY_MANUALLY) throw unsupported(operation.kind());
        Map<String, SchemaSequenceSqlRenderer.Definition> createdSequences = new LinkedHashMap<>();
        SchemaOperationRenderer custom = dialect.operationRenderer();
        List<SqlRequest> requests;
        if (custom == null) {
            requests = builtin.apply(createdSequences);
        } else {
            Supplier<List<SqlRequest>> fallback = new Supplier<>() {
                private List<SqlRequest> result;

                @Override
                public List<SqlRequest> get() {
                    if (result == null) {
                        Map<String, SchemaSequenceSqlRenderer.Definition> prepared = new LinkedHashMap<>();
                        result = List.copyOf(builtin.apply(prepared));
                        createdSequences.putAll(prepared);
                    }
                    return result;
                }
            };
            requests = List.copyOf(custom.render(operation, fallback));
            if (requests.isEmpty()) throw new IllegalArgumentException("schema operation renderer returned no requests");
        }
        // 回调和默认渲染都成功之后才发布序列准备，失败操作不能占用后续步骤的序列。
        existingSequences.putAll(createdSequences);
        return requests;
    }

    private List<SqlRequest> renderBuiltIn(SchemaOperation source,
                                         Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                                         Map<String, SchemaSequenceSqlRenderer.Definition> createdSequences,
                                         boolean physicalActualTypes,
                                         DatabaseType observedActualType) {
        return switch (source.kind()) {
            case CREATE_TABLE -> createTable((RelationalTableDefinition) source.desired(),
                                            existingSequences, createdSequences);
            case ADD_COLUMN -> addColumn(source.relation(), (ColumnDefinition) source.desired(),
                                         existingSequences, createdSequences, false);
            case RENAME_COLUMN -> List.of(new SqlRequest(dialect.renameColumnSql(source.relation(),
                    ((ColumnDefinition) source.actual()).name(), ((ColumnDefinition) source.desired()).name()), List.of()));
            case CHANGE_COLUMN -> evolution.changeColumn(source,
                    SchemaDefinitionEquality.actualColumnDdlType(dialect,
                            (ColumnDefinition) source.actual(), physicalActualTypes),
                    physicalActualTypes ? observedActualType : ((ColumnDefinition) source.actual()).databaseType(), false);
            case DROP_COLUMN, DROP_TABLE, DROP_PRIMARY_KEY, DROP_UNIQUE,
                    DROP_INDEX, DROP_CHECK, DROP_FOREIGN_KEY -> List.of(evolution.drop(source));
            case ADD_PRIMARY_KEY -> List.of(addConstraint(
                    source.relation(), primaryKey((PrimaryKeyDefinition) source.desired())));
            case CHANGE_PRIMARY_KEY -> evolution.changeConstraint(
                    source,
                    ((PrimaryKeyDefinition) source.actual()).name(),
                    ((PrimaryKeyDefinition) source.desired()).name(),
                    primaryKey((PrimaryKeyDefinition) source.desired()));
            case ADD_UNIQUE -> List.of(createUnique(
                    source.relation(), (UniqueConstraintDefinition) source.desired()));
            case CHANGE_UNIQUE -> evolution.changeUnique(source);
            case ADD_INDEX -> List.of(createIndex(source.relation(), (IndexDefinition) source.desired()));
            case CHANGE_INDEX -> evolution.changeIndex(source);
            case ADD_CHECK -> List.of(addConstraint(
                    source.relation(), check((CheckConstraintDefinition) source.desired())));
            case CHANGE_CHECK -> evolution.changeConstraint(
                    source,
                    ((CheckConstraintDefinition) source.actual()).name(),
                    ((CheckConstraintDefinition) source.desired()).name(),
                    check((CheckConstraintDefinition) source.desired()));
            case ADD_FOREIGN_KEY -> List.of(addConstraint(
                    source.relation(), foreignKey((ForeignKeyDefinition) source.desired())));
            case CHANGE_FOREIGN_KEY -> evolution.changeConstraint(
                    source,
                    ((ForeignKeyDefinition) source.actual()).name(),
                    ((ForeignKeyDefinition) source.desired()).name(),
                    foreignKey((ForeignKeyDefinition) source.desired()));
            case VERIFY_MANUALLY -> throw unsupported(source.kind());
        };
    }
    Map<String, SchemaSequenceSqlRenderer.Definition> sequenceDefinitions(List<SchemaSnapshot> snapshots) {
        return SchemaSequenceSqlRenderer.definitions(dialect, snapshots, this::dataType);
    }

    private List<SqlRequest> createTable(RelationalTableDefinition table,
                                        Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                                        Map<String, SchemaSequenceSqlRenderer.Definition> createdSequences) {
        IndexDefinition inlineIndex = prepareTable(table);
        List<SqlRequest> requests = new ArrayList<>();
        for (ColumnDefinition column : table.columns()) {
            SchemaSequenceSqlRenderer.addCreate(
                    dialect, requests, column,
                    existingSequences, createdSequences, this::dataType);
        }

        StringJoiner definitions = new StringJoiner(", ");
        PrimaryKeyDefinition primaryKey = table.primaryKey().orElse(null);
        table.columns().forEach(column -> definitions.add(columnDefinition(column, dataType(column), false)));
        if (primaryKey != null) {
            definitions.add(primaryKey(primaryKey));
        }
        table.uniqueConstraints().stream()
                .filter(key -> SchemaDefinitionEquality.ordinaryUnique(key, dialect))
                .map(this::unique).forEach(definitions::add);
        if (inlineIndex != null) {
            definitions.add(indexDefinition(inlineIndex,
                    dialect.identifier(unqualifiedIdentifier(inlineIndex.name(), "index name")), ""));
        }
        table.checks().stream().map(this::check).forEach(definitions::add);
        Set<String> indexNames = new HashSet<>();
        table.indexes().forEach(index -> indexNames.add(index.name()));
        List<ForeignKeyDefinition> deferredForeignKeys = new ArrayList<>();
        for (ForeignKeyDefinition foreignKey : table.foreignKeys()) {
            List<String> referenceColumns = foreignKey.referenceColumns();
            // 自引用若只能由独立唯一索引提供候选键，必须等 CREATE INDEX 完成后再建立外键。
            // 已由内联 PK/UK 保证的引用仍留在 CREATE TABLE 中，不额外增加一次 ALTER。
            boolean indexedSelfReference = foreignKey.reference().equals(table.identity())
                    && table.primaryKey().filter(key -> key.columns().equals(referenceColumns)).isEmpty()
                    && table.uniqueConstraints().stream().noneMatch(key ->
                            key.nullPolicy() == UniqueNullPolicy.DEFAULT && key.columns().equals(referenceColumns))
                    && table.indexes().stream().anyMatch(index -> index.unique()
                            && index.keys().size() == referenceColumns.size()
                            && index.keys().stream().allMatch(key -> referenceColumns.contains(key.column())));
            if (indexNames.contains(foreignKey.name()) || indexedSelfReference) {
                // 同时保留 MySQL 同名支撑索引先于外键的顺序，避免数据库自动补出重复索引。
                deferredForeignKeys.add(foreignKey);
            } else {
                definitions.add(foreignKey(foreignKey));
            }
        }

        ValueGeneration identity = table.columns().stream()
                .map(ColumnDefinition::generation)
                .filter(value -> value.strategy() == ValueGeneration.Strategy.IDENTITY)
                .findFirst()
                .orElse(ValueGeneration.none());
        String sql = "create table " + dialect.relationIdentifier(table.identity()) + " (" + definitions + ")"
                + table.partition().map(partition -> switch (partition.strategy()) {
                    case RANGE -> " partition by range (" + dialect.identifier(partition.column()) + ')';
                }).orElse("")
                + dialect.generatedTableClause(identity)
                + dialect.inlineTableCommentClause(table.comment());
        requests.add(new SqlRequest(sql, List.of()));
        table.uniqueConstraints().stream()
                .filter(key -> !SchemaDefinitionEquality.ordinaryUnique(key, dialect))
                .map(key -> createUnique(table.identity(), key)).forEach(requests::add);
        table.indexes().stream().filter(index -> index != inlineIndex)
                .map(index -> createIndex(table.identity(), index)).forEach(requests::add);
        deferredForeignKeys.stream()
                .map(foreignKey -> addConstraint(table.identity(), foreignKey(foreignKey)))
                .forEach(requests::add);
        dialect.tableCommentSql(table.identity(), table.comment())
                .ifPresent(comment -> requests.add(new SqlRequest(comment, List.of())));
        for (ColumnDefinition column : table.columns()) {
            dialect.columnCommentSql(table.identity(), column.name(), storageComment(column))
                    .ifPresent(comment -> requests.add(new SqlRequest(comment, List.of())));
        }
        return List.copyOf(requests);
    }
    private List<SqlRequest> addColumn(RelationIdentity relation, ColumnDefinition column,
                                      Map<String, SchemaSequenceSqlRenderer.Definition> existingSequences,
                                      Map<String, SchemaSequenceSqlRenderer.Definition> createdSequences,
                                      boolean inlinePrimaryKey) {
        List<SqlRequest> requests = new ArrayList<>();
        SchemaSequenceSqlRenderer.addCreate(
                dialect, requests, column,
                existingSequences, createdSequences, this::dataType);
        requests.add(new SqlRequest(dialect.addColumnSql(relation,
                columnDefinition(column, dataType(column), inlinePrimaryKey), column.generation()), List.of()));
        String comment = storageComment(column);
        String commentSql = dialect.columnCommentSql(relation, column.name(), comment).orElse(null);
        if (comment != null && !dialect.inlineColumnComment() && commentSql == null) {
            throw unsupported(SchemaOperation.Kind.ADD_COLUMN);
        }
        if (commentSql != null) requests.add(new SqlRequest(commentSql, List.of()));
        return List.copyOf(requests);
    }

    private List<SqlRequest> addColumn(SchemaOperation operation, boolean inlinePrimaryKey,
                                  Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
        return renderOperation(operation, sequences, prepared -> addColumn(operation.relation(),
                (ColumnDefinition) operation.desired(), sequences, prepared, inlinePrimaryKey));
    }
    SqlRequest addConstraint(RelationIdentity relation, String definition) {
        return new SqlRequest("alter table " + dialect.relationIdentifier(relation) + " add " + definition, List.of());
    }
    SqlRequest createIndex(RelationIdentity relation, IndexDefinition index) {
        String localName = unqualifiedIdentifier(index.name(), "index name");
        String indexName = dialect.identifier(localName);
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                && relation.schema().isPresent()) {
            indexName = dialect.schemaObjectIdentifier(relation, localName);
        }
        return new SqlRequest("create " + indexDefinition(index, indexName,
                " on " + dialect.relationIdentifier(relation)), List.of());
    }

    private String indexDefinition(IndexDefinition index, String renderedName, String tableClause) {
        StringJoiner keys = new StringJoiner(", ");
        index.keys().forEach(key -> keys.add(
                dialect.identifier(unqualifiedIdentifier(key.column(), "index column")) + " "
                        + key.direction().name().toLowerCase(java.util.Locale.ROOT)));
        return (index.unique() ? "unique " : "") + "index " + renderedName + tableClause + " (" + keys + ')';
    }

    private static String unqualifiedIdentifier(String value, String category) {
        String identifier = SqlIdentifiers.requireIdentifier(value);
        if (identifier.indexOf('.') >= 0) {
            throw new IllegalArgumentException(category + " must not be qualified");
        }
        return identifier;
    }

    List<SqlRequest> changeColumn(SchemaOperation operation, String actualType, DatabaseType observedActualType) {
        return renderOperation(operation, new LinkedHashMap<>(), ignored ->
                evolution.changeColumn(operation, actualType, observedActualType, true));
    }

    String columnDefinition(ColumnDefinition column) {
        return columnDefinition(column, dataType(column));
    }

    String columnDefinition(ColumnDefinition column, String type) {
        return columnDefinition(column, column.charset(), column.collation(), type, storageComment(column));
    }

    /** 单列主键可内联声明，其余物理列规则共享同一实现。 */
    String columnDefinition(ColumnDefinition column, String type, boolean inlinePrimaryKey) {
        return columnDefinition(column, column.charset(), column.collation(), type, storageComment(column), inlinePrimaryKey);
    }

    String columnDefinition(ColumnDefinition column, String charset, String collation) {
        return columnDefinition(column, charset, collation, dataType(column), storageComment(column));
    }

    private String columnDefinition(ColumnDefinition column, String charset, String collation,
                                    String type, String storageComment) {
        return columnDefinition(column, charset, collation, type, storageComment, false);
    }

    private String columnDefinition(ColumnDefinition column, String charset, String collation,
                                    String type, String storageComment, boolean inlinePrimaryKey) {
        StringBuilder sql = new StringBuilder(dialect.identifier(column.name()))
                .append(' ').append(type);
        // 字符集和排序规则属于列类型；尤其 MySQL 的 CHARACTER SET 不能放到 NOT NULL 后面。
        if (charset != null) {
            if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.MYSQL) {
                throw new UnsupportedOperationException("current dialect cannot render a column charset");
            }
            sql.append(" character set ").append(dialect.identifier(charset));
        }
        if (collation != null) {
            switch (dialect.generatedValueStyle()) {
                case SQL_SERVER -> sql.append(" collate ")
                        .append(SchemaDialectTypeSupport.sqlServerCollation(collation));
                case MYSQL, POSTGRESQL ->
                        sql.append(" collate ").append(dialect.identifier(collation));
                default -> throw new UnsupportedOperationException(
                        "current dialect cannot render a column collation");
            }
        }
        String defaultConstraint = column.defaultConstraintName();
        if (defaultConstraint != null && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            throw new UnsupportedOperationException("named column defaults require the SQL Server dialect");
        }
        if (defaultConstraint != null && column.generation().strategy() == ValueGeneration.Strategy.SEQUENCE) {
            sql.append(" constraint ").append(dialect.identifier(defaultConstraint));
        }
        sql.append(dialect.generatedValueClause(column.generation(), type));
        boolean oracle = dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE;
        String defaultExpression = defaultExpression(column);
        if (oracle && defaultExpression != null) {
            // Oracle 的 DEFAULT 子句属于列定义，必须位于 NOT NULL 等列约束之前。
            sql.append(" default ").append(defaultExpression);
        }
        if (inlinePrimaryKey) {
            if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
                sql.append(" not null");
            }
            sql.append(" primary key");
        } else if (!column.nullable()) {
            sql.append(" not null");
        } else if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            sql.append(" null");
        }
        if (!oracle && defaultExpression != null) {
            if (defaultConstraint != null) {
                sql.append(" constraint ").append(dialect.identifier(defaultConstraint));
            }
            sql.append(" default ").append(defaultExpression);
        }
        if (dialect.inlineColumnComment() && storageComment != null) {
            sql.append(" comment ").append(dialect.commentLiteral(storageComment));
        }
        return sql.toString();
    }
    String storageComment(ColumnDefinition column) {
        return SchemaColumnCommentCodec.encode(
                dialect, column.databaseType(), column.generation(), column.comment());
    }
    String dataType(ColumnDefinition column) {
        return SchemaDefinitionEquality.desiredColumnType(dialect, column);
    }

    /** 普通增量输入的物理事实适配；SQL 和类型规则与完整关系操作共用。 */
    SchemaDialect.GeneratedValueStyle generatedValueStyle() {
        return dialect.generatedValueStyle();
    }

    List<SqlRequest> addColumn(RelationIdentity table, DynamicField field,
                               Map<String, SchemaSequenceSqlRenderer.Definition> sequences) {
        ColumnDefinition column = field.column();
        String defaultName = addedDefaultConstraintName(table.qualifiedName(), field);
        if (defaultName != null) column = column.toBuilder().defaultConstraintName(defaultName).build();
        return addColumn(SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, table,
                column.name(), null, column, SchemaOperation.Compatibility.SAFE_INCREMENTAL), field.primaryKey(), sequences);
    }

    private String addedDefaultConstraintName(String table, DynamicField field) {
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER
                || field.generation().strategy() != ValueGeneration.Strategy.SEQUENCE) {
            return null;
        }
        // 仅新加列使用可重复生成的名称；已有列始终使用数据库实际回读的名称。
        return "df_" + StableDigest.sha256(ADDED_DEFAULT_NAME_DOMAIN)
                .text("TABLE", table).text("COLUMN", field.name()).finishHex();
    }

    boolean requiresPhysicalCollation(ColumnDefinition field) {
        // JSON、OFFSET_TIME 等逻辑类型也可能映射为字符存储，以最终 DDL 类型为准。
        return dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.SQL_SERVER
                && SchemaDialectTypeSupport.sqlServerCharacterType(dataType(field));
    }

    boolean requiresPhysicalLengthUnit(ColumnDefinition field) {
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.ORACLE) return false;
        String base = com.flying.orm.core.type.DatabaseType.of(dataType(field)).baseName();
        return "CHAR".equals(base) || "VARCHAR2".equals(base);
    }

    String metadataDataType(ColumnDefinition column) {
        ColumnDefinition safeColumn = Objects.requireNonNull(column, "column metadata must not be null");
        return dialect.metadataDataType(safeColumn.databaseType().declaration(),
                                safeColumn.length(),
                                safeColumn.databaseType().logicalType().temporal()
                                        ? safeColumn.temporalPrecision() : safeColumn.precision(),
                                safeColumn.scale());
    }

    boolean sameDataType(String left, String right) {
        return dialect.sameDataType(left, right);
    }

    /** 先把输入声明适配为物理差异；无变化列不能进入依赖排序或触发后端扩展。 */
    SchemaOperation prepareColumnChange(SchemaOperation operation, SchemaSnapshot snapshot, boolean declaredSource) {
        ColumnDefinition source = (ColumnDefinition) operation.actual();
        ColumnDefinition target = (ColumnDefinition) operation.desired();
        ColumnDefinition physical = snapshot == null ? null : snapshot.knownDefinition().column(source.name());
        ColumnDefinition actual = physical == null ? source : physical;
        String actualType = actualType(source, snapshot, declaredSource);
        boolean typeChanged = !dialect.sameDataType(actualType,
                SchemaTypeMapping.preserveCharacterLengthUnit(dialect, actualType, dataType(target)));
        boolean nullableChanged = source.nullable() != target.nullable();
        boolean commentChanged = !Objects.equals(storageComment(source), storageComment(target));
        if (physical == null && (dialect.rewritesFullColumnDefinition() && (typeChanged || nullableChanged || commentChanged)
                || (typeChanged || nullableChanged) && requiresPhysicalCollation(target))) {
            throw new IllegalStateException("rewriting an existing column requires complete physical column metadata");
        }
        // 增量声明未指定 DEFAULT 时保留现值；完整变更集可以显式移除默认值。
        boolean preserveDefault = declaredSource
                ? SchemaDefinitionEquality.sameDefault(source.defaultValue(), target.defaultValue(), dialect)
                : target.defaultValue().kind() == ColumnDefault.Kind.NONE;
        ColumnDefault defaultValue = preserveDefault ? actual.defaultValue() : target.defaultValue();
        String defaultName = target.defaultConstraintName() == null
                ? actual.defaultConstraintName() : target.defaultConstraintName();
        if (!defaultValue.equals(target.defaultValue()) || !Objects.equals(defaultName, target.defaultConstraintName())) {
            target = target.toBuilder().defaultValue(defaultValue).defaultConstraintName(defaultName).build();
        }
        if (!typeChanged && !commentChanged
                && SchemaDefinitionEquality.sameColumnAttributes(actual, target, dialect, operation.relation())) {
            return null;
        }
        return SchemaOperation.of(operation.kind(), operation.relation(), target.name(), actual, target,
                operation.compatibility());
    }

    boolean changesType(ColumnDefinition source, ColumnDefinition target,
                        SchemaSnapshot snapshot, boolean declaredSource) {
        String actualType = actualType(source, snapshot, declaredSource);
        return !dialect.sameDataType(actualType,
                SchemaTypeMapping.preserveCharacterLengthUnit(dialect, actualType, dataType(target)));
    }

    String actualType(ColumnDefinition source, SchemaSnapshot snapshot, boolean declaredSource) {
        String actualType = declaredSource ? dataType(source) : metadataDataType(source);
        return snapshot == null ? actualType : SchemaTypeMapping.preserveCharacterLengthUnit(dialect,
                SchemaDefinitionEquality.actualColumnDdlType(dialect,
                        snapshot.knownDefinition().column(source.name()), snapshot.physicalColumnTypes()), actualType);
    }

    String defaultExpression(ColumnDefinition column) {
        ColumnDefault defaultValue = column.defaultValue();
        return switch (defaultValue.kind()) {
            case NONE -> column.defaultConstraintName() != null
                    && column.generation().strategy() == ValueGeneration.Strategy.NONE ? "null" : null;
            case LITERAL -> literal(defaultValue.value().orElseThrow());
            case NATIVE_EXPRESSION -> "(" + SchemaDefaultExpressions.normalize(
                    (String) defaultValue.value().orElseThrow(), dialect.generatedValueStyle().name()) + ")";
            case CURRENT_DATE -> switch (dialect.generatedValueStyle()) {
                case MYSQL -> "(current_date)";
                case SQL_SERVER -> "(cast(current_timestamp as date))";
                default -> "current_date";
            };
            case CURRENT_TIME -> switch (dialect.generatedValueStyle()) {
                case MYSQL -> "(current_time)";
                case SQL_SERVER -> "(cast(current_timestamp as time))";
                case ORACLE -> throw new UnsupportedOperationException(
                        "oracle cannot render a current-time default for text-backed TIME storage");
                default -> "current_time";
            };
            case CURRENT_TIMESTAMP -> {
                if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL) {
                    List<String> arguments = DatabaseType.of(dataType(column)).arguments();
                    if (!arguments.isEmpty() && !"0".equals(arguments.getFirst())) {
                        yield "current_timestamp(" + arguments.getFirst() + ')';
                    }
                }
                yield "current_timestamp";
            }
        };
    }
    private String primaryKey(PrimaryKeyDefinition primaryKey) {
        return namedColumns(primaryKey.name(), "primary key", primaryKey.columns());
    }
    String unique(UniqueConstraintDefinition unique) {
        if (!SchemaDefinitionEquality.ordinaryUnique(unique, dialect)) {
            throw new UnsupportedOperationException("explicit unique null semantics require a dedicated index");
        }
        String kind = unique.nullPolicy() == UniqueNullPolicy.DISTINCT
                && dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.H2
                ? "unique nulls distinct" : "unique";
        return namedColumns(unique.name(), kind, unique.columns());
    }

    SqlRequest createUnique(RelationIdentity relation, UniqueConstraintDefinition key) {
        if (SchemaDefinitionEquality.ordinaryUnique(key, dialect)) {
            return addConstraint(relation, unique(key));
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            // Oracle 的普通组合 UNIQUE 仍会仲裁部分 NULL；让任一 NULL 时所有索引键均为 NULL。
            // 固定 CASE 结构由完整 metadata reader 回读，不接受调用方的任意表达式。
            String nonNull = key.columns().stream().map(column -> dialect.identifier(column) + " is not null")
                    .collect(Collectors.joining(" and "));
            String keys = key.columns().stream().map(column -> "case when " + nonNull + " then "
                    + dialect.identifier(column) + " end").collect(Collectors.joining(", "));
            return new SqlRequest("create unique index " + dialect.schemaObjectIdentifier(relation, key.name())
                    + " on " + dialect.relationIdentifier(relation) + " (" + keys + ')', List.of());
        }
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.SQL_SERVER) {
            throw new UnsupportedOperationException("current dialect cannot render explicit null-distinct uniqueness");
        }
        String columns = key.columns().stream().map(dialect::identifier).collect(Collectors.joining(", "));
        // 只表达明确的空值唯一语义，不接受上层传入过滤 SQL，也不改变连接会话选项。
        String filter = key.columns().stream().map(column -> dialect.identifier(column) + " is not null")
                .collect(Collectors.joining(" and "));
        return new SqlRequest("create unique index " + dialect.identifier(key.name())
                + " on " + dialect.relationIdentifier(relation) + " (" + columns + ") where " + filter,
                List.of());
    }
    String check(CheckConstraintDefinition check) {
        return "constraint " + dialect.identifier(check.name())
                + " check (" + predicate(check.predicate()) + ')';
    }
    String foreignKey(ForeignKeyDefinition foreignKey) {
        RelationalTableDdlValidator.validate(foreignKey, dialect);
        String sql = namedColumns(foreignKey.name(), "foreign key", foreignKey.columns())
                + " references " + dialect.relationIdentifier(foreignKey.reference())
                + " (" + columns(foreignKey.referenceColumns()) + ')';
        if (foreignKey.onDelete() != ReferentialAction.NO_ACTION) {
            sql += " on delete " + action(foreignKey.onDelete());
        }
        if (foreignKey.onUpdate() != ReferentialAction.NO_ACTION) {
            sql += " on update " + action(foreignKey.onUpdate());
        }
        return sql;
    }
    private String namedColumns(String name, String kind, List<String> columns) {
        return "constraint " + dialect.identifier(name) + ' ' + kind + " (" + columns(columns) + ')';
    }
    private String columns(List<String> columns) {
        StringJoiner values = new StringJoiner(", ");
        columns.forEach(column -> values.add(dialect.identifier(column)));
        return values.toString();
    }
    private String predicate(CheckPredicate predicate) {
        return switch (predicate) {
            case CheckPredicate.Comparison comparison -> dialect.identifier(comparison.column()) + ' '
                    + comparisonOperator(comparison.operator()) + ' ' + literal(comparison.value());
            case CheckPredicate.Range range -> '(' + dialect.identifier(range.column()) + ' '
                    + (range.lowerInclusive() ? ">=" : ">") + ' ' + literal(range.lower())
                    + " and " + dialect.identifier(range.column()) + ' '
                    + (range.upperInclusive() ? "<=" : "<") + ' ' + literal(range.upper()) + ')';
            case CheckPredicate.In in -> dialect.identifier(in.column()) + " in ("
                    + in.values().stream().map(this::literal).collect(Collectors.joining(", ")) + ')';
            case CheckPredicate.NullCheck nullCheck -> dialect.identifier(nullCheck.column())
                    + (nullCheck.negated() ? " is not null" : " is null");
            case CheckPredicate.Logical logical -> logical.predicates().stream()
                    .map(this::predicate)
                    .collect(Collectors.joining(
                            " " + logical.operator().name().toLowerCase(java.util.Locale.ROOT) + " ", "(", ")"));
            case CheckPredicate.Negation negation -> "not (" + predicate(negation.predicate()) + ')';
        };
    }

    private String literal(Object value) {
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                && value instanceof String text && text.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Oracle cannot represent an empty string literal distinctly from null");
        }
        if (value instanceof Boolean flag) {
            return switch (dialect.generatedValueStyle()) {
                case ORACLE, SQL_SERVER -> flag ? "1" : "0";
                default -> flag.toString();
            };
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Long || value instanceof BigInteger) {
            return value.toString();
        }
        if (value instanceof Float number) {
            return new BigDecimal(number.toString()).toPlainString();
        }
        if (value instanceof Double number) {
            return BigDecimal.valueOf(number).toPlainString();
        }
        if (value instanceof BigDecimal number) {
            return number.toPlainString();
        }
        if (value instanceof Enum<?> enumeration) {
            return dialect.valueLiteral(enumeration.name());
        }
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE) {
            // 普通字符串到 Oracle DATE/TIMESTAMP 的隐式转换依赖 NLS；保留受控值的时间类型。
            return switch (value) {
                case LocalDate date -> "date " + dialect.valueLiteral(date.toString());
                case LocalDateTime timestamp -> oracleTimestampLiteral(timestamp, null);
                case OffsetDateTime timestamp -> oracleTimestampLiteral(
                        timestamp.toLocalDateTime(), timestamp.getOffset());
                case Instant instant -> oracleTimestampLiteral(
                        LocalDateTime.ofInstant(instant, ZoneOffset.UTC), ZoneOffset.UTC);
                default -> dialect.valueLiteral(value.toString());
            };
        }
        return dialect.valueLiteral(value.toString());
    }
    private String oracleTimestampLiteral(LocalDateTime timestamp, ZoneOffset offset) {
        String value = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(timestamp).replace('T', ' ');
        if (offset != null) {
            value += " " + (offset.equals(ZoneOffset.UTC) ? "+00:00" : offset.getId());
        }
        return "timestamp " + dialect.valueLiteral(value);
    }

    private static String comparisonOperator(CheckPredicate.ComparisonOperator operator) {
        return switch (operator) {
            case EQUAL -> "=";
            case NOT_EQUAL -> "<>";
            case LESS_THAN -> "<";
            case LESS_THAN_OR_EQUAL -> "<=";
            case GREATER_THAN -> ">";
            case GREATER_THAN_OR_EQUAL -> ">=";
        };
    }

    private static String action(ReferentialAction action) {
        return action.name().toLowerCase(java.util.Locale.ROOT).replace('_', ' ');
    }
    static UnsupportedOperationException unsupported(SchemaOperation.Kind kind) {
        return new UnsupportedOperationException("current dialect cannot render schema operation: " + kind);
    }

    /** 校验建表语法，并选出必须与自增列同时创建的已有索引；不擅自增加索引。 */
    private IndexDefinition prepareTable(RelationalTableDefinition table) {
        RelationalTableDdlValidator.validate(table, dialect);
        List<ColumnDefinition> identities = table.columns().stream()
                .filter(column -> column.generation().strategy() == ValueGeneration.Strategy.IDENTITY)
                .toList();
        if (identities.size() > 1
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.H2
                && dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            throw new UnsupportedOperationException("a table cannot contain more than one identity column");
        }
        if (identities.isEmpty() || dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.MYSQL) {
            return null;
        }
        String identity = identities.getFirst().name();
        if (table.primaryKey().filter(key -> key.columns().getFirst().equals(identity)).isPresent()
                || table.uniqueConstraints().stream().anyMatch(key -> key.columns().getFirst().equals(identity))) {
            return null;
        }
        return table.indexes().stream().filter(index -> index.keys().getFirst().column().equals(identity))
                .findFirst().orElseThrow(() -> new UnsupportedOperationException(
                        "mysql identity column must be the first column of an index"));
    }
}
