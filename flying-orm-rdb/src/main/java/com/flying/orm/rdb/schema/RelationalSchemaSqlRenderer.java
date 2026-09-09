package com.flying.orm.rdb.schema;

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
import com.flying.orm.core.type.DatabaseType;

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
import java.util.stream.Collectors;

/**
 * 把规范关系 operation 编译成确定顺序、零业务参数的 DDL 请求。
 *
 * <p>本类只消费已经校验过的关系对象，不读取数据库，也不从说明文字反向解析 SQL。当前方言不能
 * 无损表达的操作会明确抛出 {@link UnsupportedOperationException}，由计划审阅器保留为人工步骤。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationalSchemaSqlRenderer {

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

    List<SqlRequest> render(SchemaOperation operation, Map<String, String> existingSequences) {
        return render(operation, existingSequences, true);
    }

    List<SqlRequest> render(SchemaOperation operation,
                           Map<String, String> existingSequences,
                           boolean physicalActualTypes) {
        return render(operation, existingSequences, physicalActualTypes, null);
    }

    List<SqlRequest> render(SchemaOperation operation,
                           Map<String, String> existingSequences,
                           boolean physicalActualTypes,
                           DatabaseType observedActualType) {
        SchemaOperation source = Objects.requireNonNull(operation, "schema operation must not be null");
        Map<String, String> createdSequences = new LinkedHashMap<>();
        List<SqlRequest> requests = switch (source.kind()) {
            case CREATE_TABLE -> createTable((RelationalTableDefinition) source.desired(),
                                            existingSequences, createdSequences);
            case ADD_COLUMN -> addColumn(source.relation(), (ColumnDefinition) source.desired(),
                                         existingSequences, createdSequences);
            case CHANGE_COLUMN -> evolution.changeColumn(source, physicalActualTypes, observedActualType);
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
        // 不支持的操作不能占用尚未进入可执行计划的序列。
        existingSequences.putAll(createdSequences);
        return requests;
    }
    Map<String, String> sequenceDefinitions(List<SchemaSnapshot> snapshots) {
        return SchemaSequenceSqlRenderer.definitions(dialect, snapshots, this::dataType);
    }

    private List<SqlRequest> createTable(RelationalTableDefinition table,
                                        Map<String, String> existingSequences, Map<String, String> createdSequences) {
        validateTableOptions(table);
        List<SqlRequest> requests = new ArrayList<>();
        for (ColumnDefinition column : table.columns()) {
            SchemaSequenceSqlRenderer.addCreate(
                    dialect, requests, column, existingSequences, createdSequences, this::dataType);
        }

        StringJoiner definitions = new StringJoiner(", ");
        table.columns().stream().map(this::columnDefinition).forEach(definitions::add);
        table.primaryKey().map(this::primaryKey).ifPresent(definitions::add);
        table.uniqueConstraints().stream()
                .filter(key -> SchemaDefinitionEquality.ordinaryUnique(key, dialect))
                .map(this::unique).forEach(definitions::add);
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
        table.indexes().stream().map(index -> createIndex(table.identity(), index)).forEach(requests::add);
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
                                      Map<String, String> existingSequences, Map<String, String> createdSequences) {
        List<SqlRequest> requests = new ArrayList<>();
        SchemaSequenceSqlRenderer.addCreate(
                dialect, requests, column, existingSequences, createdSequences, this::dataType);
        requests.add(new SqlRequest(dialect.addColumnSql(relation, columnDefinition(column)), List.of()));
        dialect.columnCommentSql(relation, column.name(), storageComment(column))
                .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
        return List.copyOf(requests);
    }
    SqlRequest addConstraint(RelationIdentity relation, String definition) {
        return new SqlRequest("alter table " + dialect.relationIdentifier(relation) + " add " + definition, List.of());
    }
    SqlRequest createIndex(RelationIdentity relation, IndexDefinition index) {
        String indexName = dialect.identifier(index.name());
        if (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.ORACLE
                && relation.schema().isPresent()) {
            indexName = dialect.schemaObjectIdentifier(relation, index.name());
        }
        StringJoiner keys = new StringJoiner(", ");
        index.keys().forEach(key -> keys.add(
                dialect.identifier(key.column()) + " "
                        + key.direction().name().toLowerCase(java.util.Locale.ROOT)));
        String unique = index.unique() ? "unique " : "";
        return new SqlRequest("create " + unique + "index " + indexName
                                      + " on " + dialect.relationIdentifier(relation) + " (" + keys + ")",
                              List.of());
    }

    String columnDefinition(ColumnDefinition column) {
        return columnDefinition(column, column.charset(), column.collation());
    }

    String columnDefinition(ColumnDefinition column, String charset, String collation) {
        String type = dataType(column);
        StringBuilder sql = new StringBuilder(dialect.identifier(column.name()))
                .append(' ').append(type);
        // 字符集和排序规则属于列类型；尤其 MySQL 的 CHARACTER SET 不能放到 NOT NULL 后面。
        appendStorage(sql, charset, collation);
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
        if (!column.nullable()) {
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
        String storageComment = storageComment(column);
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
        Integer precision = column.databaseType().isTemporal()
                ? column.temporalPrecision() : column.precision();
        return dialect.dataType(column.databaseType().declaration(), column.length(), precision, column.scale());
    }

    String defaultExpression(ColumnDefinition column) {
        ColumnDefault defaultValue = column.defaultValue();
        return switch (defaultValue.kind()) {
            case NONE -> column.defaultConstraintName() != null
                    && column.generation().strategy() == ValueGeneration.Strategy.NONE ? "null" : null;
            case LITERAL -> literal(defaultValue.value().orElseThrow());
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
            case CURRENT_TIMESTAMP -> "current_timestamp"
                    + (dialect.generatedValueStyle() == SchemaDialect.GeneratedValueStyle.MYSQL
                    && column.temporalPrecision() != null && column.temporalPrecision() > 0
                    ? "(" + column.temporalPrecision() + ')' : "");
        };
    }
    private void appendStorage(StringBuilder sql, String charset, String collation) {
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
        return new UnsupportedOperationException("relational schema operation requires manual SQL: " + kind);
    }

    private void validateTableOptions(RelationalTableDefinition table) {
        RelationalTableDdlValidator.validate(table, dialect);
        List<ColumnDefinition> identities = table.columns().stream()
                .filter(column -> column.generation().strategy() == ValueGeneration.Strategy.IDENTITY)
                .toList();
        if (identities.size() > 1) {
            throw new UnsupportedOperationException("a table cannot contain more than one identity column");
        }
        if (identities.isEmpty() || dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.MYSQL) {
            return;
        }
        // MySQL 的 AUTO_INCREMENT 必须落在索引首列。当前关系模型以主键表达这项稳定约束，
        // 不在渲染器里猜测普通索引能否承担生成语义，避免产出依赖数据库细节的脆弱 DDL。
        if (table.primaryKey().isEmpty()) {
            throw new UnsupportedOperationException("mysql identity column must be the table primary key");
        }
        String identity = identities.getFirst().name();
        List<String> primaryKeyColumns = table.primaryKey().orElseThrow().columns();
        if (!primaryKeyColumns.contains(identity)) {
            throw new UnsupportedOperationException("mysql identity column must be the table primary key");
        }
        if (!primaryKeyColumns.getFirst().equals(identity)) {
            throw new UnsupportedOperationException("mysql identity column must be the first primary-key column");
        }
    }
}
