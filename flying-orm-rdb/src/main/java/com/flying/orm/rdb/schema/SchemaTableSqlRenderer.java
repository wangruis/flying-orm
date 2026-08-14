package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.sql.render.SqlRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 负责把表、字段和索引变成 DDL 片段。
 *
 * <p>这里不比较新旧表结构，也不决定哪些迁移可以执行。这样表结构拼装和迁移判断各自只做一件事，
 * 以后增加字段类型或索引写法时，不会把迁移计划代码一起搅乱。</p>
 */
final class SchemaTableSqlRenderer {

    private final SchemaDialect dialect;

    SchemaTableSqlRenderer(SchemaDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
    }

    List<SqlRequest> createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        String columns = safeForm.fields().stream()
                                 .map(this::columnDefinition)
                                 .collect(Collectors.joining(", "));
        List<SqlRequest> requests = new ArrayList<>();
        addSequenceCreates(requests, safeForm.fields());
        requests.add(new SqlRequest("create table " + identifier(safeForm.table()) + " (" + columns + ")",
                                    List.of()));
        for (DynamicField field : safeForm.fields()) {
            addColumnComment(requests, safeForm.table(), field);
        }
        return List.copyOf(requests);
    }

    List<SqlRequest> createIndexes(String table, List<IndexMetadata> indexes) {
        String safeTable = identifier(Objects.requireNonNull(table, "table must not be null"));
        return Objects.requireNonNull(indexes, "indexes must not be null")
                      .stream()
                      .map(index -> createIndex(safeTable, index))
                      .toList();
    }

    /** 为 ORM 自有侧表创建经过结构化元数据约束的级联删除外键。 */
    SqlRequest createCascadeForeignKey(String table, ForeignKeyMetadata foreignKey) {
        ForeignKeyMetadata safe = Objects.requireNonNull(foreignKey, "foreign key metadata must not be null");
        String columns = safe.columns().stream().map(this::identifier).collect(Collectors.joining(", "));
        String references = safe.referenceColumns().stream()
                                .map(this::identifier)
                                .collect(Collectors.joining(", "));
        return new SqlRequest("alter table " + identifier(table)
                                      + " add constraint " + identifier(safe.name())
                                      + " foreign key (" + columns + ") references "
                                      + identifier(safe.referenceTable()) + " (" + references + ")"
                                      + " on delete cascade",
                              List.of());
    }

    String columnDefinition(DynamicField field) {
        DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
        String type = dataType(safeField);
        String sql = identifier(safeField.name()) + " " + type;
        sql += dialect.generatedValueClause(safeField.generation(), type);
        if (safeField.primaryKey()) {
            sql += " primary key";
        } else if (!safeField.nullable()) {
            sql += " not null";
        }
        if (dialect.inlineColumnComment() && safeField.comment() != null) {
            sql += " comment " + dialect.quoteLiteral(safeField.comment());
        }
        return sql;
    }

    String identifier(String value) {
        return dialect.identifier(value);
    }

    String dataType(DynamicField field) {
        DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
        return dialect.dataType(safeField.dataType(),
                                safeField.length(),
                                safeField.precision(),
                                safeField.scale());
    }

    /** 元数据把类型参数拆成独立属性；比较迁移结构时仍须用与目标字段相同的方言渲染规则还原。 */
    String dataType(ColumnMetadata column) {
        ColumnMetadata safeColumn = Objects.requireNonNull(column, "column metadata must not be null");
        return dialect.dataType(safeColumn.dataType(),
                                safeColumn.length(),
                                safeColumn.precision(),
                                safeColumn.scale());
    }

    void addSequenceCreates(List<SqlRequest> requests, List<DynamicField> fields) {
        Map<String, String> sequences = new LinkedHashMap<>();
        for (DynamicField field : fields) {
            dialect.createSequenceSql(field.generation(), dataType(field)).ifPresent(sql -> {
                String key = field.generation().sequenceName().toLowerCase(Locale.ROOT);
                String previous = sequences.putIfAbsent(key, sql);
                if (previous != null && !previous.equals(sql)) {
                    throw new IllegalArgumentException("sequence is declared with different options: "
                                                               + field.generation().sequenceName());
                }
            });
        }
        sequences.values().forEach(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    void addSequenceCreate(List<SqlRequest> requests, DynamicField field) {
        dialect.createSequenceSql(field.generation(), dataType(field))
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }

    SqlRequest createIndex(String table, IndexMetadata index) {
        IndexMetadata safeIndex = Objects.requireNonNull(index, "index metadata must not be null");
        if (safeIndex.columns().isEmpty()) {
            throw new IllegalArgumentException("index columns must not be empty: " + safeIndex.name());
        }
        String unique = safeIndex.unique() ? "unique " : "";
        String columns = safeIndex.columns().stream()
                                  .map(this::identifier)
                                  .collect(Collectors.joining(", "));
        return new SqlRequest("create " + unique + "index " + identifier(safeIndex.name())
                                      + " on " + table + " (" + columns + ")",
                              List.of());
    }

    SqlRequest dropIndex(String table, IndexMetadata index) {
        IndexMetadata safeIndex = Objects.requireNonNull(index, "index metadata must not be null");
        return new SqlRequest(dialect.dropIndexSql(table, safeIndex.name()), List.of());
    }

    void addMissingComment(List<SqlRequest> requests,
                           String table,
                           com.flying.orm.core.metadata.ColumnMetadata column,
                           DynamicField target) {
        if (target.comment() != null && !target.comment().equals(column.comment())) {
            addColumnComment(requests, table, target);
        }
    }

    void addColumnComment(List<SqlRequest> requests, String table, DynamicField field) {
        dialect.columnCommentSql(table, field.name(), field.comment())
               .ifPresent(sql -> requests.add(new SqlRequest(sql, List.of())));
    }
}
