package com.flying.orm.testkit.dialect;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 真实库元数据兼容链路：建父表、建子表、建索引、建外键，然后用生产 reader 读回来。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class ReactiveDialectMetadataScenario {

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Z][A-Z0-9_]{0,62}");

    private final String childTableName;

    private final String parentTableName;

    private ReactiveDialectMetadataScenario(String childTableName) {
        this.childTableName = validateTableName(childTableName);
        this.parentTableName = parentName(this.childTableName);
    }

    public static ReactiveDialectMetadataScenario forTable(String childTableName) {
        return new ReactiveDialectMetadataScenario(childTableName);
    }

    public String parentTableName() {
        return parentTableName;
    }

    public Mono<ReactiveDialectMetadataResult> run(ReactiveSqlExecutor executor, RdbDialect dialect) {
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        ReactiveFormMetadataReader metadataReader = ReactiveFormMetadataReaders.create(safeExecutor, safeDialect);

        return createSchema(safeExecutor, safeDialect)
                .then(metadataReader.readTable(childTableName))
                .map(this::toResult);
    }

    private Mono<Void> createSchema(ReactiveSqlExecutor executor, RdbDialect dialect) {
        List<SqlRequest> requests = createRequests(dialect);
        return Flux.fromIterable(requests)
                   .concatMap(executor::rowsUpdated)
                   .then();
    }

    private List<SqlRequest> createRequests(RdbDialect dialect) {
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        List<SqlRequest> requests = new ArrayList<>();
        requests.addAll(renderer.createTable(parentForm()));
        requests.addAll(renderer.createTable(childForm()));
        requests.addAll(renderer.createIndexes(childTableName, List.of(nameIndex())));
        requests.add(foreignKeySql(dialect));
        return List.copyOf(requests);
    }

    private ReactiveDialectMetadataResult toResult(TableMetadata table) {
        return new ReactiveDialectMetadataResult(table.name(),
                                                 table.primaryKeyColumns()
                                                      .stream()
                                                      .map(ColumnMetadata::name)
                                                      .toList(),
                                                 table.index(indexName()).columns(),
                                                 table.foreignKey(foreignKeyName()).columns(),
                                                 table.foreignKey(foreignKeyName()).referenceTable());
    }

    private DynamicForm parentForm() {
        return DynamicForm.builder("metadataParent", parentTableName)
                          .addField(DynamicField.primaryKey("ID", "VARCHAR"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .build();
    }

    private DynamicForm childForm() {
        return DynamicForm.builder("metadataChild", childTableName)
                          .addField(DynamicField.primaryKey("ID", "VARCHAR"))
                          .addField(DynamicField.of("NAME", "VARCHAR"))
                          .addField(DynamicField.of("PARENT_ID", "VARCHAR"))
                          .build();
    }

    private IndexMetadata nameIndex() {
        return IndexMetadata.builder(indexName())
                            .unique()
                            .addColumn("NAME")
                            .build();
    }

    private SqlRequest foreignKeySql(RdbDialect dialect) {
        String name = dialect.name().toLowerCase(Locale.ROOT);
        String sql;
        if ("mysql".equals(name)) {
            sql = "alter table `" + childTableName + "` add constraint `" + foreignKeyName()
                    + "` foreign key (`PARENT_ID`) references `" + parentTableName + "` (`ID`)";
        } else if ("postgresql".equals(name) || "oracle".equals(name) || "sqlserver".equals(name)) {
            // 这三种数据库都接受双引号标识符。测试表名固定为大写，Oracle 不会发生大小写折叠歧义，
            // SQL Server 也能在默认 QUOTED_IDENTIFIER 配置下原样创建约束。
            sql = "alter table \"" + childTableName + "\" add constraint \"" + foreignKeyName()
                    + "\" foreign key (\"PARENT_ID\") references \"" + parentTableName + "\" (\"ID\")";
        } else {
            throw new IllegalArgumentException("metadata scenario does not support dialect: " + dialect.name());
        }
        return SqlRequest.nativeSql(sql, List.of());
    }

    private String indexName() {
        return "UK_" + childTableName + "_NAME";
    }

    private String foreignKeyName() {
        return "FK_" + childTableName + "_PARENT";
    }

    private static String parentName(String childTableName) {
        String suffix = "_CHILD";
        if (childTableName.endsWith(suffix)) {
            return childTableName.substring(0, childTableName.length() - suffix.length()) + "_PARENT";
        }
        return childTableName + "_PARENT";
    }

    private static String validateTableName(String tableName) {
        String safeTableName = Objects.requireNonNull(tableName, "table name must not be null");
        if (!SAFE_TABLE_NAME.matcher(safeTableName).matches()) {
            throw new IllegalArgumentException("table name must start with A-Z and contain only A-Z, 0-9 or underscore");
        }
        return safeTableName;
    }
}
