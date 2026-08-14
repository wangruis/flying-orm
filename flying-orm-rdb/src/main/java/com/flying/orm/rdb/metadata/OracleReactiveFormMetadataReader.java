package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Oracle 的动态表单元数据读取器，读取数据字典中的普通表字段、主键、索引、外键和列注释。
 * 数据字典中的物理 owner、表名按精确大小写匹配；Oracle 类型等差异留在查询模板和类型映射中。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务不直接依赖数据字典 SQL。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class OracleReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.DATA_TYPE,
                   c.CHAR_LENGTH as CHARACTER_MAXIMUM_LENGTH,
                   c.DATA_PRECISION as NUMERIC_PRECISION,
                   c.DATA_SCALE as NUMERIC_SCALE,
                   cc.COMMENTS as REMARKS,
                   c.NULLABLE,
                   case when pk.CONSTRAINT_NAME is null then 'false' else 'true' end as PRIMARY_KEY
            from ALL_TAB_COLUMNS c
            left join ALL_COL_COMMENTS cc
                   on cc.OWNER = c.OWNER
                  and cc.TABLE_NAME = c.TABLE_NAME
                  and cc.COLUMN_NAME = c.COLUMN_NAME
            left join (
                select acc.OWNER, acc.TABLE_NAME, acc.COLUMN_NAME, ac.CONSTRAINT_NAME
                from ALL_CONS_COLUMNS acc
                join ALL_CONSTRAINTS ac
                  on ac.OWNER = acc.OWNER
                 and ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                 and ac.CONSTRAINT_TYPE = 'P'
            ) pk
                   on pk.OWNER = c.OWNER
                  and pk.TABLE_NAME = c.TABLE_NAME
                  and pk.COLUMN_NAME = c.COLUMN_NAME
            where c.TABLE_NAME = case
                when exists (
                    select 1
                    from ALL_TAB_COLUMNS exact_column
                    where exact_column.OWNER = c.OWNER
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
            """;

    private static final String BASE_INDEXES_SQL = """
            select i.INDEX_NAME,
                   ic.COLUMN_NAME,
                   case when i.UNIQUENESS = 'UNIQUE' then 'true' else 'false' end as UNIQUE_INDEX
            from ALL_INDEXES i
            join ALL_IND_COLUMNS ic
              on ic.INDEX_OWNER = i.OWNER
             and ic.INDEX_NAME = i.INDEX_NAME
             and ic.TABLE_OWNER = i.TABLE_OWNER
             and ic.TABLE_NAME = i.TABLE_NAME
            where i.TABLE_NAME = case
                when exists (
                    select 1
                    from ALL_TAB_COLUMNS exact_column
                    where exact_column.OWNER = i.TABLE_OWNER
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
              and not exists (
                  select 1
                  from ALL_CONSTRAINTS ac
                  where ac.OWNER = i.TABLE_OWNER
                    and ac.TABLE_NAME = i.TABLE_NAME
                    and ac.INDEX_NAME = i.INDEX_NAME
                    and ac.CONSTRAINT_TYPE = 'P'
              )
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select ac.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   acc.COLUMN_NAME,
                   rc.TABLE_NAME as REFERENCED_TABLE_NAME,
                   rcc.COLUMN_NAME as REFERENCED_COLUMN_NAME
            from ALL_CONSTRAINTS ac
            join ALL_CONS_COLUMNS acc
              on acc.OWNER = ac.OWNER
             and acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
            join ALL_CONSTRAINTS rc
              on rc.OWNER = ac.R_OWNER
             and rc.CONSTRAINT_NAME = ac.R_CONSTRAINT_NAME
            join ALL_CONS_COLUMNS rcc
              on rcc.OWNER = rc.OWNER
             and rcc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
             and rcc.POSITION = acc.POSITION
            where ac.CONSTRAINT_TYPE = 'R'
              and ac.TABLE_NAME = case
                  when exists (
                      select 1
                      from ALL_TAB_COLUMNS exact_column
                      where exact_column.OWNER = ac.OWNER
                        and exact_column.TABLE_NAME = ?
                  ) then ? else upper(?) end
            """;

    private final InformationSchemaFormMetadataReader delegate;

    private OracleReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                       "reactive sql executor must not be null"),
                                                                                       queries());
    }

    static OracleReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new OracleReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return new InformationSchemaFormMetadataReader.Queries(OracleReactiveFormMetadataReader::columnQuery,
                                                               OracleReactiveFormMetadataReader::indexQuery,
                                                               OracleReactiveFormMetadataReader::foreignKeyQuery,
                                                               OracleReactiveFormMetadataReader::logicalType);
    }

    @Override
    public ReactiveSqlExecutor metadataExecutor() {
        return delegate.metadataExecutor();
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String table) {
        return delegate.readForm(formId, table);
    }

    @Override
    public Mono<DynamicForm> readForm(String formId, String schema, String table) {
        return delegate.readForm(formId, schema, table);
    }

    @Override
    public Mono<TableMetadata> readTable(String table) {
        return delegate.readTable(table);
    }

    @Override
    public Mono<TableMetadata> readTable(String schema, String table) {
        return delegate.readTable(schema, table);
    }

    private static SqlRequest columnQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_COLUMNS_SQL
                    + " and c.OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')"
                    + " order by c.COLUMN_ID";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.OWNER = " + ownerExpression() + " order by c.COLUMN_ID";
        return new SqlRequest(sql, tableAndOwnerParameters(safeTable, schema));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL
                    + " and i.TABLE_OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')"
                    + " order by i.INDEX_NAME, ic.COLUMN_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_INDEXES_SQL
                + " and i.TABLE_OWNER = " + ownerExpression()
                + " order by i.INDEX_NAME, ic.COLUMN_POSITION";
        return new SqlRequest(sql, tableAndOwnerParameters(safeTable, schema));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and ac.OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')"
                    + " order by ac.CONSTRAINT_NAME, acc.POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL
                + " and ac.OWNER = " + ownerExpression()
                + " order by ac.CONSTRAINT_NAME, acc.POSITION";
        return new SqlRequest(sql, tableAndOwnerParameters(safeTable, schema));
    }

    /** 精确 quoted owner 存在时优先使用；否则按 Oracle 未加引号标识符规则折叠为大写。 */
    private static String ownerExpression() {
        return "case when exists (select 1 from ALL_USERS exact_owner where exact_owner.USERNAME = ?) "
                + "then ? else upper(?) end";
    }

    private static List<Object> tableParameters(String table) {
        return List.of(table, table, table);
    }

    private static List<Object> tableAndOwnerParameters(String table, String owner) {
        String safeOwner = owner.trim();
        return List.of(table, table, table, safeOwner, safeOwner, safeOwner);
    }

    private static String logicalType(String dataType) {
        String type = InformationSchemaFormMetadataReader.requireText(dataType, "data type").toUpperCase(Locale.ROOT);
        return switch (type) {
            case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR" -> "VARCHAR";
            case "CLOB", "NCLOB", "LONG" -> "TEXT";
            case "BLOB", "RAW", "LONG RAW" -> "BLOB";
            case "NUMBER", "DECIMAL", "NUMERIC" -> "DECIMAL";
            case "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE" -> "DECIMAL";
            case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "DATE" -> "TIMESTAMP";
            case "INTERVAL DAY TO SECOND", "INTERVAL YEAR TO MONTH" -> "VARCHAR";
            default -> type;
        };
    }
}
