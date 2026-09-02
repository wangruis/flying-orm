package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.type.DatabaseTypes;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * PostgreSQL 的动态表单元数据读取器，读取 information_schema，并用 pg_catalog 补充列注释和数据库特有信息。
 * 所有 schema/table 条件都使用参数绑定，调用方名称不会直接拼进字典 SQL。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务不直接依赖系统表查询细节。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class PostgreSqlReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String BASE_COLUMNS_SQL = """
            select c.column_name as COLUMN_NAME,
                   current_schema() as RESOLUTION_SCHEMA,
                   case when c.data_type = 'ARRAY' or c.data_type in ('bit', 'bit varying')
                        then pg_catalog.format_type(column_attribute.atttypid,
                                                    column_attribute.atttypmod)
                        else c.data_type end as DATA_TYPE,
                   c.character_maximum_length as CHARACTER_MAXIMUM_LENGTH,
                   c.numeric_precision as NUMERIC_PRECISION,
                   c.numeric_scale as NUMERIC_SCALE,
                   c.datetime_precision as TEMPORAL_PRECISION,
                   pg_catalog.col_description((pg_catalog.quote_ident(c.table_schema) || '.' ||
                           pg_catalog.quote_ident(c.table_name))::regclass::oid, c.ordinal_position) as REMARKS,
                   c.is_nullable as NULLABLE,
                   c.column_default as GENERATION_EXPRESSION,
                   c.is_identity as IS_IDENTITY,
                   case when pk.constraint_name is null then false else true end as PRIMARY_KEY
            from information_schema.columns c
            join pg_catalog.pg_namespace column_schema
              on column_schema.nspname = c.table_schema
            join pg_catalog.pg_class column_table
              on column_table.relnamespace = column_schema.oid
             and column_table.relname = c.table_name
            join pg_catalog.pg_attribute column_attribute
              on column_attribute.attrelid = column_table.oid
             and column_attribute.attnum = c.ordinal_position
            left join (
                select pk_kcu.table_schema,
                       pk_kcu.table_name,
                       pk_kcu.column_name,
                       pk_kcu.constraint_name
                from information_schema.key_column_usage pk_kcu
                join information_schema.table_constraints pk_tc
                  on pk_tc.constraint_schema = pk_kcu.constraint_schema
                 and pk_tc.table_schema = pk_kcu.table_schema
                 and pk_tc.table_name = pk_kcu.table_name
                 and pk_tc.constraint_name = pk_kcu.constraint_name
                 and pk_tc.constraint_type = 'PRIMARY KEY'
            ) pk
              on pk.table_schema = c.table_schema
             and pk.table_name = c.table_name
             and pk.column_name = c.column_name
            where c.table_name = ?
            """;

    private static final String BASE_INDEXES_SQL = """
            select ci.relname as INDEX_NAME,
                   a.attname as COLUMN_NAME,
                   ix.indisunique as UNIQUE_INDEX,
                   (ix.indisvalid and ix.indisready and ix.indislive
                       and ix.indpred is null and ix.indexprs is null
                       and ord.position < ix.indnkeyatts
                       and am.amname = 'btree'
                       and opc.opcdefault
                       and ix.indoption[ord.position] = 0
                       and ix.indcollation[ord.position] = a.attcollation
                       and not coalesce((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false))
                       as INDEX_REPRESENTABLE,
                   case
                       when not ix.indislive then 'index is being dropped'
                       when not ix.indisready then 'index is not ready for writes'
                       when not ix.indisvalid then 'index is invalid'
                       when ix.indpred is not null then 'partial predicate'
                       when ix.indexprs is not null then 'expression key'
                       when ord.position >= ix.indnkeyatts then 'included column'
                       when am.amname <> 'btree' then 'non-btree access method'
                       when not opc.opcdefault then 'non-default operator class'
                       when ix.indoption[ord.position] <> 0 then 'non-default sort or null ordering'
                       when ix.indcollation[ord.position] <> a.attcollation then 'non-default collation'
                       when coalesce((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false)
                           then 'nulls-not-distinct uniqueness'
                       else null
                   end as UNSUPPORTED_INDEX_REASON
            from pg_catalog.pg_class t
            join pg_catalog.pg_namespace n
              on n.oid = t.relnamespace
            join pg_catalog.pg_index ix
              on ix.indrelid = t.oid
            join pg_catalog.pg_class ci
              on ci.oid = ix.indexrelid
            join pg_catalog.pg_am am
              on am.oid = ci.relam
            join pg_catalog.generate_subscripts(ix.indkey, 1) as ord(position)
              on true
            left join pg_catalog.pg_attribute a
              on a.attrelid = t.oid
             and a.attnum = ix.indkey[ord.position]
            left join pg_catalog.pg_opclass opc
              on opc.oid = ix.indclass[ord.position]
            where t.relname = ?
              and not ix.indisprimary
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select n.nspname as TABLE_SCHEMA,
                   con.conname as FOREIGN_KEY_NAME,
                   a.attname as COLUMN_NAME,
                   rn.nspname as REFERENCED_TABLE_SCHEMA,
                   rt.relname as REFERENCED_TABLE_NAME,
                   ra.attname as REFERENCED_COLUMN_NAME
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class t
              on t.oid = con.conrelid
            join pg_catalog.pg_namespace n
              on n.oid = t.relnamespace
            join pg_catalog.pg_class rt
              on rt.oid = con.confrelid
            join pg_catalog.pg_namespace rn
              on rn.oid = rt.relnamespace
            join pg_catalog.generate_subscripts(con.conkey, 1) as ord(position)
              on true
            join pg_catalog.pg_attribute a
              on a.attrelid = t.oid
             and a.attnum = con.conkey[ord.position]
            join pg_catalog.pg_attribute ra
              on ra.attrelid = rt.oid
             and ra.attnum = con.confkey[ord.position]
            where con.contype = 'f'
              and t.relname = ?
            """;

    private final InformationSchemaFormMetadataReader delegate;

    private PostgreSqlReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                       "reactive sql executor must not be null"),
                                                                queries());
    }

    static PostgreSqlReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new PostgreSqlReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return new InformationSchemaFormMetadataReader.Queries(PostgreSqlReactiveFormMetadataReader::columnQuery,
                                                               PostgreSqlReactiveFormMetadataReader::indexQuery,
                                                               PostgreSqlReactiveFormMetadataReader::foreignKeyQuery,
                                                               PostgreSqlReactiveFormMetadataReader::logicalType);
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
            String sql = BASE_COLUMNS_SQL + """
                     and exists (
                         select 1
                         from pg_catalog.pg_class visible_table
                         join pg_catalog.pg_namespace visible_schema
                           on visible_schema.oid = visible_table.relnamespace
                         where visible_schema.nspname = c.table_schema
                           and visible_table.relname = c.table_name
                           and pg_catalog.pg_table_is_visible(visible_table.oid)
                     )
                     order by c.ordinal_position
                    """;
            return new SqlRequest(sql, List.of(safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.table_schema = ? order by c.ordinal_position";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL
                    + " and pg_catalog.pg_table_is_visible(t.oid) order by ci.relname, ord.position";
            return new SqlRequest(sql, List.of(safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and n.nspname = ? order by ci.relname, ord.position";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and pg_catalog.pg_table_is_visible(t.oid) order by con.conname, ord.position";
            return new SqlRequest(sql, List.of(safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL + " and n.nspname = ? order by con.conname, ord.position";
        return new SqlRequest(sql, List.of(safeTable, schema.trim()));
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "postgresql");
    }
}
