package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import com.flying.orm.rdb.schema.SchemaSnapshotCoverage;
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
                   c.table_schema as RESOLUTION_SCHEMA,
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
                   case when c.is_generated = 'NEVER'
                                  and (c.is_identity = 'NO' or c.identity_generation = 'BY DEFAULT')
                                  and not (c.is_identity = 'NO' and identity_sequence.oid is not null)
                                  and (c.is_identity = 'NO'
                                           and lower(coalesce(c.column_default, '')) not like 'nextval(%'
                                       or generation_sequence.seqrelid is not null)
                                  and coalesce(generation_sequence.seqcycle, false) = false
                                  and (generation_sequence.seqrelid is null or (
                                      generation_sequence.seqtypid = case when c.is_identity = 'YES'
                                          then column_attribute.atttypid
                                          else 'pg_catalog.int8'::pg_catalog.regtype end
                                      and generation_sequence.seqmin = case
                                          when generation_sequence.seqincrement > 0 then 1
                                          when generation_sequence.seqtypid = 'pg_catalog.int2'::pg_catalog.regtype
                                              then -32768
                                          when generation_sequence.seqtypid = 'pg_catalog.int4'::pg_catalog.regtype
                                              then -2147483648
                                          else -9223372036854775808 end
                                      and generation_sequence.seqmax = case
                                          when generation_sequence.seqincrement < 0 then -1
                                          when generation_sequence.seqtypid = 'pg_catalog.int2'::pg_catalog.regtype
                                              then 32767
                                          when generation_sequence.seqtypid = 'pg_catalog.int4'::pg_catalog.regtype
                                              then 2147483647
                                          else 9223372036854775807 end
                                  ))
                        then true else false end as COLUMN_REPRESENTABLE,
                   case
                       when c.is_generated <> 'NEVER' then 'generated expression'
                       when c.is_identity = 'YES' and c.identity_generation <> 'BY DEFAULT'
                           then 'identity generation mode'
                       when c.is_identity = 'NO' and identity_sequence.oid is not null
                           then 'owned serial sequence'
                       when (c.is_identity = 'YES'
                                or lower(coalesce(c.column_default, '')) like 'nextval(%')
                                and generation_sequence.seqrelid is null
                           then 'generation sequence metadata'
                       when coalesce(generation_sequence.seqcycle, false)
                           then 'cycling sequence'
                       when generation_sequence.seqrelid is not null and (
                               generation_sequence.seqtypid <> case when c.is_identity = 'YES'
                                   then column_attribute.atttypid
                                   else 'pg_catalog.int8'::pg_catalog.regtype end
                               or generation_sequence.seqmin <> case
                                   when generation_sequence.seqincrement > 0 then 1
                                   when generation_sequence.seqtypid = 'pg_catalog.int2'::pg_catalog.regtype
                                       then -32768
                                   when generation_sequence.seqtypid = 'pg_catalog.int4'::pg_catalog.regtype
                                       then -2147483648
                                   else -9223372036854775808 end
                               or generation_sequence.seqmax <> case
                                   when generation_sequence.seqincrement < 0 then -1
                                   when generation_sequence.seqtypid = 'pg_catalog.int2'::pg_catalog.regtype
                                       then 32767
                                   when generation_sequence.seqtypid = 'pg_catalog.int4'::pg_catalog.regtype
                                       then 2147483647
                                   else 9223372036854775807 end)
                           then 'non-default sequence bounds or data type'
                       else null
                   end as UNSUPPORTED_COLUMN_REASON,
                   c.column_default as COLUMN_DEFAULT,
                   c.column_default as GENERATION_EXPRESSION,
                   generation_sequence.seqstart as GENERATION_START,
                   generation_sequence.seqincrement as GENERATION_INCREMENT,
                   generation_sequence.seqcache as GENERATION_CACHE,
                   null as COLUMN_CHARSET,
                   c.collation_name as COLUMN_COLLATION,
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
            left join pg_catalog.pg_attrdef column_default
              on column_default.adrelid = column_table.oid
             and column_default.adnum = column_attribute.attnum
            left join pg_catalog.pg_depend default_dependency
              on default_dependency.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
             and default_dependency.objid = column_default.oid
             and default_dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
             and default_dependency.deptype = 'n'
            left join pg_catalog.pg_class default_sequence
              on default_sequence.oid = default_dependency.refobjid
             and default_sequence.relkind = 'S'
            left join pg_catalog.pg_class identity_sequence
              on identity_sequence.oid = pg_catalog.pg_get_serial_sequence(
                     pg_catalog.quote_ident(c.table_schema) || '.' ||
                     pg_catalog.quote_ident(c.table_name), c.column_name)::pg_catalog.regclass
             and identity_sequence.relkind = 'S'
            left join pg_catalog.pg_sequence generation_sequence
              on generation_sequence.seqrelid = coalesce(default_sequence.oid, identity_sequence.oid)
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
                       and ix.indoption[ord.position] in (0, 3)
                       and ix.indcollation[ord.position] = a.attcollation
                       and not coalesce((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false))
                        as INDEX_REPRESENTABLE,
                   case when (ix.indoption[ord.position] & 1) = 1 then 'DESC' else 'ASC' end
                        as INDEX_DIRECTION,
                   case
                       when not ix.indislive then 'index is being dropped'
                       when not ix.indisready then 'index is not ready for writes'
                       when not ix.indisvalid then 'index is invalid'
                       when ix.indpred is not null then 'partial predicate'
                       when ix.indexprs is not null then 'expression key'
                       when ord.position >= ix.indnkeyatts then 'included column'
                       when am.amname <> 'btree' then 'non-btree access method'
                       when not opc.opcdefault then 'non-default operator class'
                        when ix.indoption[ord.position] not in (0, 3) then 'non-default null ordering'
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
              and not exists (
                  select 1 from pg_catalog.pg_constraint owned_constraint
                  where owned_constraint.conindid = ix.indexrelid
              )
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select n.nspname as TABLE_SCHEMA,
                   con.conname as FOREIGN_KEY_NAME,
                   a.attname as COLUMN_NAME,
                   rn.nspname as REFERENCED_TABLE_SCHEMA,
                   rt.relname as REFERENCED_TABLE_NAME,
                    ra.attname as REFERENCED_COLUMN_NAME,
                    case con.confdeltype
                        when 'a' then 'NO_ACTION' when 'r' then 'RESTRICT'
                        when 'c' then 'CASCADE' when 'n' then 'SET_NULL'
                        when 'd' then 'SET_DEFAULT' end as ON_DELETE,
                    case con.confupdtype
                        when 'a' then 'NO_ACTION' when 'r' then 'RESTRICT'
                        when 'c' then 'CASCADE' when 'n' then 'SET_NULL'
                        when 'd' then 'SET_DEFAULT' end as ON_UPDATE,
                    (not con.condeferrable and con.convalidated and con.confmatchtype = 's')
                        as CONSTRAINT_REPRESENTABLE
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

    private static final String BASE_TABLE_SQL = """
            select pg_catalog.obj_description(t.oid, 'pg_class') as TABLE_COMMENT,
                   (t.relkind = 'r' and not t.relispartition
                       and not exists (
                           select 1 from pg_catalog.pg_inherits inheritance
                           where inheritance.inhrelid = t.oid or inheritance.inhparent = t.oid
                       )) as TABLE_REPRESENTABLE
            from pg_catalog.pg_class t
            join pg_catalog.pg_namespace n on n.oid = t.relnamespace
            where t.relname = ? and t.relkind in ('r', 'p')
            """;

    private static final String BASE_PRIMARY_KEY_SQL = """
            select con.conname as CONSTRAINT_NAME,
                   a.attname as COLUMN_NAME,
                   (not con.condeferrable and con.convalidated
                       and ix.indisvalid and ix.indisready and ix.indislive
                       and ix.indisunique and ix.indisprimary and ix.indimmediate
                       and ix.indpred is null and ix.indexprs is null
                       and ix.indnatts = ix.indnkeyatts
                       and ix.indnkeyatts = pg_catalog.cardinality(con.conkey)
                       and ix.indkey[ord.position - 1] = con.conkey[ord.position]
                       and am.amname = 'btree' and opc.opcdefault
                       and ix.indoption[ord.position - 1] = 0
                       and ix.indcollation[ord.position - 1] = a.attcollation
                       and index_relation.reloptions is null
                       and index_relation.reltablespace = 0) as CONSTRAINT_REPRESENTABLE
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class t on t.oid = con.conrelid
            join pg_catalog.pg_namespace n on n.oid = t.relnamespace
            join pg_catalog.pg_index ix on ix.indexrelid = con.conindid
            join pg_catalog.pg_class index_relation on index_relation.oid = ix.indexrelid
            join pg_catalog.pg_am am on am.oid = index_relation.relam
            join pg_catalog.generate_subscripts(con.conkey, 1) as ord(position) on true
            join pg_catalog.pg_attribute a
              on a.attrelid = t.oid and a.attnum = con.conkey[ord.position]
            join pg_catalog.pg_opclass opc on opc.oid = ix.indclass[ord.position - 1]
            where con.contype = 'p' and t.relname = ?
            """;

    private static final String BASE_UNIQUE_SQL = """
            select con.conname as CONSTRAINT_NAME,
                   a.attname as COLUMN_NAME,
                   (not con.condeferrable and con.convalidated
                       and ix.indisvalid and ix.indisready and ix.indislive
                       and ix.indisunique and not ix.indisprimary and ix.indimmediate
                       and ix.indpred is null and ix.indexprs is null
                       and ix.indnatts = ix.indnkeyatts
                       and ix.indnkeyatts = pg_catalog.cardinality(con.conkey)
                       and ix.indkey[ord.position - 1] = con.conkey[ord.position]
                       and am.amname = 'btree' and opc.opcdefault
                       and ix.indoption[ord.position - 1] = 0
                       and ix.indcollation[ord.position - 1] = a.attcollation
                       and index_relation.reloptions is null
                       and index_relation.reltablespace = 0
                       and not coalesce((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false))
                       as CONSTRAINT_REPRESENTABLE
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class t on t.oid = con.conrelid
            join pg_catalog.pg_namespace n on n.oid = t.relnamespace
            join pg_catalog.pg_index ix on ix.indexrelid = con.conindid
            join pg_catalog.pg_class index_relation on index_relation.oid = ix.indexrelid
            join pg_catalog.pg_am am on am.oid = index_relation.relam
            join pg_catalog.generate_subscripts(con.conkey, 1) as ord(position) on true
            join pg_catalog.pg_attribute a
              on a.attrelid = t.oid and a.attnum = con.conkey[ord.position]
            join pg_catalog.pg_opclass opc on opc.oid = ix.indclass[ord.position - 1]
            where con.contype = 'u' and t.relname = ?
            """;

    private static final String BASE_CHECKS_SQL = """
            select con.conname as CONSTRAINT_NAME,
                   pg_catalog.pg_get_expr(con.conbin, con.conrelid, false) as CHECK_EXPRESSION,
                   (con.convalidated and not con.connoinherit) as CHECK_REPRESENTABLE
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class t on t.oid = con.conrelid
            join pg_catalog.pg_namespace n on n.oid = t.relnamespace
            where con.contype = 'c' and t.relname = ?
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
        return InformationSchemaFormMetadataReader.Queries.complete(
                PostgreSqlReactiveFormMetadataReader::columnQuery,
                PostgreSqlReactiveFormMetadataReader::indexQuery,
                PostgreSqlReactiveFormMetadataReader::foreignKeyQuery,
                PostgreSqlReactiveFormMetadataReader::logicalType,
                PostgreSqlReactiveFormMetadataReader::tableQuery,
                PostgreSqlReactiveFormMetadataReader::primaryKeyQuery,
                PostgreSqlReactiveFormMetadataReader::uniqueConstraintQuery,
                PostgreSqlReactiveFormMetadataReader::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);
    }

    @Override
    public ReactiveSqlExecutor metadataExecutor() {
        return delegate.metadataExecutor();
    }

    @Override
    public SchemaSnapshotCoverage snapshotCoverage() {
        return delegate.snapshotCoverage();
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

    @Override
    public Mono<SchemaSnapshot> readSnapshot(String table) {
        return delegate.readSnapshot(table);
    }

    @Override
    public Mono<SchemaSnapshot> readSnapshot(String schema, String table) {
        return delegate.readSnapshot(schema, table);
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

    private static SqlRequest tableQuery(String schema, String table) {
        return scopedQuery(BASE_TABLE_SQL, schema, table, "t", "n", "");
    }

    private static SqlRequest primaryKeyQuery(String schema, String table) {
        return scopedQuery(BASE_PRIMARY_KEY_SQL, schema, table, "t", "n", " order by ord.position");
    }

    private static SqlRequest uniqueConstraintQuery(String schema, String table) {
        return scopedQuery(BASE_UNIQUE_SQL, schema, table, "t", "n",
                           " order by con.conname, ord.position");
    }

    private static SqlRequest checkConstraintQuery(String schema, String table) {
        return scopedQuery(BASE_CHECKS_SQL, schema, table, "t", "n", " order by con.conname");
    }

    private static SqlRequest scopedQuery(String base,
                                          String schema,
                                          String table,
                                          String tableAlias,
                                          String schemaAlias,
                                          String orderBy) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(base + " and pg_catalog.pg_table_is_visible(" + tableAlias + ".oid)" + orderBy,
                                  List.of(safeTable));
        }
        return new SqlRequest(base + " and " + schemaAlias + ".nspname = ?" + orderBy,
                              List.of(safeTable, schema.trim()));
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "postgresql");
    }
}
