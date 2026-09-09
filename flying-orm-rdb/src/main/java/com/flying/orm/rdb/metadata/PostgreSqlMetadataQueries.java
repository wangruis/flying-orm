package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.type.DatabaseTypes;

import java.util.List;

/**
 * PostgreSQL 的元数据查询定义。这里仅保存 information_schema 和 pg_catalog 的方言事实，
 * 查询编排和结果组装统一由 {@link InformationSchemaFormMetadataReader} 负责。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class PostgreSqlMetadataQueries {

    private static final String BASE_COLUMNS_SQL = """
            select c.column_name as COLUMN_NAME,
                   c.table_schema as RESOLUTION_SCHEMA,
                   case when c.data_type in ('ARRAY', 'USER-DEFINED', 'bit', 'bit varying')
                        then pg_catalog.format_type(column_attribute.atttypid,
                                                    column_attribute.atttypmod)
                        else c.data_type end as DATA_TYPE,
                   c.data_type as LOGICAL_DATA_TYPE,
                   pg_catalog.format_type(column_attribute.atttypid,
                                          column_attribute.atttypmod) as PHYSICAL_DATA_TYPE,
                   case when element_type.oid is null
                        then column_type_schema.nspname
                        else element_type_schema.nspname end as PHYSICAL_TYPE_SCHEMA,
                   case when element_type.oid is null
                        then column_type.typname
                        else element_type.typname end as PHYSICAL_TYPE_NAME,
                   case when element_type.oid is null then false else true end as PHYSICAL_ARRAY,
                   coalesce(element_extension.extname,
                            column_extension.extname) as PHYSICAL_TYPE_EXTENSION,
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
            join pg_catalog.pg_type column_type
              on column_type.oid = column_attribute.atttypid
            join pg_catalog.pg_namespace column_type_schema
              on column_type_schema.oid = column_type.typnamespace
            left join pg_catalog.pg_type element_type
              on element_type.oid = column_type.typelem
             and column_type.typtype <> 'd'
             and column_type.typcategory = 'A'
             and element_type.typarray = column_type.oid
            left join pg_catalog.pg_namespace element_type_schema
              on element_type_schema.oid = element_type.typnamespace
            left join pg_catalog.pg_depend column_extension_dependency
              on column_extension_dependency.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
             and column_extension_dependency.objid = column_type.oid
             and column_extension_dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             and column_extension_dependency.deptype = 'e'
            left join pg_catalog.pg_extension column_extension
              on column_extension.oid = column_extension_dependency.refobjid
            left join pg_catalog.pg_depend element_extension_dependency
              on element_extension_dependency.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
             and element_extension_dependency.objid = element_type.oid
             and element_extension_dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
             and element_extension_dependency.deptype = 'e'
            left join pg_catalog.pg_extension element_extension
              on element_extension.oid = element_extension_dependency.refobjid
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
                    and owned_constraint.contype in ('p', 'u', 'x')
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
                    case
                        when t.relkind = 'r' then not t.relispartition and not exists (
                            select 1 from pg_catalog.pg_inherits inheritance
                            where inheritance.inhrelid = t.oid or inheritance.inhparent = t.oid
                        )
                        when t.relkind = 'p' then coalesce(
                            not t.relispartition
                            and not exists (
                                select 1 from pg_catalog.pg_inherits inheritance
                                where inheritance.inhrelid = t.oid
                            )
                            and partitioning.partstrat = 'r'
                            and partitioning.partnatts = 1
                            and partitioning.partexprs is null
                            and partitioning.partattrs[0] > 0
                            and partition_attribute.attnum = partitioning.partattrs[0]
                            and partition_attribute.attnum > 0
                            and not partition_attribute.attisdropped
                            and partition_access_method.amname = 'btree'
                            and partition_opclass.opcdefault
                            and partitioning.partcollation[0] = partition_attribute.attcollation,
                            false)
                        else false
                    end as TABLE_REPRESENTABLE,
                    case
                        when t.relispartition or exists (
                            select 1 from pg_catalog.pg_inherits inheritance
                            where inheritance.inhrelid = t.oid
                        ) then 'partition or inheritance child'
                        when t.relkind = 'r' and exists (
                            select 1 from pg_catalog.pg_inherits inheritance
                            where inheritance.inhparent = t.oid
                        ) then 'inheritance parent'
                        when t.relkind = 'p' and partitioning.partrelid is null
                            then 'partition metadata'
                        when t.relkind = 'p' and partitioning.partstrat <> 'r'
                            then 'non-range partition strategy'
                        when t.relkind = 'p' and partitioning.partnatts <> 1
                            then 'multiple partition keys'
                        when t.relkind = 'p' and (
                                partitioning.partexprs is not null or partitioning.partattrs[0] = 0)
                            then 'partition expression'
                        when t.relkind = 'p' and (
                                partition_attribute.attnum is null
                                or partition_attribute.attnum <= 0
                                or partition_attribute.attisdropped)
                            then 'partition column metadata'
                        when t.relkind = 'p' and (
                                partition_opclass.oid is null
                                or partition_access_method.oid is null)
                            then 'partition operator class metadata'
                        when t.relkind = 'p' and (
                                partition_access_method.amname <> 'btree'
                                or not partition_opclass.opcdefault)
                            then 'non-default partition operator class'
                        when t.relkind = 'p' and
                                partitioning.partcollation[0] <> partition_attribute.attcollation
                            then 'non-default partition collation'
                        else null
                    end as UNSUPPORTED_TABLE_REASON,
                    (t.relkind = 'p') as TABLE_PARTITIONED,
                    case when partitioning.partstrat = 'r' then 'RANGE' else null end
                        as PARTITION_STRATEGY,
                    partition_attribute.attname as PARTITION_COLUMN
             from pg_catalog.pg_class t
             join pg_catalog.pg_namespace n on n.oid = t.relnamespace
             left join pg_catalog.pg_partitioned_table partitioning
               on partitioning.partrelid = t.oid
             left join pg_catalog.pg_attribute partition_attribute
               on partition_attribute.attrelid = t.oid
              and partition_attribute.attnum = partitioning.partattrs[0]
             left join pg_catalog.pg_opclass partition_opclass
               on partition_opclass.oid = partitioning.partclass[0]
             left join pg_catalog.pg_am partition_access_method
               on partition_access_method.oid = partition_opclass.opcmethod
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

    private PostgreSqlMetadataQueries() {
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                PostgreSqlMetadataQueries::columnQuery,
                PostgreSqlMetadataQueries::indexQuery,
                PostgreSqlMetadataQueries::foreignKeyQuery,
                PostgreSqlMetadataQueries::logicalType,
                PostgreSqlMetadataQueries::physicalType,
                PostgreSqlMetadataQueries::tableQuery,
                PostgreSqlMetadataQueries::primaryKeyQuery,
                PostgreSqlMetadataQueries::uniqueConstraintQuery,
                PostgreSqlMetadataQueries::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.POSTGRESQL);
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

    private static String physicalType(String dataType) {
        return DatabaseType.of(dataType).requireSafe("PostgreSQL physical data type").declaration();
    }
}
