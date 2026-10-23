package com.flying.orm.rdb.metadata;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.type.DatabaseTypes;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL 的元数据查询定义。这里仅保存 INFORMATION_SCHEMA 的方言事实，
 * 查询编排和结果组装统一由 {@link InformationSchemaFormMetadataReader} 负责。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class MySqlMetadataQueries {

    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";

    private static final String SUPPORTED_DEFAULT_EXPRESSION = """
            ((lower(c.DATA_TYPE) in ('timestamp', 'datetime')
              and lower(trim(c.COLUMN_DEFAULT)) regexp '^current_timestamp([(][0-6]?[)])?$')
             or (lower(c.DATA_TYPE) = 'date'
                 and lower(trim(c.COLUMN_DEFAULT)) regexp '^(current_date|curdate)([(][)])?$')
             or (lower(c.DATA_TYPE) = 'time'
                 and lower(trim(c.COLUMN_DEFAULT)) regexp '^(current_time|curtime)([(][0-6]?[)])?$'))
            """;

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   c.TABLE_NAME as METADATA_TABLE,
                   case when table_info.TABLE_TYPE = 'BASE TABLE'
                        then nullif(table_info.TABLE_COMMENT, '') end as TABLE_COMMENT,
                   c.TABLE_SCHEMA as RESOLUTION_SCHEMA,
                   case
                       when lower(c.DATA_TYPE) in ('enum', 'set') then c.COLUMN_TYPE
                       when binary left(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}'))
                                = binary '${COMMENT_ESCAPE}'
                           then c.DATA_TYPE
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and binary left(c.COLUMN_COMMENT, char_length('${OFFSET_TIME_MARKER}'))
                                = binary '${OFFSET_TIME_MARKER}'
                           then 'OFFSET_TIME'
                       when lower(c.DATA_TYPE) in ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint',
                                                   'bit', 'binary', 'varbinary')
                           then c.COLUMN_TYPE
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.COLUMN_TYPE as PHYSICAL_DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   c.DATETIME_PRECISION as TEMPORAL_PRECISION,
                   case
                       when binary left(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}'))
                                = binary '${COMMENT_ESCAPE}'
                           then substring(c.COLUMN_COMMENT, char_length('${COMMENT_ESCAPE}') + 1)
                       when lower(c.DATA_TYPE) = 'varchar' and c.CHARACTER_MAXIMUM_LENGTH = 32
                            and binary left(c.COLUMN_COMMENT, char_length('${OFFSET_TIME_MARKER}'))
                                = binary '${OFFSET_TIME_MARKER}'
                           then nullif(substring(c.COLUMN_COMMENT,
                                                char_length('${OFFSET_TIME_MARKER}') + 1), '')
                       else c.COLUMN_COMMENT
                   end as REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   case when lower(c.EXTRA) not like '%virtual generated%'
                                  and lower(c.EXTRA) not like '%stored generated%'
                                  and lower(c.EXTRA) not like '%on update%'
                                  and lower(c.EXTRA) not like '%invisible%'
                                  and lower(c.EXTRA) not like '%storage%'
                                  and lower(c.EXTRA) not like '%column_format%'
                        then true else false end as COLUMN_REPRESENTABLE,
                   case
                       when lower(c.EXTRA) like '%virtual generated%'
                                 or lower(c.EXTRA) like '%stored generated%'
                           then 'generated expression'
                       when lower(c.EXTRA) like '%on update%' then 'on-update expression'
                       when lower(c.EXTRA) like '%invisible%' then 'invisible column'
                       when lower(c.EXTRA) like '%storage%' then 'column storage option'
                       when lower(c.EXTRA) like '%column_format%' then 'column format option'
                       else null
                   end as UNSUPPORTED_COLUMN_REASON,
                   c.COLUMN_DEFAULT as COLUMN_DEFAULT,
                   case when lower(c.EXTRA) like '%default_generated%'
                             and not ${SUPPORTED_DEFAULT_EXPRESSION}
                        then true else false end as NATIVE_DEFAULT_EXPRESSION,
                   null as GENERATION_EXPRESSION,
                   c.CHARACTER_SET_NAME as COLUMN_CHARSET,
                   c.COLLATION_NAME as COLUMN_COLLATION,
                   case when lower(c.EXTRA) like '%auto_increment%' then true else false end as IS_IDENTITY,
                   case when pk.CONSTRAINT_NAME is null then false else true end as PRIMARY_KEY
            from information_schema.COLUMNS c
            left join information_schema.TABLES table_info
              on table_info.TABLE_SCHEMA = c.TABLE_SCHEMA and table_info.TABLE_NAME = c.TABLE_NAME
            left join (
                select pk_kcu.TABLE_SCHEMA,
                       pk_kcu.TABLE_NAME,
                       pk_kcu.COLUMN_NAME,
                       pk_kcu.CONSTRAINT_NAME
                from information_schema.KEY_COLUMN_USAGE pk_kcu
                join information_schema.TABLE_CONSTRAINTS pk_tc
                  on pk_tc.CONSTRAINT_SCHEMA = pk_kcu.CONSTRAINT_SCHEMA
                 and pk_tc.TABLE_SCHEMA = pk_kcu.TABLE_SCHEMA
                 and pk_tc.TABLE_NAME = pk_kcu.TABLE_NAME
                 and pk_tc.CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME
                 and pk_tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk
              on pk.TABLE_SCHEMA = c.TABLE_SCHEMA
             and pk.TABLE_NAME = c.TABLE_NAME
             and pk.COLUMN_NAME = c.COLUMN_NAME
            where ${TABLE_FILTER}
            """.replace("${OFFSET_TIME_MARKER}", OFFSET_TIME_MARKER)
               .replace("${COMMENT_ESCAPE}", COMMENT_ESCAPE)
               .replace("${SUPPORTED_DEFAULT_EXPRESSION}", SUPPORTED_DEFAULT_EXPRESSION);

    // FK 可以复用一个调用方先建的同名索引，字典又不记录索引所有权；因此这里必须保留该物理事实。
    // PRIMARY/UNIQUE 的同名索引是约束自身的实现细节，继续由约束查询统一表达，避免重复建模。
    private static final String BASE_INDEXES_SQL = """
            select s.INDEX_NAME,
                   s.TABLE_NAME as METADATA_TABLE,
                   s.COLUMN_NAME,
                   case when s.NON_UNIQUE = 0 then true else false end as UNIQUE_INDEX,
                   case when s.COLLATION = 'D' then 'DESC' else 'ASC' end as INDEX_DIRECTION,
                   case when s.COLUMN_NAME is not null
                              and s.SUB_PART is null
                              and s.COLLATION in ('A', 'D')
                              and s.INDEX_TYPE = 'BTREE'
                              and s.IS_VISIBLE = 'YES'
                              and coalesce(s.INDEX_COMMENT, '') = ''
                        then true else false end as INDEX_REPRESENTABLE,
                   case
                       when s.COLUMN_NAME is null then 'functional key part'
                       when s.SUB_PART is not null then 'prefix key part'
                       when s.COLLATION is null then 'missing key direction'
                       when s.INDEX_TYPE <> 'BTREE' then 'non-btree index'
                       when s.IS_VISIBLE <> 'YES' then 'invisible index'
                       when coalesce(s.INDEX_COMMENT, '') <> '' then 'index comment'
                       else null
                   end as UNSUPPORTED_INDEX_REASON
            from information_schema.STATISTICS s
            where ${TABLE_FILTER}
              and s.INDEX_NAME <> 'PRIMARY'
              and not exists (
                  select 1
                  from information_schema.TABLE_CONSTRAINTS owned_constraint
                  where owned_constraint.CONSTRAINT_SCHEMA = s.TABLE_SCHEMA
                    and owned_constraint.TABLE_NAME = s.TABLE_NAME
                    and owned_constraint.CONSTRAINT_NAME = s.INDEX_NAME
                    and owned_constraint.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
              )
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select kcu.TABLE_SCHEMA as TABLE_SCHEMA,
                   kcu.TABLE_NAME as METADATA_TABLE,
                   kcu.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   kcu.COLUMN_NAME,
                   kcu.REFERENCED_TABLE_SCHEMA as REFERENCED_TABLE_SCHEMA,
                   kcu.REFERENCED_TABLE_NAME,
                   kcu.REFERENCED_COLUMN_NAME,
                   replace(rc.DELETE_RULE, ' ', '_') as ON_DELETE,
                   replace(rc.UPDATE_RULE, ' ', '_') as ON_UPDATE,
                   case when rc.MATCH_OPTION = 'NONE' then true else false end
                       as CONSTRAINT_REPRESENTABLE
            from information_schema.KEY_COLUMN_USAGE kcu
            join information_schema.REFERENTIAL_CONSTRAINTS rc
              on rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
             and rc.TABLE_NAME = kcu.TABLE_NAME
             and rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            where ${TABLE_FILTER}
              and kcu.REFERENCED_TABLE_NAME is not null
            """;

    private static final String BASE_TABLE_SQL = """
            select nullif(t.TABLE_COMMENT, '') as TABLE_COMMENT,
                   case when not exists (
                       select 1 from information_schema.PARTITIONS partition_options
                       where partition_options.TABLE_SCHEMA = t.TABLE_SCHEMA
                         and partition_options.TABLE_NAME = t.TABLE_NAME
                         and partition_options.PARTITION_NAME is not null
                   ) then true else false end as TABLE_REPRESENTABLE
            from information_schema.TABLES t
            where ${TABLE_FILTER} and t.TABLE_TYPE = 'BASE TABLE'
            """;

    private static final String BASE_PRIMARY_KEY_SQL = constraintColumnsSql("PRIMARY KEY");
    private static final String BASE_UNIQUE_SQL = constraintColumnsSql("UNIQUE");

    private static String constraintColumnsSql(String type) {
        return """
            select tc.CONSTRAINT_NAME,
                   tc.TABLE_NAME as METADATA_TABLE,
                   kcu.COLUMN_NAME,
                   case when s.COLLATION = 'D' then 'DESC' else 'ASC' end as INDEX_DIRECTION,
                   case when s.COLUMN_NAME = kcu.COLUMN_NAME
                                  and s.SUB_PART is null
                                  and ${KEY_DIRECTION}
                                  and s.INDEX_TYPE = 'BTREE'
                                  and s.IS_VISIBLE = 'YES'
                                  and coalesce(s.INDEX_COMMENT, '') = ''
                        then true else false end as CONSTRAINT_REPRESENTABLE
            from information_schema.TABLE_CONSTRAINTS tc
            join information_schema.KEY_COLUMN_USAGE kcu
              on kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
             and kcu.TABLE_NAME = tc.TABLE_NAME
             and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            join information_schema.STATISTICS s
              on s.TABLE_SCHEMA = tc.TABLE_SCHEMA
             and s.TABLE_NAME = tc.TABLE_NAME
             and s.INDEX_NAME = tc.CONSTRAINT_NAME
             and s.SEQ_IN_INDEX = kcu.ORDINAL_POSITION
            where tc.CONSTRAINT_TYPE = '${CONSTRAINT_TYPE}' and ${TABLE_FILTER}
            """.replace("${CONSTRAINT_TYPE}", type)
               .replace("${KEY_DIRECTION}", "UNIQUE".equals(type)
                       ? "s.COLLATION in ('A', 'D')" : "s.COLLATION = 'A'");
    }

    private static final String BASE_CHECKS_SQL = """
            select tc.CONSTRAINT_NAME,
                   tc.TABLE_NAME as METADATA_TABLE,
                   cc.CHECK_CLAUSE as CHECK_EXPRESSION,
                   case when tc.ENFORCED = 'YES' then true else false end as CHECK_REPRESENTABLE
            from information_schema.TABLE_CONSTRAINTS tc
            join information_schema.CHECK_CONSTRAINTS cc
              on cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            where tc.CONSTRAINT_TYPE = 'CHECK' and ${TABLE_FILTER}
            """;

    private MySqlMetadataQueries() {
    }

    static MetadataQueryProfile.Queries queries() {
        return MetadataQueryProfile.Queries.complete(
                MySqlMetadataQueries::columnQuery,
                MySqlMetadataQueries::indexQuery,
                MySqlMetadataQueries::foreignKeyQuery,
                MySqlMetadataQueries::logicalType,
                MySqlMetadataQueries::tableQuery,
                MySqlMetadataQueries::primaryKeyQuery,
                MySqlMetadataQueries::uniqueConstraintQuery,
                MySqlMetadataQueries::checkConstraintQuery,
                MetadataQueryProfile.SnapshotDialect.MYSQL);
    }

    private static SqlRequest columnQuery(String schema, String table) {
        return scopedQuery(BASE_COLUMNS_SQL, schema, table, "c", " order by c.ORDINAL_POSITION");
    }

    private static SqlRequest indexQuery(String schema, String table) {
        return scopedQuery(BASE_INDEXES_SQL, schema, table, "s",
                           " order by s.INDEX_NAME, s.SEQ_IN_INDEX");
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        return scopedQuery(BASE_FOREIGN_KEYS_SQL, schema, table, "kcu",
                           " order by kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION");
    }

    private static SqlRequest tableQuery(String schema, String table) {
        return scopedQuery(BASE_TABLE_SQL, schema, table, "t", "");
    }

    private static SqlRequest primaryKeyQuery(String schema, String table) {
        return scopedQuery(BASE_PRIMARY_KEY_SQL, schema, table, "tc", " order by kcu.ORDINAL_POSITION");
    }

    private static SqlRequest uniqueConstraintQuery(String schema, String table) {
        return scopedQuery(BASE_UNIQUE_SQL, schema, table, "tc",
                           " order by tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION");
    }

    private static SqlRequest checkConstraintQuery(String schema, String table) {
        return scopedQuery(BASE_CHECKS_SQL, schema, table, "tc", " order by tc.CONSTRAINT_NAME");
    }

    private static SqlRequest scopedQuery(String base,
                                          String schema,
                                          String table,
                                          String tableAlias,
                                          String orderBy) {
        List<Object> parameters = new ArrayList<>(2);
        String filter = "1 = 1";
        if (table != null) {
            String safeTable = MetadataQueryProfile.requireText(table, "table");
            filter = tableAlias + ".TABLE_NAME = ?";
            parameters.add(safeTable);
        }
        String schemaFilter = "DATABASE()";
        if (schema != null && !schema.isBlank()) {
            schemaFilter = "?";
            String safeSchema = schema.trim();
            parameters.add(safeSchema);
        }
        return new SqlRequest(base.replace("${TABLE_FILTER}", filter)
                + " and " + tableAlias + ".TABLE_SCHEMA = " + schemaFilter + orderBy, parameters);
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "mysql");
    }
}
