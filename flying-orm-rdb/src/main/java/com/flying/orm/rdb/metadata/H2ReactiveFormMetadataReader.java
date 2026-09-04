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
 * H2 的动态表单元数据读取器。它走 R2DBC 查询 INFORMATION_SCHEMA，不碰 JDBC 元数据。
 * 主要服务内嵌开发和测试环境，返回结构仍与生产方言使用同一套 DynamicForm/TableMetadata 模型。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务只依赖元数据 reader 接口。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class H2ReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String BASE_COLUMNS_SQL = """
            select lower(c.COLUMN_NAME) as COLUMN_NAME,
                   lower(c.TABLE_SCHEMA) as RESOLUTION_SCHEMA,
                   case
                       when upper(c.DATA_TYPE) = 'CHARACTER VARYING'
                            and c.CHARACTER_MAXIMUM_LENGTH = 1000000000 then 'TEXT'
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION,
                   c.NUMERIC_SCALE,
                   c.DATETIME_PRECISION as TEMPORAL_PRECISION,
                   c.REMARKS,
                   c.IS_NULLABLE as NULLABLE,
                   case when c.IS_GENERATED = 'NEVER'
                                  and c.COLUMN_NAME = upper(c.COLUMN_NAME)
                                  and regexp_like(c.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and c.TABLE_NAME = upper(c.TABLE_NAME)
                                  and regexp_like(c.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and c.TABLE_SCHEMA = upper(c.TABLE_SCHEMA)
                                  and regexp_like(c.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                                  and c.DOMAIN_NAME is null
                                  and c.GEOMETRY_TYPE is null
                                  and c.COLUMN_ON_UPDATE is null
                                  and c.IS_VISIBLE
                                  and not c.DEFAULT_ON_NULL
                                  and (c.IS_IDENTITY = 'NO' or (
                                      c.IDENTITY_GENERATION = 'BY DEFAULT'
                                      and c.IDENTITY_CYCLE = 'NO'
                                      and c.IDENTITY_CACHE is not null
                                      and c.IDENTITY_START is not null
                                      and c.IDENTITY_INCREMENT is not null
                                      and c.DATA_TYPE in ('SMALLINT', 'INTEGER', 'BIGINT')
                                      and c.IDENTITY_MINIMUM = case
                                          when c.IDENTITY_INCREMENT >= 0 then least(c.IDENTITY_START, 1)
                                          when c.DATA_TYPE = 'SMALLINT' then -32768
                                          when c.DATA_TYPE = 'INTEGER' then -2147483648
                                          else -9223372036854775808 end
                                      and c.IDENTITY_MAXIMUM = case
                                          when c.IDENTITY_INCREMENT < 0 then greatest(c.IDENTITY_START, -1)
                                          when c.DATA_TYPE = 'SMALLINT' then 32767
                                          when c.DATA_TYPE = 'INTEGER' then 2147483647
                                          else 9223372036854775807 end
                                  ))
                                  and not (upper(trim(coalesce(c.COLUMN_DEFAULT, '')))
                                      like 'NEXT VALUE FOR %' and generation_sequence.SEQUENCE_NAME is null)
                                  and (generation_sequence.SEQUENCE_NAME is null or (
                                      generation_sequence.SEQUENCE_NAME = upper(generation_sequence.SEQUENCE_NAME)
                                      and regexp_like(generation_sequence.SEQUENCE_NAME,
                                                      '^[A-Z_][A-Z0-9_]*$')
                                      and generation_sequence.SEQUENCE_SCHEMA =
                                          upper(generation_sequence.SEQUENCE_SCHEMA)
                                      and regexp_like(generation_sequence.SEQUENCE_SCHEMA,
                                                      '^[A-Z_][A-Z0-9_]*$')
                                      and generation_sequence.DATA_TYPE = 'BIGINT'
                                      and generation_sequence.NUMERIC_SCALE = 0
                                      and generation_sequence.CYCLE_OPTION = 'NO'
                                      and generation_sequence.REMARKS is null
                                      and generation_sequence.MINIMUM_VALUE = case
                                          when generation_sequence.INCREMENT >= 0
                                              then least(generation_sequence.START_VALUE, 1)
                                          else -9223372036854775808 end
                                      and generation_sequence.MAXIMUM_VALUE = case
                                          when generation_sequence.INCREMENT >= 0
                                              then 9223372036854775807
                                          else greatest(generation_sequence.START_VALUE, -1) end
                                  ))
                        then true else false end as COLUMN_REPRESENTABLE,
                   case
                       when c.COLUMN_NAME <> upper(c.COLUMN_NAME)
                                or not regexp_like(c.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                           then 'quoted or mixed-case column name'
                       when c.TABLE_NAME <> upper(c.TABLE_NAME)
                                or not regexp_like(c.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                or c.TABLE_SCHEMA <> upper(c.TABLE_SCHEMA)
                                or not regexp_like(c.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                           then 'quoted or mixed-case table identity'
                       when c.IS_GENERATED <> 'NEVER' then 'generated expression'
                       when c.DOMAIN_NAME is not null then 'domain data type'
                       when c.GEOMETRY_TYPE is not null then 'parameterized geometry data type'
                       when c.COLUMN_ON_UPDATE is not null then 'on-update expression'
                       when not c.IS_VISIBLE then 'invisible column'
                       when c.DEFAULT_ON_NULL then 'default-on-null'
                       when c.IS_IDENTITY = 'YES' and c.IDENTITY_GENERATION <> 'BY DEFAULT'
                           then 'identity generation mode'
                       when c.IS_IDENTITY = 'YES' and (c.IDENTITY_CYCLE <> 'NO'
                               or c.IDENTITY_CACHE is null
                               or c.IDENTITY_START is null
                               or c.IDENTITY_INCREMENT is null
                               or c.DATA_TYPE not in ('SMALLINT', 'INTEGER', 'BIGINT')
                               or c.IDENTITY_MINIMUM <> case
                                   when c.IDENTITY_INCREMENT >= 0 then least(c.IDENTITY_START, 1)
                                   when c.DATA_TYPE = 'SMALLINT' then -32768
                                   when c.DATA_TYPE = 'INTEGER' then -2147483648
                                   else -9223372036854775808 end
                               or c.IDENTITY_MAXIMUM <> case
                                   when c.IDENTITY_INCREMENT < 0 then greatest(c.IDENTITY_START, -1)
                                   when c.DATA_TYPE = 'SMALLINT' then 32767
                                   when c.DATA_TYPE = 'INTEGER' then 2147483647
                                   else 9223372036854775807 end)
                           then 'non-default identity options'
                       when upper(trim(coalesce(c.COLUMN_DEFAULT, ''))) like 'NEXT VALUE FOR %'
                                and generation_sequence.SEQUENCE_NAME is null
                           then 'generation sequence metadata'
                       when generation_sequence.SEQUENCE_NAME is not null and (
                               generation_sequence.SEQUENCE_NAME <> upper(generation_sequence.SEQUENCE_NAME)
                               or not regexp_like(generation_sequence.SEQUENCE_NAME,
                                                  '^[A-Z_][A-Z0-9_]*$')
                               or generation_sequence.SEQUENCE_SCHEMA <>
                                  upper(generation_sequence.SEQUENCE_SCHEMA)
                               or not regexp_like(generation_sequence.SEQUENCE_SCHEMA,
                                                  '^[A-Z_][A-Z0-9_]*$'))
                           then 'quoted or mixed-case sequence name'
                       when generation_sequence.SEQUENCE_NAME is not null and (
                               generation_sequence.DATA_TYPE <> 'BIGINT'
                               or generation_sequence.NUMERIC_SCALE <> 0
                               or generation_sequence.CYCLE_OPTION <> 'NO'
                               or generation_sequence.REMARKS is not null
                               or generation_sequence.MINIMUM_VALUE <> case
                                   when generation_sequence.INCREMENT >= 0
                                       then least(generation_sequence.START_VALUE, 1)
                                   else -9223372036854775808 end
                               or generation_sequence.MAXIMUM_VALUE <> case
                                   when generation_sequence.INCREMENT >= 0
                                       then 9223372036854775807
                                   else greatest(generation_sequence.START_VALUE, -1) end)
                           then 'non-default sequence options'
                       else null
                   end as UNSUPPORTED_COLUMN_REASON,
                   c.COLUMN_DEFAULT as COLUMN_DEFAULT,
                   case when generation_sequence.SEQUENCE_NAME is null then c.COLUMN_DEFAULT
                        else 'NEXT VALUE FOR "' || lower(generation_sequence.SEQUENCE_SCHEMA)
                             || '"."' || lower(generation_sequence.SEQUENCE_NAME) || '"'
                   end as GENERATION_EXPRESSION,
                   case when c.IS_IDENTITY = 'YES' then c.IDENTITY_START
                        else generation_sequence.START_VALUE end as GENERATION_START,
                   case when c.IS_IDENTITY = 'YES' then c.IDENTITY_INCREMENT
                        else generation_sequence.INCREMENT end as GENERATION_INCREMENT,
                   case when c.IS_IDENTITY = 'YES' then c.IDENTITY_CACHE
                        else generation_sequence.CACHE end as GENERATION_CACHE,
                   cast(null as varchar) as COLUMN_CHARSET,
                   cast(null as varchar) as COLUMN_COLLATION,
                   c.IS_IDENTITY as IS_IDENTITY,
                   case when pk.CONSTRAINT_NAME is null then false else true end as PRIMARY_KEY
            from INFORMATION_SCHEMA.COLUMNS c
            left join INFORMATION_SCHEMA.SEQUENCES generation_sequence
              on c.IS_IDENTITY = 'NO'
             and c.COLUMN_DEFAULT = 'NEXT VALUE FOR "'
                 || replace(generation_sequence.SEQUENCE_SCHEMA, '"', '""')
                 || '"."' || replace(generation_sequence.SEQUENCE_NAME, '"', '""') || '"'
            left join (
                select pk_kcu.TABLE_SCHEMA,
                       pk_kcu.TABLE_NAME,
                       pk_kcu.COLUMN_NAME,
                       pk_kcu.CONSTRAINT_NAME
                from INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_kcu
                join INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk_tc
                  on pk_tc.CONSTRAINT_SCHEMA = pk_kcu.CONSTRAINT_SCHEMA
                 and pk_tc.TABLE_SCHEMA = pk_kcu.TABLE_SCHEMA
                 and pk_tc.TABLE_NAME = pk_kcu.TABLE_NAME
                 and pk_tc.CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME
                 and pk_tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk
              on pk.TABLE_SCHEMA = c.TABLE_SCHEMA
             and pk.TABLE_NAME = c.TABLE_NAME
             and pk.COLUMN_NAME = c.COLUMN_NAME
            where c.TABLE_NAME = case
                when exists (
                    select 1
                    from INFORMATION_SCHEMA.COLUMNS exact_column
                    where exact_column.TABLE_SCHEMA = c.TABLE_SCHEMA
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
            """;

    private static final String BASE_INDEXES_SQL = """
            select lower(i.INDEX_NAME) as INDEX_NAME,
                   lower(ic.COLUMN_NAME) as COLUMN_NAME,
                   case when ic.IS_UNIQUE then true else false end as UNIQUE_INDEX,
                   ic.ORDERING_SPECIFICATION as INDEX_DIRECTION,
                   case when i.INDEX_TYPE_NAME in ('INDEX', 'UNIQUE INDEX')
                              and i.INDEX_NAME = upper(i.INDEX_NAME)
                              and regexp_like(i.INDEX_NAME, '^[A-Z_][A-Z0-9_]*$')
                              and ic.COLUMN_NAME = upper(ic.COLUMN_NAME)
                              and regexp_like(ic.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                              and i.TABLE_NAME = upper(i.TABLE_NAME)
                              and regexp_like(i.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                              and i.TABLE_SCHEMA = upper(i.TABLE_SCHEMA)
                              and regexp_like(i.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                              and i.REMARKS is null
                              and (ic.ORDERING_SPECIFICATION = 'ASC'
                                   or ic.ORDERING_SPECIFICATION = 'DESC')
                              and case upper(null_ordering_setting.SETTING_VALUE)
                                  when 'LOW' then (ic.ORDERING_SPECIFICATION = 'ASC'
                                                      and ic.NULL_ORDERING = 'FIRST')
                                                   or (ic.ORDERING_SPECIFICATION = 'DESC'
                                                      and ic.NULL_ORDERING = 'LAST')
                                  when 'HIGH' then (ic.ORDERING_SPECIFICATION = 'ASC'
                                                       and ic.NULL_ORDERING = 'LAST')
                                                    or (ic.ORDERING_SPECIFICATION = 'DESC'
                                                       and ic.NULL_ORDERING = 'FIRST')
                                  when 'FIRST' then ic.NULL_ORDERING = 'FIRST'
                                  when 'LAST' then ic.NULL_ORDERING = 'LAST'
                                  else false
                              end
                              and (not ic.IS_UNIQUE or i.NULLS_DISTINCT = 'YES')
                        then true else false end as INDEX_REPRESENTABLE,
                   case
                       when i.INDEX_TYPE_NAME not in ('INDEX', 'UNIQUE INDEX') then 'unsupported index type'
                       when i.INDEX_NAME <> upper(i.INDEX_NAME)
                                or not regexp_like(i.INDEX_NAME, '^[A-Z_][A-Z0-9_]*$')
                                or ic.COLUMN_NAME <> upper(ic.COLUMN_NAME)
                                or not regexp_like(ic.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                           then 'quoted or mixed-case index identity'
                       when i.TABLE_NAME <> upper(i.TABLE_NAME)
                                or not regexp_like(i.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                or i.TABLE_SCHEMA <> upper(i.TABLE_SCHEMA)
                                or not regexp_like(i.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                           then 'quoted or mixed-case table identity'
                       when i.REMARKS is not null then 'index comment'
                       when ic.ORDERING_SPECIFICATION not in ('ASC', 'DESC') then 'unsupported key direction'
                       when not coalesce(case upper(null_ordering_setting.SETTING_VALUE)
                           when 'LOW' then (ic.ORDERING_SPECIFICATION = 'ASC'
                                               and ic.NULL_ORDERING = 'FIRST')
                                            or (ic.ORDERING_SPECIFICATION = 'DESC'
                                               and ic.NULL_ORDERING = 'LAST')
                           when 'HIGH' then (ic.ORDERING_SPECIFICATION = 'ASC'
                                                and ic.NULL_ORDERING = 'LAST')
                                             or (ic.ORDERING_SPECIFICATION = 'DESC'
                                                and ic.NULL_ORDERING = 'FIRST')
                           when 'FIRST' then ic.NULL_ORDERING = 'FIRST'
                           when 'LAST' then ic.NULL_ORDERING = 'LAST'
                           else false
                       end, false) then 'explicit null ordering'
                       when ic.IS_UNIQUE and i.NULLS_DISTINCT <> 'YES' then 'non-default null uniqueness'
                       else 'missing index ordering metadata'
                   end as UNSUPPORTED_INDEX_REASON
            from INFORMATION_SCHEMA.INDEXES i
            join INFORMATION_SCHEMA.INDEX_COLUMNS ic
              on ic.TABLE_SCHEMA = i.TABLE_SCHEMA
             and ic.TABLE_NAME = i.TABLE_NAME
             and ic.INDEX_NAME = i.INDEX_NAME
            cross join (
                select max(SETTING_VALUE) as SETTING_VALUE
                from INFORMATION_SCHEMA.SETTINGS
                where SETTING_NAME = 'DEFAULT_NULL_ORDERING'
            ) null_ordering_setting
            where i.TABLE_NAME = case
                when exists (
                    select 1
                    from INFORMATION_SCHEMA.COLUMNS exact_column
                    where exact_column.TABLE_SCHEMA = i.TABLE_SCHEMA
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
              and i.INDEX_NAME is not null
              and not exists (
                  select 1
                  from INFORMATION_SCHEMA.TABLE_CONSTRAINTS owned_constraint
                  where owned_constraint.TABLE_SCHEMA = i.TABLE_SCHEMA
                    and owned_constraint.TABLE_NAME = i.TABLE_NAME
                    and owned_constraint.INDEX_SCHEMA = i.INDEX_SCHEMA
                    and owned_constraint.INDEX_NAME = i.INDEX_NAME
                    and owned_constraint.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
              )
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select lower(tc.TABLE_SCHEMA) as TABLE_SCHEMA,
                   lower(fk.CONSTRAINT_NAME) as FOREIGN_KEY_NAME,
                   lower(fk.COLUMN_NAME) as COLUMN_NAME,
                   lower(pk.TABLE_SCHEMA) as REFERENCED_TABLE_SCHEMA,
                   lower(pk.TABLE_NAME) as REFERENCED_TABLE_NAME,
                   lower(pk.COLUMN_NAME) as REFERENCED_COLUMN_NAME,
                   replace(rc.DELETE_RULE, ' ', '_') as ON_DELETE,
                   replace(rc.UPDATE_RULE, ' ', '_') as ON_UPDATE,
                   case when tc.IS_DEFERRABLE = 'NO'
                                  and tc.INITIALLY_DEFERRED = 'NO'
                                  and tc.ENFORCED = 'YES'
                                  and tc.REMARKS is null
                                  and rc.MATCH_OPTION = 'NONE'
                                  and tc.TABLE_SCHEMA = upper(tc.TABLE_SCHEMA)
                                  and regexp_like(tc.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                                  and tc.TABLE_NAME = upper(tc.TABLE_NAME)
                                  and regexp_like(tc.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and fk.CONSTRAINT_NAME = upper(fk.CONSTRAINT_NAME)
                                  and regexp_like(fk.CONSTRAINT_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and fk.COLUMN_NAME = upper(fk.COLUMN_NAME)
                                  and regexp_like(fk.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and pk.TABLE_SCHEMA = upper(pk.TABLE_SCHEMA)
                                  and regexp_like(pk.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                                  and pk.TABLE_NAME = upper(pk.TABLE_NAME)
                                  and regexp_like(pk.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and pk.COLUMN_NAME = upper(pk.COLUMN_NAME)
                                  and regexp_like(pk.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                        then true else false end as CONSTRAINT_REPRESENTABLE
            from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
              on fk.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and fk.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
              on rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             and rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
              on pk.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
             and pk.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
             and pk.ORDINAL_POSITION = fk.POSITION_IN_UNIQUE_CONSTRAINT
            where tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
              and tc.TABLE_NAME = case
                  when exists (
                      select 1
                      from INFORMATION_SCHEMA.COLUMNS exact_column
                      where exact_column.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        and exact_column.TABLE_NAME = ?
                  ) then ? else upper(?) end
            """;

    private static final String BASE_TABLE_SQL = """
            select t.REMARKS as TABLE_COMMENT,
                   case when t.TABLE_TYPE = 'BASE TABLE'
                                  and t.TABLE_NAME = upper(t.TABLE_NAME)
                                  and regexp_like(t.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and t.TABLE_SCHEMA = upper(t.TABLE_SCHEMA)
                                  and regexp_like(t.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                        then true else false end as TABLE_REPRESENTABLE
            from INFORMATION_SCHEMA.TABLES t
            where t.TABLE_NAME = case
                when exists (
                    select 1 from INFORMATION_SCHEMA.TABLES exact_table
                    where exact_table.TABLE_SCHEMA = t.TABLE_SCHEMA
                      and exact_table.TABLE_NAME = ?
                ) then ? else upper(?) end
            """;

    private static final String BASE_PRIMARY_KEY_SQL = constraintColumnsSql("PRIMARY KEY");
    private static final String BASE_UNIQUE_SQL = constraintColumnsSql("UNIQUE");

    private static final String BASE_CHECKS_SQL = """
            select lower(tc.CONSTRAINT_NAME) as CONSTRAINT_NAME,
                   cc.CHECK_CLAUSE as CHECK_EXPRESSION,
                   case when tc.IS_DEFERRABLE = 'NO'
                                  and tc.INITIALLY_DEFERRED = 'NO'
                                  and tc.ENFORCED = 'YES'
                                  and tc.REMARKS is null
                                  and tc.CONSTRAINT_NAME = upper(tc.CONSTRAINT_NAME)
                                  and regexp_like(tc.CONSTRAINT_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and tc.TABLE_NAME = upper(tc.TABLE_NAME)
                                  and regexp_like(tc.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                  and tc.TABLE_SCHEMA = upper(tc.TABLE_SCHEMA)
                                  and regexp_like(tc.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                        then true else false end as CHECK_REPRESENTABLE
            from INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
            join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
              on tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
             and tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            where tc.CONSTRAINT_TYPE = 'CHECK'
              and tc.TABLE_NAME = case
                  when exists (
                      select 1 from INFORMATION_SCHEMA.TABLES exact_table
                      where exact_table.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        and exact_table.TABLE_NAME = ?
                  ) then ? else upper(?) end
            """;

    private final InformationSchemaFormMetadataReader delegate;

    private H2ReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(executor,
                                                                                      "reactive sql executor must not be null"),
                                                                                      queries());
    }

    static H2ReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new H2ReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                H2ReactiveFormMetadataReader::columnQuery,
                H2ReactiveFormMetadataReader::indexQuery,
                H2ReactiveFormMetadataReader::foreignKeyQuery,
                H2ReactiveFormMetadataReader::logicalType,
                H2ReactiveFormMetadataReader::tableQuery,
                H2ReactiveFormMetadataReader::primaryKeyQuery,
                H2ReactiveFormMetadataReader::uniqueConstraintQuery,
                H2ReactiveFormMetadataReader::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.H2);
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
            String sql = BASE_COLUMNS_SQL
                    + " and c.TABLE_SCHEMA = current_schema() order by c.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_COLUMNS_SQL + " and c.TABLE_SCHEMA = " + schemaExpression()
                + " order by c.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static SqlRequest indexQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_INDEXES_SQL
                    + " and i.TABLE_SCHEMA = current_schema()"
                    + " order by i.INDEX_NAME, ic.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_INDEXES_SQL + " and i.TABLE_SCHEMA = " + schemaExpression()
                + " order by i.INDEX_NAME, ic.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static SqlRequest foreignKeyQuery(String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = BASE_FOREIGN_KEYS_SQL
                    + " and tc.TABLE_SCHEMA = current_schema()"
                    + " order by fk.CONSTRAINT_NAME, fk.ORDINAL_POSITION";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = BASE_FOREIGN_KEYS_SQL
                + " and tc.TABLE_SCHEMA = " + schemaExpression()
                + " order by fk.CONSTRAINT_NAME, fk.ORDINAL_POSITION";
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static SqlRequest tableQuery(String schema, String table) {
        return scopedQuery(BASE_TABLE_SQL, schema, table, "t", " order by t.TABLE_NAME");
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
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(base + " and " + tableAlias + ".TABLE_SCHEMA = current_schema()" + orderBy,
                                  tableParameters(safeTable));
        }
        String sql = base + " and " + tableAlias + ".TABLE_SCHEMA = " + schemaExpression() + orderBy;
        return new SqlRequest(sql, tableAndSchemaParameters(safeTable, schema));
    }

    private static String constraintColumnsSql(String type) {
        return """
                select lower(tc.CONSTRAINT_NAME) as CONSTRAINT_NAME,
                       lower(kcu.COLUMN_NAME) as COLUMN_NAME,
                       case when tc.IS_DEFERRABLE = 'NO'
                                      and tc.INITIALLY_DEFERRED = 'NO'
                                      and tc.ENFORCED = 'YES'
                                      and tc.REMARKS is null
                                      and (tc.CONSTRAINT_TYPE <> 'UNIQUE' or tc.NULLS_DISTINCT = 'YES')
                                      and tc.CONSTRAINT_NAME = upper(tc.CONSTRAINT_NAME)
                                      and regexp_like(tc.CONSTRAINT_NAME, '^[A-Z_][A-Z0-9_]*$')
                                      and kcu.COLUMN_NAME = upper(kcu.COLUMN_NAME)
                                      and regexp_like(kcu.COLUMN_NAME, '^[A-Z_][A-Z0-9_]*$')
                                      and tc.TABLE_NAME = upper(tc.TABLE_NAME)
                                      and regexp_like(tc.TABLE_NAME, '^[A-Z_][A-Z0-9_]*$')
                                      and tc.TABLE_SCHEMA = upper(tc.TABLE_SCHEMA)
                                      and regexp_like(tc.TABLE_SCHEMA, '^[A-Z_][A-Z0-9_]*$')
                            then true else false end as CONSTRAINT_REPRESENTABLE
                from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                join INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  on kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
                 and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                 and kcu.TABLE_NAME = tc.TABLE_NAME
                 and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                where tc.CONSTRAINT_TYPE = '${TYPE}'
                  and tc.TABLE_NAME = case
                      when exists (
                          select 1 from INFORMATION_SCHEMA.TABLES exact_table
                          where exact_table.TABLE_SCHEMA = tc.TABLE_SCHEMA
                            and exact_table.TABLE_NAME = ?
                      ) then ? else upper(?) end
                """.replace("${TYPE}", type);
    }

    /** H2 未加引号名称会折叠为大写；已有精确 quoted 名称时必须优先保留精确身份。 */
    private static String schemaExpression() {
        return "case when exists (select 1 from INFORMATION_SCHEMA.SCHEMATA exact_schema "
                + "where exact_schema.SCHEMA_NAME = ?) then ? else upper(?) end";
    }

    private static List<Object> tableParameters(String table) {
        return List.of(table, table, table);
    }

    private static List<Object> tableAndSchemaParameters(String table, String schema) {
        String safeSchema = schema.trim();
        return List.of(table, table, table, safeSchema, safeSchema, safeSchema);
    }

    private static String logicalType(String dataType) {
        return DatabaseTypes.logicalDeclaration(dataType, "h2");
    }
}
