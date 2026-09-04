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
 * Oracle 的动态表单元数据读取器，读取数据字典中的普通表字段、主键、索引、外键和列注释。
 * 数据字典中的物理 owner、表名按精确大小写匹配；Oracle 类型等差异留在查询模板和类型映射中。
 * 具体实现由 {@link ReactiveFormMetadataReaders} 在包内选择，业务不直接依赖数据字典 SQL。
 *
 * @author wangr
 * @date 2026-07-28
 * @version v1.0
 */
final class OracleReactiveFormMetadataReader implements ReactiveFormMetadataReader, ReactiveMetadataExecutorSource {

    private static final String TIME_MARKER = "[[flying-orm:v1:TIME]]";
    private static final String OFFSET_TIME_MARKER = "[[flying-orm:v1:OFFSET_TIME]]";
    private static final String COMMENT_ESCAPE = "[[flying-orm:v1:COMMENT]]";
    private static final String SEQUENCE_MARKER_PREFIX = "[[flying-orm:v1:SEQUENCE:";
    // Oracle 把 DATA_DEFAULT 暴露为 LONG，SQL 里不能可靠地解析 sequence.nextval。
    // flying-orm 创建列时把序列名写进可逆注释标记；查询用标记定位 ALL_SEQUENCES，Java 端再和
    // DATA_DEFAULT 解析出的真实名称交叉核对，避免把错误标记当成数据库事实。
    private static final String SEQUENCE_NAME_EXPRESSION =
            "case when NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('" + SEQUENCE_MARKER_PREFIX
                    + "')), 'NLS_SORT=BINARY') = NLSSORT('" + SEQUENCE_MARKER_PREFIX
                    + "', 'NLS_SORT=BINARY') and instr(cc.COMMENTS, ']]', LENGTH('"
                    + SEQUENCE_MARKER_PREFIX + "') + 1) > 0 then substr(cc.COMMENTS, LENGTH('"
                    + SEQUENCE_MARKER_PREFIX + "') + 1, instr(cc.COMMENTS, ']]', LENGTH('"
                    + SEQUENCE_MARKER_PREFIX + "') + 1) - LENGTH('" + SEQUENCE_MARKER_PREFIX
                    + "') - 1) end";
    private static final String STORAGE_COMMENT_EXPRESSION =
            "case when " + SEQUENCE_NAME_EXPRESSION + " is null then cc.COMMENTS else substr(cc.COMMENTS, "
                    + "instr(cc.COMMENTS, ']]', LENGTH('" + SEQUENCE_MARKER_PREFIX + "') + 1) + 2) end";

    private static final String BASE_COLUMNS_SQL_TEMPLATE = """
            select c.COLUMN_NAME,
                   c.OWNER as RESOLUTION_SCHEMA,
                   case
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 18
                            and NLSSORT(SUBSTR(${STORAGE_COMMENT}, 1, LENGTH('${TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${TIME_MARKER}', 'NLS_SORT=BINARY') then 'TIME'
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 32
                            and NLSSORT(SUBSTR(${STORAGE_COMMENT}, 1, LENGTH('${OFFSET_TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${OFFSET_TIME_MARKER}', 'NLS_SORT=BINARY') then 'OFFSET_TIME'
                       when c.DATA_TYPE = 'NUMBER' and c.DATA_PRECISION = 19 and nvl(c.DATA_SCALE, 0) = 0
                           then 'BIGINT'
                       when c.DATA_TYPE = 'NUMBER' and c.DATA_PRECISION = 10 and nvl(c.DATA_SCALE, 0) = 0
                           then 'INTEGER'
                       when c.DATA_TYPE = 'NUMBER' and c.DATA_PRECISION = 1 and nvl(c.DATA_SCALE, 0) = 0
                           then 'BOOLEAN'
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.CHAR_LENGTH as CHARACTER_MAXIMUM_LENGTH,
                   c.DATA_PRECISION as NUMERIC_PRECISION,
                   c.DATA_SCALE as NUMERIC_SCALE,
                   case when c.DATA_TYPE like 'TIMESTAMP%' then c.DATA_SCALE end as TEMPORAL_PRECISION,
                   case
                       when NLSSORT(SUBSTR(${STORAGE_COMMENT}, 1, LENGTH('${COMMENT_ESCAPE}')), 'NLS_SORT=BINARY')
                            = NLSSORT('${COMMENT_ESCAPE}', 'NLS_SORT=BINARY')
                           then substr(${STORAGE_COMMENT}, length('${COMMENT_ESCAPE}') + 1)
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 18
                            and NLSSORT(SUBSTR(${STORAGE_COMMENT}, 1, LENGTH('${TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${TIME_MARKER}', 'NLS_SORT=BINARY')
                           then nullif(substr(${STORAGE_COMMENT}, length('${TIME_MARKER}') + 1), '')
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 32
                            and NLSSORT(SUBSTR(${STORAGE_COMMENT}, 1, LENGTH('${OFFSET_TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${OFFSET_TIME_MARKER}', 'NLS_SORT=BINARY')
                           then nullif(substr(${STORAGE_COMMENT}, length('${OFFSET_TIME_MARKER}') + 1), '')
                       else nullif(${STORAGE_COMMENT}, '')
                   end as REMARKS,
                   c.NULLABLE,
                   case when c.HIDDEN_COLUMN = 'NO'
                              and c.VIRTUAL_COLUMN = 'NO'
                              and c.DATA_TYPE_OWNER is null
                              and not (idc.COLUMN_NAME is null and ${DEFAULT_ON_NULL} = 'YES')
                              and ((idc.COLUMN_NAME is null and (${SEQUENCE_NAME} is null or (
                                  column_sequence.SEQUENCE_NAME is not null
                                  and column_sequence.CYCLE_FLAG = 'N'
                                  and column_sequence.ORDER_FLAG = 'N'
                                  and nvl(${COLUMN_SCALE_FLAG}, 'N') = 'N'
                                  and nvl(${COLUMN_EXTEND_FLAG}, 'N') = 'N'
                                  and nvl(${COLUMN_SHARDED_FLAG}, 'N') = 'N'
                                  and nvl(${COLUMN_SESSION_FLAG}, 'N') = 'N'
                                  and nvl(${COLUMN_KEEP_VALUE}, 'N') = 'N'
                                  and column_sequence.MIN_VALUE = 1
                                  and column_sequence.MAX_VALUE = 9999999999999999999999999999
                              ))) or (idc.COLUMN_NAME is not null and (
                                  idc.GENERATION_TYPE = 'BY DEFAULT'
                                  and ${DEFAULT_ON_NULL} = 'YES'
                                  and identity_sequence.SEQUENCE_NAME is not null
                                  and identity_sequence.CYCLE_FLAG = 'N'
                                  and identity_sequence.ORDER_FLAG = 'N'
                                  and nvl(${SCALE_FLAG}, 'N') = 'N'
                                  and nvl(${EXTEND_FLAG}, 'N') = 'N'
                                  and nvl(${SHARDED_FLAG}, 'N') = 'N'
                                  and nvl(${SESSION_FLAG}, 'N') = 'N'
                                  and nvl(${KEEP_VALUE}, 'N') = 'N'
                                  and identity_sequence.MIN_VALUE = case
                                      when identity_sequence.INCREMENT_BY > 0 then 1
                                      else -999999999999999999999999999 end
                                  and identity_sequence.MAX_VALUE = case
                                      when identity_sequence.INCREMENT_BY > 0
                                          then 9999999999999999999999999999
                                      else -1 end
                                  and regexp_substr(idc.IDENTITY_OPTIONS,
                                      'START WITH:[[:space:]]*([+-]?[0-9]+)', 1, 1, 'i', 1) is not null
                              )))
                        then 'true' else 'false' end as COLUMN_REPRESENTABLE,
                   case
                       when c.HIDDEN_COLUMN is null or c.HIDDEN_COLUMN <> 'NO' then 'hidden column'
                       when c.VIRTUAL_COLUMN <> 'NO' then 'virtual column'
                       when c.DATA_TYPE_OWNER is not null then 'user-defined data type'
                       when idc.COLUMN_NAME is null and ${DEFAULT_ON_NULL} = 'YES' then 'default on null'
                       when idc.COLUMN_NAME is not null and idc.GENERATION_TYPE <> 'BY DEFAULT'
                           then 'identity generation mode'
                       when idc.COLUMN_NAME is not null and ${DEFAULT_ON_NULL} <> 'YES'
                           then 'identity default-on-null mode'
                       when idc.COLUMN_NAME is not null and identity_sequence.SEQUENCE_NAME is null
                           then 'identity sequence metadata'
                       when idc.COLUMN_NAME is null and ${SEQUENCE_NAME} is not null
                               and column_sequence.SEQUENCE_NAME is null
                           then 'named sequence metadata'
                       when idc.COLUMN_NAME is null and ${SEQUENCE_NAME} is not null and (
                               column_sequence.CYCLE_FLAG <> 'N'
                               or column_sequence.ORDER_FLAG <> 'N'
                               or nvl(${COLUMN_SCALE_FLAG}, 'N') <> 'N'
                               or nvl(${COLUMN_EXTEND_FLAG}, 'N') <> 'N'
                               or nvl(${COLUMN_SHARDED_FLAG}, 'N') <> 'N'
                               or nvl(${COLUMN_SESSION_FLAG}, 'N') <> 'N'
                               or nvl(${COLUMN_KEEP_VALUE}, 'N') <> 'N'
                               or column_sequence.MIN_VALUE <> 1
                               or column_sequence.MAX_VALUE <> 9999999999999999999999999999)
                           then 'non-default named sequence options'
                       when idc.COLUMN_NAME is not null and (
                               identity_sequence.CYCLE_FLAG <> 'N'
                               or identity_sequence.ORDER_FLAG <> 'N'
                               or nvl(${SCALE_FLAG}, 'N') <> 'N'
                               or nvl(${EXTEND_FLAG}, 'N') <> 'N'
                               or nvl(${SHARDED_FLAG}, 'N') <> 'N'
                               or nvl(${SESSION_FLAG}, 'N') <> 'N'
                               or nvl(${KEEP_VALUE}, 'N') <> 'N'
                               or identity_sequence.MIN_VALUE <> case
                                   when identity_sequence.INCREMENT_BY > 0 then 1
                                   else -999999999999999999999999999 end
                               or identity_sequence.MAX_VALUE <> case
                                   when identity_sequence.INCREMENT_BY > 0
                                       then 9999999999999999999999999999
                                   else -1 end)
                           then 'non-default identity sequence options'
                       else null
                   end as UNSUPPORTED_COLUMN_REASON,
                   c.DATA_DEFAULT as COLUMN_DEFAULT,
                   c.DATA_DEFAULT as GENERATION_EXPRESSION,
                   ${SEQUENCE_NAME} as GENERATION_SEQUENCE_NAME,
                   case when ${SEQUENCE_NAME} is not null then 1 else
                       to_number(regexp_substr(idc.IDENTITY_OPTIONS,
                           'START WITH:[[:space:]]*([+-]?[0-9]+)', 1, 1, 'i', 1))
                   end as GENERATION_START,
                   coalesce(identity_sequence.INCREMENT_BY, column_sequence.INCREMENT_BY)
                       as GENERATION_INCREMENT,
                   coalesce(identity_sequence.CACHE_SIZE, column_sequence.CACHE_SIZE)
                       as GENERATION_CACHE,
                   cast(null as varchar2(128)) as COLUMN_CHARSET,
                   ${COLUMN_COLLATION} as COLUMN_COLLATION,
                   case when idc.COLUMN_NAME is null then 'false' else 'true' end as IS_IDENTITY,
                   case when pk.CONSTRAINT_NAME is null then 'false' else 'true' end as PRIMARY_KEY
            from ALL_TAB_COLS c
            left join ALL_COL_COMMENTS cc
                   on cc.OWNER = c.OWNER
                  and cc.TABLE_NAME = c.TABLE_NAME
                  and cc.COLUMN_NAME = c.COLUMN_NAME
            left join ALL_TAB_IDENTITY_COLS idc
                   on idc.OWNER = c.OWNER
                  and idc.TABLE_NAME = c.TABLE_NAME
                  and idc.COLUMN_NAME = c.COLUMN_NAME
            left join ALL_SEQUENCES identity_sequence
                   on identity_sequence.SEQUENCE_OWNER = idc.OWNER
                  and identity_sequence.SEQUENCE_NAME = idc.SEQUENCE_NAME
            left join ALL_SEQUENCES column_sequence
                   on column_sequence.SEQUENCE_OWNER = case
                          when instr(${SEQUENCE_NAME}, '.') = 0 then c.OWNER
                          else substr(${SEQUENCE_NAME}, 1, instr(${SEQUENCE_NAME}, '.') - 1) end
                  and column_sequence.SEQUENCE_NAME = case
                          when instr(${SEQUENCE_NAME}, '.') = 0 then ${SEQUENCE_NAME}
                          else substr(${SEQUENCE_NAME}, instr(${SEQUENCE_NAME}, '.') + 1) end
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
                    from ALL_TAB_COLS exact_column
                    where exact_column.OWNER = c.OWNER
                      and exact_column.TABLE_NAME = ?
                ) then ? else upper(?) end
              and not (
                  c.HIDDEN_COLUMN = 'YES'
                  and c.VIRTUAL_COLUMN = 'YES'
                  and exists (
                      select 1
                      from ALL_IND_COLUMNS hidden_index_column
                      where hidden_index_column.TABLE_OWNER = c.OWNER
                        and hidden_index_column.TABLE_NAME = c.TABLE_NAME
                        and hidden_index_column.COLUMN_NAME = c.COLUMN_NAME
                        and hidden_index_column.DESCEND = 'DESC'
                  )
              )
            """.replace("${TIME_MARKER}", TIME_MARKER)
               .replace("${OFFSET_TIME_MARKER}", OFFSET_TIME_MARKER)
               .replace("${COMMENT_ESCAPE}", COMMENT_ESCAPE);

    private static final String BASE_COLUMNS_SQL = columnsSql(
            "c.DEFAULT_ON_NULL",
            "identity_sequence.SCALE_FLAG",
            "identity_sequence.EXTEND_FLAG",
            "identity_sequence.SHARDED_FLAG",
            "identity_sequence.SESSION_FLAG",
            "identity_sequence.KEEP_VALUE",
            "case when c.COLLATION = 'USING_NLS_COMP' then null else c.COLLATION end");

    private static final String BASE_COLUMNS_SQL_12C = columnsSql(
            "case when idc.COLUMN_NAME is null then 'NO' else 'YES' end",
            "'N'", "'N'", "'N'", "'N'", "'N'",
            "cast(null as varchar2(128))");

    private static final String BASE_INDEXES_SQL = """
            select i.INDEX_NAME,
                   ic.COLUMN_NAME,
                   index_expression.COLUMN_EXPRESSION as INDEX_EXPRESSION,
                   case when i.UNIQUENESS = 'UNIQUE' then 'true' else 'false' end as UNIQUE_INDEX,
                   case when i.PARTITIONED = 'NO'
                              and i.VISIBILITY = 'VISIBLE'
                              and i.STATUS = 'VALID'
                              and i.INDEX_TYPE in ('NORMAL', 'FUNCTION-BASED NORMAL')
                              and i.COMPRESSION = 'DISABLED'
                              and ic.DESCEND in ('ASC', 'DESC')
                        then 'true' else 'false' end as INDEX_REPRESENTABLE,
                   ic.DESCEND as INDEX_DIRECTION,
                   case
                       when i.PARTITIONED <> 'NO' then 'partitioned index'
                       when i.VISIBILITY <> 'VISIBLE' then 'invisible index'
                       when i.STATUS <> 'VALID' then 'index is not valid'
                       when i.INDEX_TYPE not in ('NORMAL', 'FUNCTION-BASED NORMAL') then 'non-normal index'
                       when i.COMPRESSION <> 'DISABLED' then 'compressed index'
                       when ic.DESCEND not in ('ASC', 'DESC') then 'unsupported key direction'
                       else 'function-based key part'
                   end as UNSUPPORTED_INDEX_REASON
            from ALL_INDEXES i
            join ALL_IND_COLUMNS ic
              on ic.INDEX_OWNER = i.OWNER
             and ic.INDEX_NAME = i.INDEX_NAME
             and ic.TABLE_OWNER = i.TABLE_OWNER
             and ic.TABLE_NAME = i.TABLE_NAME
            left join ALL_IND_EXPRESSIONS index_expression
              on index_expression.INDEX_OWNER = i.OWNER
             and index_expression.INDEX_NAME = i.INDEX_NAME
             and index_expression.TABLE_OWNER = i.TABLE_OWNER
             and index_expression.TABLE_NAME = i.TABLE_NAME
             and index_expression.COLUMN_POSITION = ic.COLUMN_POSITION
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
                    and ac.CONSTRAINT_TYPE in ('P', 'U')
              )
            """;

    private static final String BASE_FOREIGN_KEYS_SQL = """
            select ac.OWNER as TABLE_SCHEMA,
                   ac.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   acc.COLUMN_NAME,
                   rc.OWNER as REFERENCED_TABLE_SCHEMA,
                   rc.TABLE_NAME as REFERENCED_TABLE_NAME,
                   rcc.COLUMN_NAME as REFERENCED_COLUMN_NAME,
                   case ac.DELETE_RULE
                       when 'CASCADE' then 'CASCADE'
                       when 'SET NULL' then 'SET_NULL'
                       else 'NO_ACTION'
                   end as ON_DELETE,
                   'NO_ACTION' as ON_UPDATE,
                   case when ac.STATUS = 'ENABLED'
                              and ac.DEFERRABLE = 'NOT DEFERRABLE'
                              and ac.VALIDATED = 'VALIDATED'
                              and ac.RELY is null
                              and ac.INVALID is null
                              and ac.DELETE_RULE in ('CASCADE', 'SET NULL', 'NO ACTION')
                        then 'true' else 'false' end as CONSTRAINT_REPRESENTABLE
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

    private static final String BASE_TABLE_SQL = """
            select tc.COMMENTS as TABLE_COMMENT,
                   case when t.STATUS = 'VALID'
                              and t.TEMPORARY = 'N'
                              and t.SECONDARY = 'N'
                              and t.NESTED = 'NO'
                              and t.PARTITIONED = 'NO'
                              and t.IOT_TYPE is null
                              and t.CLUSTER_NAME is null
                              and not exists (
                                  select 1 from ALL_EXTERNAL_TABLES external_table
                                  where external_table.OWNER = t.OWNER
                                    and external_table.TABLE_NAME = t.TABLE_NAME)
                              and not exists (
                                  select 1 from ALL_TAB_COLS hidden_column
                                  where hidden_column.OWNER = t.OWNER
                                    and hidden_column.TABLE_NAME = t.TABLE_NAME
                                    and hidden_column.HIDDEN_COLUMN = 'YES'
                                    and not (
                                        hidden_column.VIRTUAL_COLUMN = 'YES'
                                        and exists (
                                            select 1
                                            from ALL_IND_COLUMNS hidden_index_column
                                            where hidden_index_column.TABLE_OWNER = hidden_column.OWNER
                                              and hidden_index_column.TABLE_NAME = hidden_column.TABLE_NAME
                                              and hidden_index_column.COLUMN_NAME = hidden_column.COLUMN_NAME
                                              and hidden_index_column.DESCEND = 'DESC'
                                        )
                                    ))
                        then 'true' else 'false' end as TABLE_REPRESENTABLE
            from ALL_TABLES t
            left join ALL_TAB_COMMENTS tc
              on tc.OWNER = t.OWNER and tc.TABLE_NAME = t.TABLE_NAME and tc.TABLE_TYPE = 'TABLE'
            where t.TABLE_NAME = case
                when exists (
                    select 1 from ALL_TABLES exact_table
                    where exact_table.OWNER = t.OWNER and exact_table.TABLE_NAME = ?
                ) then ? else upper(?) end
            """;

    private static final String BASE_PRIMARY_KEY_SQL = constraintColumnsSql("P");
    private static final String BASE_UNIQUE_SQL = constraintColumnsSql("U");

    private static final String BASE_CHECKS_SQL = """
            select ac.CONSTRAINT_NAME,
                   ac.SEARCH_CONDITION_VC as CHECK_EXPRESSION,
                   case when ac.STATUS = 'ENABLED'
                              and ac.DEFERRABLE = 'NOT DEFERRABLE'
                              and ac.VALIDATED = 'VALIDATED'
                              and ac.RELY is null
                              and ac.INVALID is null
                              and ac.SEARCH_CONDITION_VC is not null
                              and length(ac.SEARCH_CONDITION_VC) < 4000
                        then 'true' else 'false' end as CHECK_REPRESENTABLE
            from ALL_CONSTRAINTS ac
            where ac.CONSTRAINT_TYPE = 'C'
              and not (ac.GENERATED = 'GENERATED NAME'
                  and regexp_like(ac.SEARCH_CONDITION_VC,
                      '^[[:space:]]*"[^"]+"[[:space:]]+IS[[:space:]]+NOT[[:space:]]+NULL[[:space:]]*$', 'i'))
              and ac.TABLE_NAME = case
                  when exists (
                      select 1 from ALL_TABLES exact_table
                      where exact_table.OWNER = ac.OWNER and exact_table.TABLE_NAME = ?
                  ) then ? else upper(?) end
            """;

    private static String constraintColumnsSql(String constraintType) {
        return """
                select ac.CONSTRAINT_NAME,
                       acc.COLUMN_NAME,
                       case when ac.STATUS = 'ENABLED'
                                  and ac.DEFERRABLE = 'NOT DEFERRABLE'
                                  and ac.VALIDATED = 'VALIDATED'
                                  and ac.RELY is null
                                  and ac.INVALID is null
                                  and i.INDEX_NAME = ac.CONSTRAINT_NAME
                                  and i.UNIQUENESS = 'UNIQUE'
                                  and i.PARTITIONED = 'NO'
                                  and i.VISIBILITY = 'VISIBLE'
                                  and i.STATUS = 'VALID'
                                  and i.INDEX_TYPE = 'NORMAL'
                                  and i.COMPRESSION = 'DISABLED'
                                  and ic.COLUMN_NAME = acc.COLUMN_NAME
                                  and ic.COLUMN_POSITION = acc.POSITION
                                  and ic.DESCEND = 'ASC'
                                  and not exists (
                                      select 1 from ALL_IND_EXPRESSIONS ie
                                      where ie.INDEX_OWNER = i.OWNER
                                        and ie.INDEX_NAME = i.INDEX_NAME
                                        and ie.TABLE_OWNER = i.TABLE_OWNER
                                        and ie.TABLE_NAME = i.TABLE_NAME
                                  )
                                  and (select count(*) from ALL_IND_COLUMNS counted_index_column
                                       where counted_index_column.INDEX_OWNER = i.OWNER
                                         and counted_index_column.INDEX_NAME = i.INDEX_NAME
                                         and counted_index_column.TABLE_OWNER = i.TABLE_OWNER
                                         and counted_index_column.TABLE_NAME = i.TABLE_NAME)
                                      = (select count(*) from ALL_CONS_COLUMNS counted_constraint_column
                                         where counted_constraint_column.OWNER = ac.OWNER
                                           and counted_constraint_column.CONSTRAINT_NAME = ac.CONSTRAINT_NAME)
                            then 'true' else 'false' end as CONSTRAINT_REPRESENTABLE
                from ALL_CONSTRAINTS ac
                join ALL_CONS_COLUMNS acc
                  on acc.OWNER = ac.OWNER and acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
                left join ALL_INDEXES i
                  on i.OWNER = ac.INDEX_OWNER and i.INDEX_NAME = ac.INDEX_NAME
                left join ALL_IND_COLUMNS ic
                  on ic.INDEX_OWNER = i.OWNER
                 and ic.INDEX_NAME = i.INDEX_NAME
                 and ic.TABLE_OWNER = i.TABLE_OWNER
                 and ic.TABLE_NAME = i.TABLE_NAME
                 and ic.COLUMN_POSITION = acc.POSITION
                where ac.CONSTRAINT_TYPE = '__CONSTRAINT_TYPE__'
                  and ac.TABLE_NAME = case
                      when exists (
                          select 1 from ALL_TABLES exact_table
                          where exact_table.OWNER = ac.OWNER and exact_table.TABLE_NAME = ?
                      ) then ? else upper(?) end
                """.replace("__CONSTRAINT_TYPE__", constraintType);
    }

    private final InformationSchemaFormMetadataReader delegate;

    private OracleReactiveFormMetadataReader(ReactiveSqlExecutor executor) {
        this.delegate = new InformationSchemaFormMetadataReader(Objects.requireNonNull(
                executor, "reactive sql executor must not be null"), queries());
    }

    static OracleReactiveFormMetadataReader create(ReactiveSqlExecutor executor) {
        return new OracleReactiveFormMetadataReader(executor);
    }

    static InformationSchemaFormMetadataReader.Queries queries() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                OracleReactiveFormMetadataReader::columnQuery,
                OracleReactiveFormMetadataReader::indexQuery,
                OracleReactiveFormMetadataReader::foreignKeyQuery,
                OracleReactiveFormMetadataReader::logicalType,
                OracleReactiveFormMetadataReader::tableQuery,
                OracleReactiveFormMetadataReader::primaryKeyQuery,
                OracleReactiveFormMetadataReader::uniqueConstraintQuery,
                OracleReactiveFormMetadataReader::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);
    }

    static InformationSchemaFormMetadataReader.Queries queries12c() {
        return InformationSchemaFormMetadataReader.Queries.complete(
                OracleReactiveFormMetadataReader::columnQuery12c,
                OracleReactiveFormMetadataReader::indexQuery,
                OracleReactiveFormMetadataReader::foreignKeyQuery,
                OracleReactiveFormMetadataReader::logicalType,
                OracleReactiveFormMetadataReader::tableQuery,
                OracleReactiveFormMetadataReader::primaryKeyQuery,
                OracleReactiveFormMetadataReader::uniqueConstraintQuery,
                OracleReactiveFormMetadataReader::checkConstraintQuery,
                InformationSchemaFormMetadataReader.SnapshotDialect.ORACLE);
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
        return columnQuery(BASE_COLUMNS_SQL, schema, table);
    }

    private static SqlRequest columnQuery12c(String schema, String table) {
        return columnQuery(BASE_COLUMNS_SQL_12C, schema, table);
    }

    private static SqlRequest columnQuery(String baseSql, String schema, String table) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            String sql = baseSql
                    + " and c.OWNER = sys_context('USERENV', 'CURRENT_SCHEMA')"
                    + " order by c.COLUMN_ID";
            return new SqlRequest(sql, tableParameters(safeTable));
        }
        String sql = baseSql + " and c.OWNER = " + ownerExpression() + " order by c.COLUMN_ID";
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

    private static SqlRequest tableQuery(String schema, String table) {
        return completeQuery(BASE_TABLE_SQL, "t.OWNER", schema, table, "");
    }

    private static SqlRequest primaryKeyQuery(String schema, String table) {
        return completeQuery(BASE_PRIMARY_KEY_SQL, "ac.OWNER", schema, table,
                             " order by acc.POSITION");
    }

    private static SqlRequest uniqueConstraintQuery(String schema, String table) {
        return completeQuery(BASE_UNIQUE_SQL, "ac.OWNER", schema, table,
                             " order by ac.CONSTRAINT_NAME, acc.POSITION");
    }

    private static SqlRequest checkConstraintQuery(String schema, String table) {
        return completeQuery(BASE_CHECKS_SQL, "ac.OWNER", schema, table,
                             " order by ac.CONSTRAINT_NAME");
    }

    private static SqlRequest completeQuery(String base,
                                            String ownerColumn,
                                            String schema,
                                            String table,
                                            String orderBy) {
        String safeTable = InformationSchemaFormMetadataReader.requireText(table, "table");
        if (schema == null || schema.isBlank()) {
            return new SqlRequest(base + " and " + ownerColumn
                    + " = sys_context('USERENV', 'CURRENT_SCHEMA')" + orderBy,
                                  tableParameters(safeTable));
        }
        return new SqlRequest(base + " and " + ownerColumn + " = " + ownerExpression() + orderBy,
                              tableAndOwnerParameters(safeTable, schema));
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
        return DatabaseTypes.logicalDeclaration(dataType, "oracle");
    }

    private static String columnsSql(String defaultOnNull,
                                     String scaleFlag,
                                     String extendFlag,
                                     String shardedFlag,
                                     String sessionFlag,
                                     String keepValue,
                                     String columnCollation) {
        return BASE_COLUMNS_SQL_TEMPLATE
                .replace("${STORAGE_COMMENT}", STORAGE_COMMENT_EXPRESSION)
                .replace("${SEQUENCE_NAME}", SEQUENCE_NAME_EXPRESSION)
                .replace("${DEFAULT_ON_NULL}", defaultOnNull)
                .replace("${SCALE_FLAG}", scaleFlag)
                .replace("${EXTEND_FLAG}", extendFlag)
                .replace("${SHARDED_FLAG}", shardedFlag)
                .replace("${SESSION_FLAG}", sessionFlag)
                .replace("${KEEP_VALUE}", keepValue)
                .replace("${COLUMN_SCALE_FLAG}", scaleFlag.replace("identity_sequence", "column_sequence"))
                .replace("${COLUMN_EXTEND_FLAG}", extendFlag.replace("identity_sequence", "column_sequence"))
                .replace("${COLUMN_SHARDED_FLAG}", shardedFlag.replace("identity_sequence", "column_sequence"))
                .replace("${COLUMN_SESSION_FLAG}", sessionFlag.replace("identity_sequence", "column_sequence"))
                .replace("${COLUMN_KEEP_VALUE}", keepValue.replace("identity_sequence", "column_sequence"))
                .replace("${COLUMN_COLLATION}", columnCollation);
    }
}
