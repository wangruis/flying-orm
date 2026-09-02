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

    private static final String BASE_COLUMNS_SQL = """
            select c.COLUMN_NAME,
                   SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') as RESOLUTION_SCHEMA,
                   case
                       when NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${COMMENT_ESCAPE}')), 'NLS_SORT=BINARY')
                            = NLSSORT('${COMMENT_ESCAPE}', 'NLS_SORT=BINARY') then c.DATA_TYPE
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 18
                            and NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${TIME_MARKER}', 'NLS_SORT=BINARY') then 'TIME'
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 32
                            and NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${OFFSET_TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${OFFSET_TIME_MARKER}', 'NLS_SORT=BINARY') then 'OFFSET_TIME'
                       else c.DATA_TYPE
                   end as DATA_TYPE,
                   c.CHAR_LENGTH as CHARACTER_MAXIMUM_LENGTH,
                   c.DATA_PRECISION as NUMERIC_PRECISION,
                   c.DATA_SCALE as NUMERIC_SCALE,
                   case when c.DATA_TYPE like 'TIMESTAMP%' then c.DATA_SCALE end as TEMPORAL_PRECISION,
                   case
                       when NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${COMMENT_ESCAPE}')), 'NLS_SORT=BINARY')
                            = NLSSORT('${COMMENT_ESCAPE}', 'NLS_SORT=BINARY')
                           then substr(cc.COMMENTS, length('${COMMENT_ESCAPE}') + 1)
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 18
                            and NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${TIME_MARKER}', 'NLS_SORT=BINARY')
                           then nullif(substr(cc.COMMENTS, length('${TIME_MARKER}') + 1), '')
                       when c.DATA_TYPE = 'VARCHAR2' and c.CHAR_LENGTH = 32
                            and NLSSORT(SUBSTR(cc.COMMENTS, 1, LENGTH('${OFFSET_TIME_MARKER}')), 'NLS_SORT=BINARY')
                                = NLSSORT('${OFFSET_TIME_MARKER}', 'NLS_SORT=BINARY')
                           then nullif(substr(cc.COMMENTS, length('${OFFSET_TIME_MARKER}') + 1), '')
                       else cc.COMMENTS
                   end as REMARKS,
                   c.NULLABLE,
                   c.DATA_DEFAULT as GENERATION_EXPRESSION,
                   case when idc.COLUMN_NAME is null then 'false' else 'true' end as IS_IDENTITY,
                   case when pk.CONSTRAINT_NAME is null then 'false' else 'true' end as PRIMARY_KEY
            from ALL_TAB_COLUMNS c
            left join ALL_COL_COMMENTS cc
                   on cc.OWNER = c.OWNER
                  and cc.TABLE_NAME = c.TABLE_NAME
                  and cc.COLUMN_NAME = c.COLUMN_NAME
            left join ALL_TAB_IDENTITY_COLS idc
                   on idc.OWNER = c.OWNER
                  and idc.TABLE_NAME = c.TABLE_NAME
                  and idc.COLUMN_NAME = c.COLUMN_NAME
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
            """.replace("${TIME_MARKER}", TIME_MARKER)
               .replace("${OFFSET_TIME_MARKER}", OFFSET_TIME_MARKER)
               .replace("${COMMENT_ESCAPE}", COMMENT_ESCAPE);

    private static final String BASE_INDEXES_SQL = """
            select i.INDEX_NAME,
                   ic.COLUMN_NAME,
                   case when i.UNIQUENESS = 'UNIQUE' then 'true' else 'false' end as UNIQUE_INDEX,
                   case when i.PARTITIONED = 'NO'
                              and i.VISIBILITY = 'VISIBLE'
                              and i.STATUS = 'VALID'
                              and i.INDEX_TYPE = 'NORMAL'
                              and ic.DESCEND = 'ASC'
                              and not exists (
                                  select 1
                                  from ALL_IND_EXPRESSIONS ie
                                  where ie.INDEX_OWNER = i.OWNER
                                    and ie.INDEX_NAME = i.INDEX_NAME
                                    and ie.TABLE_OWNER = i.TABLE_OWNER
                                    and ie.TABLE_NAME = i.TABLE_NAME
                                    and ie.COLUMN_POSITION = ic.COLUMN_POSITION
                              )
                        then 'true' else 'false' end as INDEX_REPRESENTABLE,
                   case
                       when i.PARTITIONED <> 'NO' then 'partitioned index'
                       when i.VISIBILITY <> 'VISIBLE' then 'invisible index'
                       when i.STATUS <> 'VALID' then 'index is not valid'
                       when i.INDEX_TYPE <> 'NORMAL' then 'non-normal index'
                       when ic.DESCEND <> 'ASC' then 'descending key part'
                       else 'function-based key part'
                   end as UNSUPPORTED_INDEX_REASON
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
            select ac.OWNER as TABLE_SCHEMA,
                   ac.CONSTRAINT_NAME as FOREIGN_KEY_NAME,
                   acc.COLUMN_NAME,
                   rc.OWNER as REFERENCED_TABLE_SCHEMA,
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
        return DatabaseTypes.logicalDeclaration(dataType, "oracle");
    }
}
