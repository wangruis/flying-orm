package com.flying.orm.testkit.dialect;

import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.List;

/**
 * 内置方言的 SQL 合同清单。后面跑真实数据库时，也用这份清单做第一层对齐。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public final class DialectSqlContracts {

    private DialectSqlContracts() {
    }

    public static List<DialectSqlContractCase> builtIns() {
        return List.of(
                new DialectSqlContractCase(
                        "h2",
                        RdbDialect.h2(),
                        "create table Users (id BIGINT primary key, name VARCHAR, enabled BOOLEAN, created_at TIMESTAMP)",
                        "select id, name, enabled, created_at from Users where name = ? order by id asc limit ? offset ?",
                        "merge into Users (id, name, enabled, created_at) key (id) values (?, ?, ?, ?)"),
                new DialectSqlContractCase(
                        "mysql",
                        RdbDialect.mysql(),
                        "create table `Users` (`id` BIGINT primary key, `name` VARCHAR(255), `enabled` BOOLEAN, `created_at` DATETIME)",
                        "select `id`, `name`, `enabled`, `created_at` from `Users` where `name` = ? order by `id` asc limit ? offset ?",
                        "insert into `Users` (`id`, `name`, `enabled`, `created_at`) values (?, ?, ?, ?) on duplicate key update `name` = values(`name`), `enabled` = values(`enabled`), `created_at` = values(`created_at`)"),
                new DialectSqlContractCase(
                        "postgresql",
                        RdbDialect.postgresql(),
                        "create table \"Users\" (\"id\" BIGINT primary key, \"name\" VARCHAR(255), \"enabled\" BOOLEAN, \"created_at\" TIMESTAMP)",
                        "select \"id\", \"name\", \"enabled\", \"created_at\" from \"Users\" where \"name\" = ? order by \"id\" asc limit ? offset ?",
                        "insert into \"Users\" (\"id\", \"name\", \"enabled\", \"created_at\") values (?, ?, ?, ?) on conflict (\"id\") do update set \"name\" = excluded.\"name\", \"enabled\" = excluded.\"enabled\", \"created_at\" = excluded.\"created_at\""),
                new DialectSqlContractCase(
                        "oracle",
                        RdbDialect.oracle(),
                        "create table \"Users\" (\"id\" NUMBER(19) primary key, \"name\" VARCHAR2(255), \"enabled\" NUMBER(1), \"created_at\" TIMESTAMP)",
                        "select \"id\", \"name\", \"enabled\", \"created_at\" from \"Users\" where \"name\" = ? order by \"id\" asc offset ? rows fetch next ? rows only",
                        "merge into \"Users\" target using (select ? as \"id\", ? as \"name\", ? as \"enabled\", ? as \"created_at\" from dual) source on (target.\"id\" = source.\"id\") when matched then update set target.\"name\" = source.\"name\", target.\"enabled\" = source.\"enabled\", target.\"created_at\" = source.\"created_at\" when not matched then insert (\"id\", \"name\", \"enabled\", \"created_at\") values (source.\"id\", source.\"name\", source.\"enabled\", source.\"created_at\")"),
                new DialectSqlContractCase(
                        "sqlserver",
                        RdbDialect.sqlServer(),
                        "create table [Users] ([id] BIGINT not null primary key, [name] NVARCHAR(255) null, "
                                + "[enabled] BIT null, [created_at] DATETIME2 null)",
                        "select [id], [name], [enabled], [created_at] from [Users] where [name] = ? order by [id] asc offset ? rows fetch next ? rows only",
                        "merge into [Users] with (holdlock) as target using (values (?, ?, ?, ?)) as source ([id], [name], [enabled], [created_at]) on target.[id] = source.[id] when matched then update set target.[name] = source.[name], target.[enabled] = source.[enabled], target.[created_at] = source.[created_at] when not matched then insert ([id], [name], [enabled], [created_at]) values (source.[id], source.[name], source.[enabled], source.[created_at]);"));
    }
}
