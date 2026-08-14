package com.flying.orm.benchmark.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

/** 保存一个数据库目标的连接信息和少量方言相关 SQL。凭据不参与 toString，也不会进入报告。 */
final class JdbcPerformanceTarget {

    private final String key;
    private final String name;
    private final String url;
    private final String user;
    private final String password;
    private final String table;
    private final String quotedTable;

    private JdbcPerformanceTarget(String key, String name, String url, String user, String password,
                                  String table, String quotedTable) {
        this.key = key;
        this.name = name;
        this.url = url;
        this.user = user;
        this.password = password;
        this.table = table;
        this.quotedTable = quotedTable;
    }

    static JdbcPerformanceTarget mysql(JdbcPerformanceArguments args) {
        return new JdbcPerformanceTarget("mysql", "MySQL", args.mysqlUrl, args.mysqlUser, args.mysqlPassword,
                                         "FLYING_ORM_PERFORMANCE_MYSQL", "`FLYING_ORM_PERFORMANCE_MYSQL`");
    }

    static JdbcPerformanceTarget postgresql(JdbcPerformanceArguments args) {
        return new JdbcPerformanceTarget("postgresql", "PostgreSQL", args.postgresqlUrl,
                                         args.postgresqlUser, args.postgresqlPassword,
                                         "FLYING_ORM_PERFORMANCE_PG", "\"FLYING_ORM_PERFORMANCE_PG\"");
    }

    HikariDataSource openPool(int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setPoolName("flying-orm-jdbc-" + key);
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);
        config.setMinimumIdle(Math.min(2, poolSize));
        config.setConnectionTimeout(30_000L);
        config.setValidationTimeout(5_000L);
        config.setInitializationFailTimeout(30_000L);
        config.setAutoCommit(true);
        return new HikariDataSource(config);
    }

    DynamicForm form() {
        return DynamicForm.builder("flyingOrmPerformance", table)
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .addField(DynamicField.of("value", "BIGINT"))
                          .build();
    }

    String name() { return name; }

    String dropSql() {
        return "drop table if exists " + quotedTable;
    }

    String createSql() {
        return "create table " + quotedTable
                + " (id bigint primary key, name varchar(128) not null, value bigint not null)";
    }

    String querySql() {
        return "select id, name, value from " + quotedTable + " where id = ?";
    }

    String updateSql() {
        return "update " + quotedTable + " set value = value + 1 where id = ?";
    }
}
