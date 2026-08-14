package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 驱动绑定标记只在创建 Statement 前改写。Oracle 官方驱动支持匿名问号，SQL Server 驱动使用 @P0 开始的标记。
 */
class R2dbcBindMarkersContractTest {

    @Test
    void keepsOracleQuestionMarkersAndNumbersSqlServerMarkersFromZero() {
        R2dbcBindMarkers oracle = R2dbcBindMarkers.from(connectionFactory("Oracle Database"));
        R2dbcBindMarkers sqlServer = R2dbcBindMarkers.from(connectionFactory("Microsoft SQL Server"));

        assertEquals("select * from users where id = ? and name = ?",
                     oracle.adapt("select * from users where id = ? and name = ?",
                                  2,
                                  SqlBindMarkerStyle.CANONICAL));
        assertEquals("select * from users where id = @P0 and name = @P1",
                     sqlServer.adapt("select * from users where id = ? and name = ?",
                                     2,
                                     SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.adapt("select * from users where id = ?",
                                        0,
                                        SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                     () -> oracle.adapt("select * from users", 1, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void keepsQuestionMarksInsideParameterlessPostgresqlDdlText() {
        R2dbcBindMarkers postgresql = R2dbcBindMarkers.from(connectionFactory("PostgreSQL"));
        String ddl = "comment on column users.name is 'what?'";

        assertEquals(ddl, postgresql.adapt(ddl, 0, SqlBindMarkerStyle.CANONICAL));
        assertEquals("select '?' as marker_text from users where id = $1 /* really? */",
                     postgresql.adapt("select '?' as marker_text from users where id = ? /* really? */",
                                      1,
                                      SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                     () -> postgresql.adapt("select * from users where id = ?",
                                            0,
                                            SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void keepsQuestionMarksInsideDatabaseSpecificQuotedTextAndNestedComments() {
        R2dbcBindMarkers postgresql = R2dbcBindMarkers.from(connectionFactory("PostgreSQL"));
        R2dbcBindMarkers sqlServer = R2dbcBindMarkers.from(connectionFactory("Microsoft SQL Server"));

        assertEquals("select $body$begin return '?'; end$body$, $1 /* outer /* inner */ ? */",
                     postgresql.adapt(
                             "select $body$begin return '?'; end$body$, ? /* outer /* inner */ ? */",
                             1,
                             SqlBindMarkerStyle.CANONICAL));
        assertEquals("select [?] from [orders?] where id = @P0 /* outer /* inner */ ? */",
                     sqlServer.adapt(
                             "select [?] from [orders?] where id = ? /* outer /* inner */ ? */",
                                     1,
                                     SqlBindMarkerStyle.CANONICAL));
    }

    /** MySQL 反引号标识符中的问号只是名称内容，不能占用真实参数位置。 */
    @Test
    void keepsQuestionMarksInsideMySqlBacktickIdentifiers() {
        R2dbcBindMarkers mysql = R2dbcBindMarkers.from(connectionFactory("MySQL"));
        String sql = "select `question?`, `escaped``?` from `audit?log` where id = ?";

        assertEquals(sql, mysql.adapt(sql, 1, SqlBindMarkerStyle.CANONICAL));
    }

    /** Oracle 替代引号允许正文直接含单引号，正文里的问号不能占用真实参数位置。 */
    @Test
    void keepsQuestionMarksInsideOracleAlternativeQuotedText() {
        R2dbcBindMarkers oracle = R2dbcBindMarkers.from(connectionFactory("Oracle Database"));
        String sql = "select q'[Mary's ?]', Q'!It's still ?!', nq'<It's national ?>' "
                + "from dual where id = ?";

        assertEquals(sql, oracle.adapt(sql, 1, SqlBindMarkerStyle.CANONICAL));
    }

    @Test
    void rejectsUnclosedCanonicalSqlBeforeItReachesTheDriver() {
        R2dbcBindMarkers mysql = R2dbcBindMarkers.from(connectionFactory("MySQL"));
        R2dbcBindMarkers postgresql = R2dbcBindMarkers.from(connectionFactory("PostgreSQL"));

        assertThrows(IllegalArgumentException.class,
                     () -> mysql.adapt("select * from users where name = '?", 0, SqlBindMarkerStyle.CANONICAL));
        assertThrows(IllegalArgumentException.class,
                     () -> postgresql.adapt("select * from users /* ?", 0, SqlBindMarkerStyle.CANONICAL));
    }

    private static ConnectionFactory connectionFactory(String name) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new UnsupportedOperationException("contract test does not open connections");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> name;
            }
        };
    }
}
