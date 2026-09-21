package com.flying.orm.core.sql.render;

/**
 * 关系表业务条件命名包工厂，用于把常见 in / not-in 关系条件打包注册。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
public final class RelationTermPackage {

    private RelationTermPackage() {
    }

    /**
     * 创建关系表业务条件命名包。
     *
     * @param packageName         命名包名称
     * @param relationTable       关系表名
     * @param relationAlias       关系表别名
     * @param relationKeyColumn   关系表中指向外层字段的列名
     * @param relationValueColumn 关系表中接收 term 值的列名
     * @param existsTermId        关系存在 term id
     * @param notExistsTermId     关系不存在 term id
     * @return 关系表业务条件命名包
     */
    public static SqlTermPackage of(String packageName,
                                    String relationTable,
                                    String relationAlias,
                                    String relationKeyColumn,
                                    String relationValueColumn,
                                    String existsTermId,
                                    String notExistsTermId) {
        return SqlTermPackage.of(packageName,
                                 SqlTermHandler.relationExists(existsTermId,
                                                               relationTable,
                                                               relationAlias,
                                                               relationKeyColumn,
                                                               relationValueColumn),
                                 SqlTermHandler.relationNotExists(notExistsTermId,
                                                                  relationTable,
                                                                  relationAlias,
                                                                  relationKeyColumn,
                                                                  relationValueColumn));
    }
}
