package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;

import java.util.List;
import java.util.function.Supplier;

/**
 * 结构操作的后端 DDL 扩展；只生成请求，不执行 SQL 或改变结构事实。
 *
 * <p>回调必须确定、无状态且可并发调用。完全替换操作时，由回调负责该操作所需的序列、索引和注释等
 * 依赖语句；调用默认渲染后必须保留其必要依赖。默认渲染器在当前回调内惰性计算一次，不可保存或异步调用。
 * 返回非空请求列表，列表顺序即执行顺序；渲染异常原样向调用方传播，不转换为人工处理计划。
 * 这是可信后端 SQL 扩展，不接受前端 SQL。普通表单迁移中的建表、列变化、索引和外键也使用此入口；
 * 整表单计划替换使用 {@link FormSchemaDdlRenderer}。</p>
 *
 * @author wangr
 * @version v4.1.2
 */
@FunctionalInterface
public interface SchemaOperationRenderer {
    /** 根据结构化事实生成 DDL，未定制的操作可直接返回 defaultRenderer.get()。 */
    List<SqlRequest> render(SchemaOperation operation, Supplier<List<SqlRequest>> defaultRenderer);
}
