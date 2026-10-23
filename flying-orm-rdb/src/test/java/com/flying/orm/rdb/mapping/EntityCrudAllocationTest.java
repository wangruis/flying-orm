package com.flying.orm.rdb.mapping;

import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.schema.FormSchemaSqlRenderer;
import com.flying.orm.rdb.schema.RelationalSchemaSqlRenderer;
import com.flying.orm.rdb.schema.SchemaOperation;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class EntityCrudAllocationTest {
    private static volatile Object sink;

    // Opt-in budgets: HotSpot JDK 21, -Xms128m -Xmx128m, compressed oops/class pointers, 8-byte alignment.
    // Conversion semantics remain covered by ValueCodecRegistryPriorityTest without this switch.
    @Test
    @EnabledIfSystemProperty(named = "flying.orm.scalarAllocationChecks", matches = "true")
    void integerConversionsAvoidIntermediateDecimals() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        Long value = 4096L;
        double bytes = allocated(() -> sink = codecs.read(value, java.math.BigInteger.class));
        assertEquals(java.math.BigInteger.valueOf(value), sink);
        System.out.println("integral BigInteger conversion B/value=" + bytes);
        assertTrue(bytes < 80, "integral conversion must not allocate an intermediate decimal: " + bytes);
    }

    @Test
    @EnabledIfSystemProperty(named = "flying.orm.scalarAllocationChecks", matches = "true")
    void wideningIntegerConversionsOnlyAllocateTheirResult() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        Integer value = 4096;
        double bytes = allocated(() -> sink = codecs.read(value, Long.class));
        assertEquals(4096L, sink);
        System.out.println("integral Long conversion B/value=" + bytes);
        assertTrue(bytes < 40, "integral widening must not allocate an intermediate decimal: " + bytes);
    }

    @Test
    @EnabledIfSystemProperty(named = "flying.orm.scalarAllocationChecks", matches = "true")
    void integerBooleanConversionsDoNotAllocateDecimals() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        Long value = 4096L;
        double bytes = allocated(() -> sink = codecs.read(value, Boolean.class));
        assertEquals(true, sink);
        System.out.println("integral Boolean conversion B/value=" + bytes);
        assertTrue(bytes < 16, "integral truth conversion must not allocate a decimal: " + bytes);
    }

    @Test
    void batchUpdateSnapshotsDoNotResizeTheKnownFieldMap() {
        Map<String, Object> values = Map.of("name", "Ada", "status", 1);
        com.flying.orm.core.condition.ConditionGroup where =
                com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 7L).build();
        double bytes = allocated(() -> sink = new com.flying.orm.rdb.form.BatchUpdate(values, where));
        assertEquals(values, ((com.flying.orm.rdb.form.BatchUpdate) sink).values());
        System.out.println("two-field batch update snapshot B/row=" + bytes);
        assertTrue(bytes < 344, "known-size batch values must not resize their map: " + bytes);
    }

    @Test
    void completeBeanBindingsDoNotCopyAlreadyExactArrays() throws ReflectiveOperationException {
        MappingPlan<Bean> mapper = MappingPlan.of(Bean.class);
        java.lang.reflect.Field field = MappingPlan.class.getDeclaredField("beanWriter");
        field.setAccessible(true);
        Object writer = field.get(mapper);
        java.lang.reflect.Method bind = writer.getClass().getDeclaredMethod("bind", DynamicRow.class);
        bind.setAccessible(true);
        DynamicRow columns = DynamicRow.copyOf(Map.of("name", "Ada"));
        double bytes = allocated(() -> {
            try {
                sink = bind.invoke(writer, columns);
            } catch (ReflectiveOperationException error) {
                throw new AssertionError(error);
            }
        });
        assertEquals("Ada", mapper.map(columns).name);
        System.out.println("complete bean column binding B/layout=" + bytes);
        assertTrue(bytes < 112, "full bean bindings must reuse exact arrays: " + bytes);
    }

    @Test
    void jdbcTemplatePreparationDoesNotCompileAnIntermediateBackend() {
        SqlTemplateRegistry.Builder builder = SqlTemplateRegistry.builder();
        for (int index = 0; index < 4; index++) {
            builder.register(SqlTemplate.query("people" + index,
                    "select name from people where id = :id and status = :status", java.util.Set.of()));
        }
        SqlTemplateRegistry templates = builder.build();
        RdbDialect dialect = RdbDialect.postgresql();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        double reactive = allocated(() -> sink = SqlTemplateEngine.create(templates, dialect, codecs));
        double jdbc = allocated(() -> sink = SqlTemplateEngine.createJdbc(templates, dialect, codecs));
        SqlTemplateEngine engine = (SqlTemplateEngine) sink;
        assertEquals("select name from people where id = ? and status = ?",
                engine.render("people0", Map.of("id", 7L, "status", 1), Map.of()).sql());
        System.out.println("template preparation reactive/JDBC B/op=" + reactive + "/" + jdbc);
        assertTrue(jdbc < reactive + 512,
                "JDBC preparation must not build and discard a reactive statement plan: " + jdbc);
    }

    @Test
    void preparedCreateAppendsColumnsWithoutIntermediateStrings() {
        RelationalTableDefinition table = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                .addField(DynamicField.of("status", "INTEGER")).build().toTableDefinition();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, table.identity(),
                table.name(), null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema());
        double bytes = allocated(() -> sink = renderer.render(operation));
        assertEquals("create table \"people\" (\"id\" BIGINT, \"name\" VARCHAR(64), \"status\" INTEGER)",
                renderer.render(operation).getFirst().sql());
        System.out.println("prepared create allocation B/op=" + bytes);
        assertTrue(bytes < 1400, "ordinary CREATE must avoid repeated SQL-buffer expansion: " + bytes);
    }

    @Test
    void scalarSchemaNamesReuseNormalizedMetadata() {
        SchemaDialect dialect = RdbDialect.postgresql().schema();
        double bytes = allocated(() -> sink = dialect.dataType("BIGINT"));
        assertEquals("BIGINT", sink);
        System.out.println("scalar schema type allocation B/op=" + bytes);
        assertTrue(bytes < 128, "scalar schema mapping must reuse its normalized name: " + bytes);
    }

    @Test
    void simpleSchemaSizingDoesNotCreateModifierMatchers() {
        SchemaDialect dialect = SchemaDialect.standard();
        double bytes = allocated(() -> sink = dialect.dataType("VARCHAR", 64, null, null));
        assertEquals("VARCHAR(64)", sink);
        System.out.println("simple schema sizing allocation B/op=" + bytes);
        assertTrue(bytes < 512, "plain sizing must not build a modifier matcher: " + bytes);
    }

    @Test
    void unqualifiedSchemaIdentityAvoidsQualifiedNameAssembly() {
        SchemaDialect dialect = RdbDialect.postgresql().schema();
        RelationIdentity identity = RelationIdentity.table("people");
        double bytes = allocated(() -> sink = dialect.relationIdentifier(identity));
        assertEquals("\"people\"", sink);
        System.out.println("unqualified schema identity allocation B/op=" + bytes);
        assertTrue(bytes < 96, "one name segment must not build a qualified-name joiner: " + bytes);
    }

    @Test
    void emptyRelationalDefinitionsDoNotBuildUnusedCollections() {
        RelationIdentity identity = RelationIdentity.table("people");
        double bytes = allocated(() -> sink = RelationalTableDefinition.builder(identity).build());
        RelationalTableDefinition table = (RelationalTableDefinition) sink;
        assertTrue(table.columns().isEmpty());
        assertTrue(table.findIndex("missing").isEmpty());
        assertTrue(table.findForeignKey("missing").isEmpty());
        System.out.println("empty relational definition allocation B/op=" + bytes);
        assertTrue(bytes < 384, "absent metadata must not build mutable collections and wrappers: " + bytes);
    }

    @Test
    void ordinaryCreateAvoidsIntermediateCollectionPipelines() {
        DynamicForm form = DynamicForm.builder("people", "people")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                .addField(DynamicField.of("status", "INTEGER")).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        double bytes = allocated(() -> sink = renderer.createTable(form));
        assertEquals("create table \"people\" (\"id\" BIGINT, \"name\" VARCHAR(64), \"status\" INTEGER)",
                renderer.createTable(form).getFirst().sql());
        System.out.println("ordinary create allocation B/op=" + bytes);
        assertTrue(bytes < 6000, "ordinary CREATE must not allocate empty collection pipelines: " + bytes);
    }

    @Test
    void fixedSchemaMappingDimensionsReuseTheParsedTarget() {
        SchemaDialect dialect = SchemaDialect.builder().mapType("label", "VARCHAR(64)").build();
        double bytes = allocated(() -> sink = dialect.dataType("label", 80, null, null));
        assertEquals("VARCHAR(80)", sink);
        System.out.println("sized fixed schema mapping allocation B/type=" + bytes);
        assertTrue(bytes < 768, "type dimensions must use the already parsed target: " + bytes);
    }

    @Test
    void fixedSchemaMappingsDoNotReparseTheirConfiguredTarget() {
        SchemaDialect dialect = SchemaDialect.builder().mapType("label", "VARCHAR(64)").build();
        double bytes = allocated(() -> sink = dialect.dataType("label"));
        assertEquals("VARCHAR(64)", sink);
        System.out.println("fixed schema mapping allocation B/type=" + bytes);
        assertTrue(bytes < 256, "fixed type mappings must prepare their target once: " + bytes);
    }

    @Test
    void unchangedSchemaTypeMappingDoesNotParseTheSameDeclarationTwice() {
        SchemaDialect dialect = SchemaDialect.standard();
        double parsed = allocated(() -> sink = DatabaseType.of("VARCHAR(64)"));
        double mapped = allocated(() -> sink = dialect.dataType("VARCHAR(64)"));
        assertEquals("VARCHAR(64)", sink);
        System.out.println("sized type allocation parsed/mapped B/type=" + parsed + "/" + mapped);
        assertTrue(mapped < parsed + 512, "unchanged mapped types must retain their parsed form: " + mapped);
    }

    @Test
    void compactSizedTypesDoNotNormalizeAbsentWhitespace() {
        double bytes = allocated(() -> sink = DatabaseType.of("VARCHAR(64)"));
        assertEquals("varchar(64)", ((DatabaseType) sink).canonical());
        System.out.println("compact sized type allocation B/type=" + bytes);
        assertTrue(bytes < 1400, "compact type syntax must skip whitespace replacement: " + bytes);
    }

    @Test
    void argumentFreeComparisonShapesReuseCanonicalText() {
        DatabaseType type = DatabaseType.of("TIMESTAMP WITH TIME ZONE[]");
        double bytes = allocated(() -> sink = type.comparisonShape());
        assertSame(type.canonical(), sink);
        System.out.println("argument-free shape allocation B/type=" + bytes);
        assertTrue(bytes < 64, "argument-free type comparison must reuse normalized text: " + bytes);
    }

    @Test
    void scalarDatabaseTypesDoNotAllocateGeneralSyntaxMatchers() {
        double bytes = allocated(() -> sink = DatabaseType.of("INTEGER"));
        assertEquals("integer", ((DatabaseType) sink).canonical());
        System.out.println("scalar database type allocation B/type=" + bytes);
        assertTrue(bytes < 256, "plain scalar types must not allocate general syntax matchers: " + bytes);
    }

    @Test
    void singleTypeMappingsDoNotAllocateCandidateLists() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.standard();
        EntityTypeMappingRegistry.Mapping expected = mappings.resolve(String.class);
        double bytes = allocated(() -> sink = mappings.resolve(String.class));
        assertSame(expected, sink);
        System.out.println("single type lookup allocation B/lookup=" + bytes);
        assertTrue(bytes < 64, "a single type match must not allocate candidate lists: " + bytes);
    }

    @Test
    void scalarAssignmentsDoNotCompileAnEntityReadPlan() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            models.metadata(GetterBean.class);
            long metadataEntries = models.estimatedMappings();
            Object name = new StringBuilder("Ada");
            assertSame(name, models.writeValue(GetterBean.class, "name", name));
            assertEquals(7L, models.writeValue(GetterBean.class, "id", 7L));
            assertNull(models.writeValue(GetterBean.class, "name", null));
            assertThrows(IllegalArgumentException.class,
                    () -> models.writeValue(GetterBean.class, "missing", name));
            assertEquals(metadataEntries, models.estimatedMappings(),
                    "scalar assignments need metadata, not a complete entity read plan");
        }
    }

    @Test
    void beanQueriesDoNotAllocateAnEmptyConstructorArgumentArrayPerRow() {
        RowMapper<Bean> mapper = MappingPlan.of(Bean.class);
        Function<IntFunction<Object>, Bean> binding = mapper.bindColumns(DynamicRow.copyOf(Map.of("name", "Ada")));
        IntFunction<Object> values = index -> "Ada";
        double bytes = allocated(() -> sink = binding.apply(values));
        assertEquals("Ada", ((Bean) sink).name);
        System.out.println("entity bean mapping allocation B/row=" + bytes);
        assertTrue(bytes < 32, "bean mapping must not allocate empty reflection arguments: " + bytes);
    }

    @Test
    void entityGettersDoNotAllocateAnEmptyArgumentArrayPerProperty() {
        EntityValues<GetterBean> reader = EntityValues.createUncached(GetterBean.class);
        GetterBean entity = new GetterBean();
        double bytes = allocated(() -> sink = reader.readForInsert(entity));
        assertEquals(Map.of("active", true, "id", 7L, "name", "Ada", "status", 1), sink);
        System.out.println("entity getter insert allocation B/row=" + bytes);
        assertTrue(bytes < 352, "entity inserts must not allocate per-getter argument arrays: " + bytes);
    }

    private static double allocated(Runnable operation) {
        assumeTrue(ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .noneMatch(argument -> argument.startsWith("-javaagent:")),
                "allocation limits require an uninstrumented VM");
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        int iterations = 30_000;
        for (int index = 0; index < iterations; index++) operation.run();
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) operation.run();
        return (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
    }

    private static final class Bean {
        private String name;
    }

    private static final class GetterBean {
        private boolean active;
        private long id;
        private String name;
        private int status;

        public boolean isActive() { return true; }
        public long getId() { return 7L; }
        public String getName() { return "Ada"; }
        public int getStatus() { return 1; }
    }
}
