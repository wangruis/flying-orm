package com.flying.orm.rdb.codec;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Mono;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EnumMaskValueCodecTest {
    enum Flag { A, B, C }
    enum Foreign { A }
    enum TooMany {
        V0, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12,
        V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25,
        V26, V27, V28, V29, V30, V31, V32, V33, V34, V35, V36, V37, V38,
        V39, V40, V41, V42, V43, V44, V45, V46, V47, V48, V49, V50, V51,
        V52, V53, V54, V55, V56, V57, V58, V59, V60, V61, V62, V63, V64
    }
    private static final EnumMaskValueCodec<Flag> CODEC = EnumMaskValueCodec.of(Flag.class,
            flag -> switch (flag) { case A -> 1L; case B -> 8L; case C -> Long.MIN_VALUE; });

    @Test
    void explicitBitsRoundTripEmptyNullDuplicatesAndSignBit() {
        assertEquals(9L, CODEC.mask(Flag.A, Flag.B, Flag.A));
        assertEquals(Long.MIN_VALUE | 9L, CODEC.write(new Flag[]{Flag.C, Flag.B, Flag.A}));
        assertArrayEquals(new Flag[]{Flag.A, Flag.B, Flag.C}, CODEC.read(Long.MIN_VALUE | 9L, Flag[].class));
        assertArrayEquals(new Flag[]{Flag.B}, CODEC.read(new BigDecimal("8"), Flag[].class));
        assertEquals(0L, CODEC.mask());
        assertArrayEquals(new Flag[0], CODEC.read(0L, Flag[].class));
        assertNull(CODEC.write(null));
        assertNull(CODEC.read(null, Flag[].class));
        Flag[] first = CODEC.read(9L, Flag[].class);
        first[0] = Flag.C;
        assertArrayEquals(new Flag[]{Flag.A, Flag.B}, CODEC.read(9L, Flag[].class));
    }

    @Test
    void ordinalMappingRejectsWrappedBitsRatherThanAliasingConstants() {
        EnumMaskValueCodec<Flag> ordinal = EnumMaskValueCodec.ordinal(Flag.class);
        assertEquals(5L, ordinal.mask(Flag.A, Flag.C));
        assertArrayEquals(new Flag[]{Flag.A, Flag.C}, ordinal.read(5L, Flag[].class));
        assertThrows(IllegalArgumentException.class, () -> EnumMaskValueCodec.ordinal(TooMany.class));
    }

    @Test
    void unknownOrAmbiguousBitsNeverSilentlyDropData() {
        assertThrows(IllegalArgumentException.class, () -> EnumMaskValueCodec.of(Flag.class, value -> 0));
        assertThrows(IllegalArgumentException.class, () -> EnumMaskValueCodec.of(Flag.class, value -> 3));
        assertThrows(IllegalArgumentException.class, () -> EnumMaskValueCodec.of(Flag.class, value -> 1));
        assertThrows(IllegalArgumentException.class, () -> CODEC.read(2L, Flag[].class));
        assertThrows(IllegalArgumentException.class, () -> CODEC.read(new BigDecimal("8.5"), Flag[].class));
        assertThrows(IllegalArgumentException.class, () -> CODEC.write(new Foreign[]{Foreign.A}));
    }

    @Test
    void plugsIntoExistingEntitySchemaWithoutNewAnnotations() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("flag-mask", Flag[].class, DatabaseType.of("BIGINT"), CODEC).build();
        EntitySchemaDescriptor<Flags> schema = EntitySchemaDescriptor.builder(Flags.class)
                .typeMappings(mappings).build();
        assertEquals(DatabaseType.of("BIGINT"), schema.metadata().field("flags").databaseType());
        assertEquals(9L, mappings.valueCodecs().write(new Flag[]{Flag.A, Flag.B}));
        assertArrayEquals(new Flag[]{Flag.A, Flag.B}, mappings.valueCodecs().read(9L, Flag[].class));
    }

    @TableName("flag_rows")
    record Flags(@TableId Long id, Flag[] flags) { }

    @Test
    void publicRepositoriesBindMasksForInsertUpdateAndCustomConditions() {
        EntitySchemaDescriptor<Flags> schema = EntitySchemaDescriptor.builder(Flags.class)
                .typeMappings(EntityTypeMappingRegistry.builder()
                        .register("flag-mask", Flag[].class, DatabaseType.of("BIGINT"), CODEC).build()).build();
        List<SqlRequest> requests = new ArrayList<>();
        JdbcConnectionAccess jdbc = JdbcConnectionAccess.of(request -> {
            requests.add(request);
            throw new IllegalStateException("no database in binding probe");
        }, (connection, request) -> fail("no connection was acquired"));
        R2dbcConnectionAccess reactive = R2dbcConnectionAccess.of(request -> {
            requests.add(request);
            return Mono.error(new IllegalStateException("no database in binding probe"));
        }, (signal, connection, request) -> { throw new AssertionError("no connection was acquired"); });
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(BitMaskTermHandlers.postgresql()).build();
        try (FlyingOrmClients clients = FlyingOrmClients.builder(jdbc, reactive).dialect(RdbDialect.postgresql())
                .renderer(renderer).entitySchema(schema)
                .structuredConditionResolver(BitMaskTermHandlers.conditions()).build()) {
            Flags entity = new Flags(77L, new Flag[]{Flag.A, Flag.B});
            assertThrows(RuntimeException.class, () -> clients.syncRepository(Flags.class).insert(entity));
            assertThrows(RuntimeException.class, () -> clients.repository(Flags.class).insert(entity).block());
            assertEquals(List.of(77L, 9L), requests.get(0).parameters());
            assertEquals(requests.get(0).parameters(), requests.get(1).parameters());
            assertThrows(RuntimeException.class, () -> clients.syncRepository(Flags.class).createUpdate()
                    .set(Flags::flags, new Flag[]{Flag.B}).where(Flags::id, 77L).execute());
            assertThrows(RuntimeException.class, () -> clients.repository(Flags.class).createUpdate()
                    .set(Flags::flags, new Flag[]{Flag.B}).where(Flags::id, 77L).execute().block());
            assertEquals(List.of(8L, 77L), requests.get(2).parameters());
            assertEquals(requests.get(2).parameters(), requests.get(3).parameters());
            assertThrows(RuntimeException.class, () -> clients.syncRepository(Flags.class).createQuery()
                    .where(Flags::flags, "mask-all", CODEC.mask(Flag.B)).fetch());
            assertThrows(RuntimeException.class, () -> clients.repository(Flags.class).createQuery()
                    .where(Flags::flags, "mask-all", CODEC.mask(Flag.B)).fetch().collectList().block());
            assertEquals(List.of(8L, 8L), requests.get(4).parameters());
            assertEquals(requests.get(4).parameters(), requests.get(5).parameters());
            Flags decoded = clients.forms().entityModels().rowMapper(Flags.class, schema.valueCodecs())
                    .map(DynamicRow.copyOf(Map.of("id", 77L, "flags", 9L)));
            assertArrayEquals(entity.flags(), decoded.flags());
            FieldUsePolicy denied = FieldUsePolicy.builder()
                    .allow("id", FieldUse.PROJECT).allow("flags", FieldUse.PROJECT).build();
            for (String operator : List.of("mask-any", "mask-not-any", "mask-all", "mask-not-all",
                    "mask-contained-by", "mask-not-contained-by")) {
                int binds = operator.endsWith("all") ? 2 : 1;
                QuerySpec query = QuerySpec.structured(schema.form(),
                        StructuredConditionInput.term("flags", operator, 0L));
                int before = requests.size();
                assertThrows(RuntimeException.class, () -> clients.syncForms()
                        .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxBindCount(binds)).select(query));
                assertThrows(RuntimeException.class, () -> clients.forms()
                        .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxBindCount(binds))
                        .select(query).collectList().block());
                assertEquals(before + 2, requests.size());
                assertThrows(IllegalArgumentException.class,
                        () -> clients.syncForms().withFieldUsePolicy(denied).select(query));
                assertThrows(IllegalArgumentException.class,
                        () -> clients.forms().withFieldUsePolicy(denied).select(query).collectList().block());
                assertThrows(IllegalArgumentException.class, () -> clients.syncForms()
                        .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxBindCount(binds - 1)).select(query));
                assertThrows(IllegalArgumentException.class, () -> clients.forms()
                        .withQueryShapeLimits(QueryShapeLimits.defaults().withMaxBindCount(binds - 1))
                        .select(query).collectList().block());
                assertEquals(before + 2, requests.size());
            }
        }
    }
}
