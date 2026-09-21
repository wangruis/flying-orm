package com.flying.orm.core.compat;

import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.error.OrmErrorReportProvider;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.scope.ScopeAccessException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 冻结 3.1.0 已经公开的核心类型形状。
 *
 * <p>japicmp 负责完整的类、方法和字段比较；这里专门把 record 组件、sealed permits、异常父类和
 * 注解默认值写成直接可读的合同，避免这些 JVM 形状只藏在一份工具报告里。</p>
 */
class PublicApiShapeTest {

    private static final String BASELINE_COMMIT = "a2785d17e6c963c5f2cd870506322064dab788a9";

    private static final String BASELINE_CORE_SHA256 =
            "1B14E30956FA0DBDCF4F753FF88D0E3D358BD0BFCCBDB68F98FAA89CDE1C8114";

    private static final String BASELINE_RDB_SHA256 =
            "9B55FA0E0CBC32F881D45B3CC8F4B47FB7FB20318B2D24867FD45971CAE03EC7";

    @Test
    void keepsPublishedRecordComponentsInTheirDeclaredOrder() {
        assertRecord(DynamicField.class,
                "identity:com.flying.orm.core.field.FieldIdentity",
                "databaseType:com.flying.orm.core.type.DatabaseType",
                "primaryKey:boolean", "nullable:boolean", "unique:boolean",
                "length:java.lang.Integer", "precision:java.lang.Integer", "scale:java.lang.Integer",
                "comment:java.lang.String", "generation:com.flying.orm.core.metadata.ValueGeneration");
        assertRecord(ColumnMetadata.class,
                "identity:com.flying.orm.core.field.FieldIdentity",
                "databaseType:com.flying.orm.core.type.DatabaseType",
                "primaryKey:boolean", "nullable:boolean", "length:java.lang.Integer",
                "precision:java.lang.Integer", "scale:java.lang.Integer",
                "comment:java.lang.String", "generation:com.flying.orm.core.metadata.ValueGeneration");
        assertRecord(ForeignKeyMetadata.class,
                "name:java.lang.String", "columns:java.util.List",
                "referenceTable:java.lang.String", "referenceColumns:java.util.List");
        assertRecord(CursorSort.class,
                "field:java.lang.String", "direction:com.flying.orm.core.page.CursorDirection");
        assertRecord(CursorPageQuery.class,
                "size:int", "sorts:java.util.List", "cursor:java.util.List");
    }

    @Test
    void keepsPublishedSealedHierarchyAndExceptionKinds() {
        assertTrue(ConditionNode.class.isSealed());
        Set<Class<?>> permitted = Set.of(ConditionNode.class.getPermittedSubclasses());
        assertEquals(Set.of(ConditionGroup.class, TermCondition.class), permitted);

        assertEquals(IllegalArgumentException.class, ScopeAccessException.class.getSuperclass());
        assertEquals(IllegalArgumentException.class, StructuredConditionException.class.getSuperclass());
        assertTrue(OrmErrorReportProvider.class.isAssignableFrom(ScopeAccessException.class));
        assertTrue(OrmErrorReportProvider.class.isAssignableFrom(StructuredConditionException.class));
    }

    @Test
    void keepsTableFieldAsTheSingleNonPersistentPropertyContract() throws ReflectiveOperationException {
        assertAnnotationMethod(TableField.class.getMethod("value"), String.class, "");
        assertAnnotationMethod(TableField.class.getMethod("exist"), boolean.class, true);
        assertAnnotationMethod(TableField.class.getMethod("select"), boolean.class, true);
        assertAnnotationMethod(TableField.class.getMethod("fill"), FieldFill.class, FieldFill.DEFAULT);
        assertAnnotationMethod(TableField.class.getMethod("insertStrategy"),
                               FieldStrategy.class, FieldStrategy.DEFAULT);
        assertAnnotationMethod(TableField.class.getMethod("updateStrategy"),
                               FieldStrategy.class, FieldStrategy.DEFAULT);
    }

    @Test
    void apiCompatibilityInputsMatchTheFrozenBaseline() throws IOException, NoSuchAlgorithmException {
        String oldCoreProperty = System.getProperty("api.compat.core.old.jar");
        String oldRdbProperty = System.getProperty("api.compat.rdb.old.jar");
        assumeTrue(oldCoreProperty != null || oldRdbProperty != null,
                   "the baseline JAR checks run only when a frozen baseline is supplied");
        assertTrue(Boolean.getBoolean("api.compat.enabled"),
                   "baseline JAR inputs require the explicit api-compat profile");

        Path oldCore = requiredFile("api.compat.core.old.jar");
        Path oldRdb = requiredFile("api.compat.rdb.old.jar");
        Path newCore = configuredPath("api.compat.core.new.jar");
        Path newRdb = configuredPath("api.compat.rdb.new.jar");
        assertFalse(sameCanonicalPath(oldCore, newCore), "old and candidate core JAR must be different files");
        assertFalse(sameCanonicalPath(oldRdb, newRdb), "old and candidate RDB JAR must be different files");
        assertEquals(oldCore.getParent().toRealPath(), oldRdb.getParent().toRealPath(),
                     "both baseline JARs must come from the same frozen directory");

        Path manifest = oldCore.getParent().resolve("baseline.properties");
        assertTrue(Files.isRegularFile(manifest), "baseline.properties is missing beside the old JARs");
        Properties baseline = new Properties();
        try (InputStream input = Files.newInputStream(manifest)) {
            baseline.load(input);
        }
        assertEquals(BASELINE_COMMIT, baseline.getProperty("baseline.commit"));
        assertEquals("3.1.0", baseline.getProperty("baseline.source.branch"));
        assertEquals("3.1.0", baseline.getProperty("baseline.project.version"));
        assertEquals(BASELINE_CORE_SHA256, baseline.getProperty("core.sha256"));
        assertEquals(BASELINE_RDB_SHA256, baseline.getProperty("rdb.sha256"));
        assertArtifact(oldCore, "core", baseline);
        assertArtifact(oldRdb, "rdb", baseline);
    }

    private static void assertRecord(Class<?> type, String... expectedComponents) {
        assertTrue(type.isRecord(), () -> type.getName() + " must remain a record");
        List<String> actual = Arrays.stream(type.getRecordComponents())
                .map(PublicApiShapeTest::componentShape)
                .toList();
        assertEquals(List.of(expectedComponents), actual, () -> type.getName() + " record components changed");
    }

    private static String componentShape(RecordComponent component) {
        return component.getName() + ":" + component.getType().getTypeName();
    }

    private static void assertAnnotationMethod(Method method, Class<?> returnType, Object defaultValue) {
        assertEquals(returnType, method.getReturnType());
        assertEquals(defaultValue, method.getDefaultValue());
    }

    private static Path requiredFile(String property) {
        Path path = configuredPath(property);
        assertTrue(Files.isRegularFile(path), () -> property + " must point to a regular JAR: " + path);
        return path;
    }

    private static Path configuredPath(String property) {
        String value = System.getProperty(property);
        assertTrue(value != null && !value.isBlank() && !value.startsWith("${"),
                   () -> property + " must be supplied to the api-compat profile");
        return Path.of(value).toAbsolutePath().normalize();
    }

    private static boolean sameCanonicalPath(Path left, Path right) throws IOException {
        Path canonicalLeft = Files.exists(left) ? left.toRealPath() : left.toAbsolutePath().normalize();
        Path canonicalRight = Files.exists(right) ? right.toRealPath() : right.toAbsolutePath().normalize();
        return canonicalLeft.equals(canonicalRight);
    }

    private static void assertArtifact(Path artifact, String prefix, Properties baseline)
            throws IOException, NoSuchAlgorithmException {
        assertEquals(baseline.getProperty(prefix + ".file"), artifact.getFileName().toString());
        assertEquals(Long.parseLong(baseline.getProperty(prefix + ".length")), Files.size(artifact));
        assertEquals(baseline.getProperty(prefix + ".sha256"), sha256(artifact));
    }

    private static String sha256(Path file) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[64 * 1024];
            int length;
            while ((length = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, length);
            }
        }
        return HexFormat.of().withUpperCase().formatHex(digest.digest());
    }
}
