package com.flying.orm.rdb.api;

import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * 锁定 V2.0.0 正式公开 API。
 *
 * <p>快照在正式发布前锁定目标 API；只有经过明确审查的 2.0.0 API 决策才能同步它。
 * 版本发布后如需调整，应新建下一版本基线。</p>
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
class PublicApiBaselineTest {

    private static final String V2_BASELINE_RESOURCE = "/api/v2.0.0-public-api.txt";

    private static final Map<String, Class<?>> MODULE_MARKERS = moduleMarkers();

    @Test
    void v200BaselineRemainsAvailableForReleaseReview() throws Exception {
        try (InputStream input = PublicApiBaselineTest.class.getResourceAsStream(V2_BASELINE_RESOURCE)) {
            if (input == null || normalize(new String(input.readAllBytes(), StandardCharsets.UTF_8)).isBlank()) {
                fail("V2.0.0 public API baseline is missing");
            }
        }
    }

    @Test
    void coreAndRdbPublicApiMatchesV200Baseline() throws Exception {
        String actual = normalize(PublicApiSnapshot.capture(MODULE_MARKERS));
        try (InputStream input = PublicApiBaselineTest.class.getResourceAsStream(V2_BASELINE_RESOURCE)) {
            if (input == null) {
                fail("V2.0.0 public API baseline is missing");
            }
            String expected = normalize(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            if (!expected.equals(actual)) {
                fail("V2.0.0 public API differs from its reviewed release baseline; "
                             + firstDifference(expected, actual));
            }
        }
    }

    /** 给 API 快照失败提供首个差异，避免靠肉眼比较整份清单。 */
    private static String firstDifference(String expected, String actual) {
        List<String> expectedLines = expected.lines().toList();
        List<String> actualLines = actual.lines().toList();
        int commonLength = Math.min(expectedLines.size(), actualLines.size());
        for (int index = 0; index < commonLength; index++) {
            if (!expectedLines.get(index).equals(actualLines.get(index))) {
                return "line " + (index + 1) + " expected <" + expectedLines.get(index)
                        + "> but was <" + actualLines.get(index) + ">";
            }
        }
        return "line count expected " + expectedLines.size() + " but was " + actualLines.size();
    }

    private static Map<String, Class<?>> moduleMarkers() {
        Map<String, Class<?>> markers = new LinkedHashMap<>();
        markers.put("flying-orm-core", ConditionNode.class);
        markers.put("flying-orm-rdb", ReactiveSqlExecutor.class);
        // LinkedHashMap 固定模块顺序，不能换成不承诺遍历顺序的只读 Map。
        return Collections.unmodifiableMap(markers);
    }

    private static String normalize(String content) {
        return content.replace("\r\n", "\n")
                      .replace('\r', '\n')
                      .replaceAll("[\\t ]+(?=\\n|$)", "");
    }

    /** 只在测试源码里使用的快照器，不进入任何正式发布包。 */
    static final class PublicApiSnapshot {

        private static final int API_MEMBER_MODIFIERS = Modifier.PUBLIC
                | Modifier.PROTECTED
                | Modifier.STATIC
                | Modifier.FINAL
                | Modifier.ABSTRACT
                | Modifier.SYNCHRONIZED
                | Modifier.NATIVE
                | Modifier.STRICT
                | Modifier.TRANSIENT
                | Modifier.VOLATILE;

        private PublicApiSnapshot() {
        }

        static String capture(Map<String, Class<?>> moduleMarkers) throws Exception {
            StringBuilder snapshot = new StringBuilder(64 * 1024);
            snapshot.append("# flying-orm public API snapshot\n")
                    .append("# Generated from compiled flying-orm-core and flying-orm-rdb bytecode.\n");
            for (Map.Entry<String, Class<?>> module : moduleMarkers.entrySet()) {
                snapshot.append("\nMODULE ").append(module.getKey()).append('\n');
                List<Class<?>> apiTypes = loadApiTypes(module.getValue());
                for (Class<?> type : apiTypes) {
                    appendType(snapshot, type);
                }
            }
            return normalize(snapshot.toString());
        }

        private static List<Class<?>> loadApiTypes(Class<?> marker)
                throws IOException, URISyntaxException, ClassNotFoundException {
            URL locationUrl = Objects.requireNonNull(marker.getProtectionDomain().getCodeSource(),
                                                     "module marker has no code source")
                                     .getLocation();
            Path location = Path.of(locationUrl.toURI());
            Set<String> names = Files.isDirectory(location)
                    ? classNamesFromDirectory(location)
                    : classNamesFromJar(location);
            List<Class<?>> types = new ArrayList<>();
            ClassLoader loader = marker.getClassLoader();
            for (String name : names) {
                Class<?> type = Class.forName(name, false, loader);
                if (isEffectiveApiType(type)) {
                    types.add(type);
                }
            }
            types.sort(Comparator.comparing(Class::getName));
            return List.copyOf(types);
        }

        private static Set<String> classNamesFromDirectory(Path root) throws IOException {
            Set<String> names = new LinkedHashSet<>();
            try (Stream<Path> paths = Files.walk(root)) {
                paths.filter(Files::isRegularFile)
                     .filter(path -> path.getFileName().toString().endsWith(".class"))
                     .map(path -> root.relativize(path).toString())
                     .map(PublicApiSnapshot::toClassName)
                     .filter(PublicApiSnapshot::isLoadableClassName)
                     .sorted()
                     .forEach(names::add);
            }
            return names;
        }

        private static Set<String> classNamesFromJar(Path jarPath) throws IOException {
            Set<String> names = new LinkedHashSet<>();
            try (JarFile jar = new JarFile(jarPath.toFile())) {
                jar.stream()
                   .filter(entry -> !entry.isDirectory())
                   .map(JarEntry::getName)
                   .filter(name -> name.endsWith(".class"))
                   .map(PublicApiSnapshot::toClassName)
                   .filter(PublicApiSnapshot::isLoadableClassName)
                   .sorted()
                   .forEach(names::add);
            }
            return names;
        }

        private static String toClassName(String classFile) {
            return classFile.substring(0, classFile.length() - ".class".length())
                            .replace('/', '.')
                            .replace('\\', '.');
        }

        private static boolean isLoadableClassName(String name) {
            return !name.equals("module-info") && !name.endsWith(".package-info");
        }

        private static boolean isEffectiveApiType(Class<?> type) {
            if (type.isSynthetic() || type.isAnonymousClass() || type.isLocalClass()) {
                return false;
            }
            if (type.getName().contains(".internal.") || type.isAnnotationPresent(InternalApi.class)) {
                return false;
            }
            Class<?> current = type;
            while (current.getEnclosingClass() != null) {
                if (!isPublicOrProtected(current.getModifiers())) {
                    return false;
                }
                current = current.getEnclosingClass();
            }
            return Modifier.isPublic(current.getModifiers());
        }

        private static void appendType(StringBuilder snapshot, Class<?> type) {
            snapshot.append("TYPE ")
                    .append(modifiers(type.getModifiers()))
                    .append(typeKind(type)).append(' ')
                    .append(type.getName())
                    .append(typeParameters(type.getTypeParameters()));
            Type superclass = type.getGenericSuperclass();
            if (superclass != null && superclass != Object.class) {
                snapshot.append(" extends ").append(typeName(superclass));
            }
            Type[] interfaces = type.getGenericInterfaces();
            if (interfaces.length > 0) {
                snapshot.append(type.isInterface() ? " extends " : " implements ")
                        .append(joinTypes(interfaces));
            }
            if (type.isSealed()) {
                snapshot.append(" permits ")
                        .append(Arrays.stream(type.getPermittedSubclasses())
                                      .map(Class::getName)
                                      .sorted()
                                      .reduce((left, right) -> left + ", " + right)
                                      .orElse(""));
            }
            snapshot.append('\n');

            appendRecordComponents(snapshot, type);
            appendFields(snapshot, type);
            appendConstructors(snapshot, type);
            appendMethods(snapshot, type);
            snapshot.append("END\n");
        }

        private static void appendRecordComponents(StringBuilder snapshot, Class<?> type) {
            if (!type.isRecord()) {
                return;
            }
            for (RecordComponent component : type.getRecordComponents()) {
                snapshot.append("  COMPONENT ")
                        .append(typeName(component.getGenericType())).append(' ')
                        .append(component.getName()).append('\n');
            }
        }

        private static void appendFields(StringBuilder snapshot, Class<?> type) {
            Arrays.stream(type.getDeclaredFields())
                  .filter(field -> isPublicOrProtected(field.getModifiers()))
                  .filter(field -> !field.isSynthetic())
                  .sorted(Comparator.comparing(Field::getName)
                                    .thenComparing(field -> typeName(field.getGenericType())))
                  .forEach(field -> {
                      if (field.isEnumConstant()) {
                          snapshot.append("  ENUM_CONSTANT ").append(field.getName()).append('\n');
                      } else {
                          snapshot.append("  FIELD ")
                                  .append(modifiers(field.getModifiers()))
                                  .append(typeName(field.getGenericType())).append(' ')
                                  .append(field.getName()).append('\n');
                      }
                  });
        }

        private static void appendConstructors(StringBuilder snapshot, Class<?> type) {
            Arrays.stream(type.getDeclaredConstructors())
                  .filter(constructor -> isPublicOrProtected(constructor.getModifiers()))
                  .filter(constructor -> !constructor.isSynthetic())
                  .sorted(Comparator.comparing(PublicApiSnapshot::constructorKey))
                  .forEach(constructor -> snapshot.append("  CONSTRUCTOR ")
                                                  .append(executableSignature(constructor))
                                                  .append('\n'));
        }

        private static void appendMethods(StringBuilder snapshot, Class<?> type) {
            Arrays.stream(type.getDeclaredMethods())
                  .filter(method -> isPublicOrProtected(method.getModifiers()))
                  .filter(method -> !method.isSynthetic() && !method.isBridge())
                  .filter(method -> !method.isAnnotationPresent(InternalApi.class))
                  .sorted(Comparator.comparing(PublicApiSnapshot::methodKey))
                  .forEach(method -> {
                      snapshot.append("  METHOD ").append(methodSignature(method));
                      Object defaultValue = method.getDefaultValue();
                      if (defaultValue != null) {
                          snapshot.append(" default ").append(defaultValue(defaultValue));
                      }
                      snapshot.append('\n');
                  });
        }

        private static String executableSignature(Constructor<?> constructor) {
            return modifiers(constructor.getModifiers())
                    + typeParameters(constructor.getTypeParameters())
                    + "<init>(" + parameters(constructor.getGenericParameterTypes(), constructor.isVarArgs()) + ")"
                    + exceptions(constructor.getGenericExceptionTypes());
        }

        private static String methodSignature(Method method) {
            String defaultModifier = method.isDefault() ? "default " : "";
            return modifiers(method.getModifiers())
                    + defaultModifier
                    + typeParameters(method.getTypeParameters())
                    + typeName(method.getGenericReturnType()) + ' '
                    + method.getName() + '('
                    + parameters(method.getGenericParameterTypes(), method.isVarArgs()) + ')'
                    + exceptions(method.getGenericExceptionTypes());
        }

        private static String constructorKey(Constructor<?> constructor) {
            return parameters(constructor.getGenericParameterTypes(), constructor.isVarArgs());
        }

        private static String methodKey(Method method) {
            return method.getName() + '(' + parameters(method.getGenericParameterTypes(), method.isVarArgs()) + ')'
                    + typeName(method.getGenericReturnType());
        }

        private static String parameters(Type[] parameterTypes, boolean varArgs) {
            List<String> names = new ArrayList<>(parameterTypes.length);
            for (int index = 0; index < parameterTypes.length; index++) {
                String name = typeName(parameterTypes[index]);
                if (varArgs && index == parameterTypes.length - 1 && name.endsWith("[]")) {
                    name = name.substring(0, name.length() - 2) + "...";
                }
                names.add(name);
            }
            return String.join(", ", names);
        }

        private static String exceptions(Type[] exceptionTypes) {
            if (exceptionTypes.length == 0) {
                return "";
            }
            return " throws " + Arrays.stream(exceptionTypes)
                                       .map(PublicApiSnapshot::typeName)
                                       .sorted()
                                       .reduce((left, right) -> left + ", " + right)
                                       .orElse("");
        }

        private static String typeParameters(TypeVariable<?>[] variables) {
            if (variables.length == 0) {
                return "";
            }
            List<String> definitions = new ArrayList<>(variables.length);
            for (TypeVariable<?> variable : variables) {
                StringBuilder definition = new StringBuilder(variable.getName());
                List<String> bounds = Arrays.stream(variable.getBounds())
                                            .map(PublicApiSnapshot::typeName)
                                            .filter(bound -> !bound.equals(Object.class.getName()))
                                            .toList();
                if (!bounds.isEmpty()) {
                    definition.append(" extends ").append(String.join(" & ", bounds));
                }
                definitions.add(definition.toString());
            }
            return '<' + String.join(", ", definitions) + "> ";
        }

        private static String joinTypes(Type[] types) {
            return Arrays.stream(types)
                         .map(PublicApiSnapshot::typeName)
                         // 泛型实参顺序属于签名，Map<K,V> 不能和 Map<V,K> 生成同一份快照。
                         .reduce((left, right) -> left + ", " + right)
                         .orElse("");
        }

        private static String typeName(Type type) {
            if (type instanceof Class<?> concrete) {
                return concrete.isArray()
                        ? typeName(concrete.getComponentType()) + "[]"
                        : concrete.getName();
            }
            if (type instanceof ParameterizedType parameterized) {
                return typeName(parameterized.getRawType()) + '<'
                        + joinTypes(parameterized.getActualTypeArguments()) + '>';
            }
            if (type instanceof GenericArrayType array) {
                return typeName(array.getGenericComponentType()) + "[]";
            }
            if (type instanceof TypeVariable<?> variable) {
                return variable.getName();
            }
            if (type instanceof WildcardType wildcard) {
                if (wildcard.getLowerBounds().length > 0) {
                    return "? super " + joinTypes(wildcard.getLowerBounds());
                }
                List<String> upperBounds = Arrays.stream(wildcard.getUpperBounds())
                                                 .map(PublicApiSnapshot::typeName)
                                                 .filter(bound -> !bound.equals(Object.class.getName()))
                                                 .toList();
                return upperBounds.isEmpty() ? "?" : "? extends " + String.join(" & ", upperBounds);
            }
            return type.getTypeName();
        }

        private static String defaultValue(Object value) {
            if (!value.getClass().isArray()) {
                return String.valueOf(value);
            }
            int length = java.lang.reflect.Array.getLength(value);
            List<String> values = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                values.add(String.valueOf(java.lang.reflect.Array.get(value, index)));
            }
            return '[' + String.join(", ", values) + ']';
        }

        private static String typeKind(Class<?> type) {
            if (type.isAnnotation()) {
                return "annotation";
            }
            if (type.isEnum()) {
                return "enum";
            }
            if (type.isRecord()) {
                return "record";
            }
            if (type.isInterface()) {
                return "interface";
            }
            return "class";
        }

        private static String modifiers(int modifiers) {
            String text = Modifier.toString(modifiers & API_MEMBER_MODIFIERS);
            return text.isEmpty() ? "" : text + ' ';
        }

        private static boolean isPublicOrProtected(int modifiers) {
            return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
        }
    }
}
