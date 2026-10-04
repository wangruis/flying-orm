package com.flying.orm.rdb.architecture;

import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.InternalApi;
import org.junit.jupiter.api.Test;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Keeps the external consumer source in the normal quality gate without installing artifacts. */
class PublicConsumerCompilationTest {
    @Test
    void configuredEntityAndBatchAccessorsAreStablePublicContracts() throws Exception {
        for (Class<?> type : List.of(SyncFormClient.class, ReactiveFormClient.class)) {
            for (String name : List.of("entityRenderer", "defaultFieldScope", "defaultBatchWriteOptions")) {
                assertFalse(type.getMethod(name).isAnnotationPresent(InternalApi.class),
                        () -> type.getSimpleName() + "." + name + " must be a stable public contract");
            }
        }
    }

    @Test
    void upperServiceCanCompileAgainstTheCurrentPublicContracts() throws Exception {
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "the Java 21 build requires a JDK compiler");
        Path classes = Path.of(FlyingOrmClients.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path module = classes.getParent().getParent();
        Path source = module.resolve("src/test/resources/com/flying/orm/compat/UpperServiceConsumer.java");
        assertTrue(Files.isRegularFile(source), "the upper consumer fixture must be present");
        Path output = Files.createDirectories(module.resolve("target/api-consumer-test-classes"));
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            boolean compiled = compiler.getTask(null, files, diagnostics,
                    List.of("--release", "21", "-proc:none", "-classpath", System.getProperty("java.class.path"),
                            "-d", output.toString()), null, files.getJavaFileObjects(source.toFile())).call();
            assertTrue(compiled, () -> diagnostics.getDiagnostics().toString());
        }
    }
}
