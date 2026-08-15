package com.flying.orm.benchmark.database;

import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 采集真实库性能运行的源码和已加载字节码身份。
 *
 * <p>只记录不可逆 SHA-256 与公开版本，不输出 classpath、本机绝对路径或 Git diff 正文。</p>
 *
 * @author wangr
 * @date 2026-08-15
 * @version v1.0
 */
final class BenchmarkRunIdentity {

    private static final int MINIMUM_GIT_PREFIX = 7;
    private static final long GIT_TIMEOUT_SECONDS = 30;
    private static final List<String> SOURCE_PATHS = List.of(
            "flying-orm-core", "flying-orm-rdb", "flying-orm-benchmark");

    private BenchmarkRunIdentity() {
    }

    static DatabasePerformanceReport.RunIdentity capture() {
        Path repository = repositoryRoot(Path.of("").toAbsolutePath());
        String gitHead = text(git(repository, "rev-parse", "HEAD"));
        byte[] status = git(repository, "status", "--porcelain=v1", "--untracked-files=no", "--",
                            SOURCE_PATHS.get(0), SOURCE_PATHS.get(1), SOURCE_PATHS.get(2));
        byte[] diff = git(repository, "diff", "--binary", "--no-ext-diff", "HEAD", "--",
                          SOURCE_PATHS.get(0), SOURCE_PATHS.get(1), SOURCE_PATHS.get(2));
        String collectors = ManagementFactory.getGarbageCollectorMXBeans().stream()
                .map(bean -> bean.getName().trim())
                .sorted()
                .reduce((left, right) -> left + ", " + right)
                .orElse("unavailable");
        return new DatabasePerformanceReport.RunIdentity(
                gitHead, status.length > 0, sha256(diff),
                sha256(System.getProperty("java.class.path", "").getBytes(StandardCharsets.UTF_8)),
                classSha256(RealDatabasePerformanceRunner.class), classSha256(R2dbcSqlExecutor.class),
                System.getProperty("java.vm.name", "unknown"),
                System.getProperty("java.vm.version", "unknown"), collectors);
    }

    static void requireCommitLabel(DatabasePerformanceReport.RunIdentity identity, String label) {
        DatabasePerformanceReport.RunIdentity safeIdentity = Objects.requireNonNull(
                identity, "benchmark run identity must not be null");
        String safeLabel = Objects.requireNonNull(label, "benchmark Git commit label must not be null").trim();
        boolean exact = safeIdentity.gitHead().equalsIgnoreCase(safeLabel);
        boolean prefix = safeLabel.length() >= MINIMUM_GIT_PREFIX
                && safeIdentity.gitHead().regionMatches(true, 0, safeLabel, 0, safeLabel.length());
        if (!exact && !prefix) {
            throw new IllegalArgumentException("benchmark Git commit label does not match the current HEAD");
        }
    }

    static String classSha256(Class<?> type) {
        Class<?> safeType = Objects.requireNonNull(type, "benchmark class type must not be null");
        String resource = "/" + safeType.getName().replace('.', '/') + ".class";
        try (InputStream input = safeType.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("benchmark class bytes are unavailable");
            }
            return sha256(input.readAllBytes());
        } catch (IOException error) {
            throw new IllegalStateException("benchmark class bytes could not be read", error);
        }
    }

    static String implementationVersion(Class<?> type) {
        Class<?> safeType = Objects.requireNonNull(type, "benchmark implementation type must not be null");
        String packageVersion = safeType.getPackage() == null
                ? null : safeType.getPackage().getImplementationVersion();
        if (packageVersion != null && !packageVersion.isBlank()) {
            return packageVersion.trim();
        }
        if (safeType.getModule().getDescriptor() != null) {
            String moduleVersion = safeType.getModule().getDescriptor().rawVersion().orElse(null);
            if (moduleVersion != null && !moduleVersion.isBlank()) {
                return moduleVersion.trim();
            }
        }
        String fileName = codeSourceFileName(safeType);
        int firstDigit = firstDigit(fileName);
        int jarSuffix = fileName.toLowerCase(java.util.Locale.ROOT).lastIndexOf(".jar");
        return firstDigit >= 0 && jarSuffix > firstDigit
                ? fileName.substring(firstDigit, jarSuffix) : "unavailable";
    }

    private static Path repositoryRoot(Path start) {
        Path current = start;
        while (current != null) {
            if (Files.exists(current.resolve(".git"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("benchmark repository root is unavailable");
    }

    private static byte[] git(Path repository, String... arguments) {
        List<String> command = new ArrayList<>(arguments.length + 1);
        command.add("git");
        command.addAll(List.of(arguments));
        ProcessBuilder builder = new ProcessBuilder(command).directory(repository.toFile());
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        try {
            Process process = builder.start();
            byte[] output = process.getInputStream().readAllBytes();
            if (!process.waitFor(GIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("benchmark Git evidence command timed out");
            }
            if (process.exitValue() != 0) {
                throw new IllegalStateException("benchmark Git evidence command failed");
            }
            return output;
        } catch (IOException error) {
            throw new IllegalStateException("benchmark Git evidence command could not start", error);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("benchmark Git evidence command was interrupted", error);
        }
    }

    private static String codeSourceFileName(Class<?> type) {
        try {
            if (type.getProtectionDomain() == null || type.getProtectionDomain().getCodeSource() == null) {
                return "";
            }
            Path path = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
            return path.getFileName() == null ? "" : path.getFileName().toString();
        } catch (URISyntaxException | IllegalArgumentException ignored) {
            return "";
        }
    }

    private static int firstDigit(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (Character.isDigit(value.charAt(index))) {
                return index;
            }
        }
        return -1;
    }

    private static String text(byte[] value) {
        String safe = new String(value, StandardCharsets.UTF_8).trim();
        if (safe.isEmpty()) {
            throw new IllegalStateException("benchmark Git evidence is empty");
        }
        return safe;
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }
}
