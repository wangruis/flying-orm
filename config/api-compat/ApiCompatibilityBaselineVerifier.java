import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Properties;

/**
 * 在 Maven 进入编译前确认 API/ABI 比较使用的确实是冻结的 3.1.0 制品。
 *
 * <p>这段校验故意不放在 JUnit 里，因为 {@code -DskipTests} 和
 * {@code -Dmaven.test.skip=true} 都不应该关闭兼容基线。japicmp 的路径也直接指向同一目录，
 * 因而命令行参数不能把“旧包”偷偷替换成本次候选包。</p>
 */
public final class ApiCompatibilityBaselineVerifier {
    private static final String BASELINE_DIRECTORY =
            ".tmp/upper-capability-baseline/a2785d17";
    private static final String BASELINE_COMMIT =
            "a2785d17e6c963c5f2cd870506322064dab788a9";
    private static final String MANIFEST_SHA256 =
            "6B8FE27E795E25BCC5FF238CC98C8A2BC8E0B135DAE895E00B3DE534F6A09C62";
    private static final String CORE_SHA256 =
            "1B14E30956FA0DBDCF4F753FF88D0E3D358BD0BFCCBDB68F98FAA89CDE1C8114";
    private static final String RDB_SHA256 =
            "9B55FA0E0CBC32F881D45B3CC8F4B47FB7FB20318B2D24867FD45971CAE03EC7";
    private static final long CORE_LENGTH = 297_405L;
    private static final long RDB_LENGTH = 1_820_402L;

    private ApiCompatibilityBaselineVerifier() {
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("expected exactly one repository-root argument");
        }
        Path repository = Path.of(arguments[0]).toRealPath();
        Path baseline = repository.resolve(BASELINE_DIRECTORY).normalize();
        Path manifest = baseline.resolve("baseline.properties");
        Path core = baseline.resolve("flying-orm-core-3.1.0.jar");
        Path rdb = baseline.resolve("flying-orm-rdb-3.1.0.jar");

        requireRegularFile(manifest, "baseline manifest");
        requireRegularFile(core, "core baseline JAR");
        requireRegularFile(rdb, "RDB baseline JAR");
        requireEquals(MANIFEST_SHA256, sha256(manifest), "baseline manifest SHA-256");
        requireArtifact(core, CORE_LENGTH, CORE_SHA256, "core");
        requireArtifact(rdb, RDB_LENGTH, RDB_SHA256, "RDB");

        Properties identity = new Properties();
        try (InputStream input = Files.newInputStream(manifest)) {
            identity.load(input);
        }
        requireEquals(BASELINE_COMMIT, identity.getProperty("baseline.commit"), "baseline commit");
        requireEquals("3.1.0", identity.getProperty("baseline.source.branch"), "baseline branch");
        requireEquals("3.1.0", identity.getProperty("baseline.project.version"), "baseline version");
        requireEquals(core.getFileName().toString(), identity.getProperty("core.file"), "core file name");
        requireEquals(rdb.getFileName().toString(), identity.getProperty("rdb.file"), "RDB file name");
        requireEquals(Long.toString(CORE_LENGTH), identity.getProperty("core.length"), "core length");
        requireEquals(Long.toString(RDB_LENGTH), identity.getProperty("rdb.length"), "RDB length");
        requireEquals(CORE_SHA256, identity.getProperty("core.sha256"), "core manifest SHA-256");
        requireEquals(RDB_SHA256, identity.getProperty("rdb.sha256"), "RDB manifest SHA-256");
        System.out.println("Verified frozen API/ABI baseline: " + BASELINE_COMMIT);
    }

    private static void requireRegularFile(Path path, String description) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path)) {
            throw new IllegalStateException(description + " is missing or is not a regular file: " + path);
        }
    }

    private static void requireArtifact(
            Path path,
            long expectedLength,
            String expectedSha256,
            String description) throws IOException, NoSuchAlgorithmException {
        long actualLength = Files.size(path);
        if (actualLength != expectedLength) {
            throw new IllegalStateException(description + " baseline length changed: expected="
                    + expectedLength + ", actual=" + actualLength);
        }
        requireEquals(expectedSha256, sha256(path), description + " baseline SHA-256");
    }

    private static void requireEquals(String expected, String actual, String description) {
        if (!expected.equals(actual)) {
            throw new IllegalStateException(description + " changed: expected="
                    + expected + ", actual=" + actual);
        }
    }

    private static String sha256(Path path) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[64 * 1024];
            int length;
            while ((length = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, length);
            }
        }
        return HexFormat.of().withUpperCase().formatHex(digest.digest());
    }
}
