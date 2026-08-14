package com.flying.orm.benchmark;

import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.RowMapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 测 record/Bean 映射和实体取值的本地开销。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EntityMappingBenchmark {

    private RowMapper<UserRecord> recordMapper;

    private RowMapper<UserBean> beanMapper;

    private EntityValues<UserRecord> recordValues;

    private EntityValues<UserBean> beanValues;

    private Map<String, Object> row;

    private UserRecord record;

    private UserBean bean;

    @Setup(Level.Trial)
    public void setUp() {
        recordMapper = RowMapper.of(UserRecord.class);
        beanMapper = RowMapper.of(UserBean.class);
        recordValues = EntityValues.createUncached(UserRecord.class);
        beanValues = EntityValues.createUncached(UserBean.class);
        row = row();
        record = new UserRecord(1L, "Alice", 18, true);
        bean = new UserBean();
        bean.setId(1L);
        bean.setName("Alice");
        bean.setAge(18);
        bean.setEnabled(true);
    }

    @Benchmark
    public UserRecord mapRecord() {
        return recordMapper.map(row);
    }

    @Benchmark
    public UserBean mapBean() {
        return beanMapper.map(row);
    }

    @Benchmark
    public Map<String, Object> readRecordValues() {
        return recordValues.read(record);
    }

    @Benchmark
    public Map<String, Object> readBeanValues() {
        return beanValues.read(bean);
    }

    private static Map<String, Object> row() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", 1L);
        values.put("NAME", "Alice");
        values.put("AGE", 18);
        values.put("ENABLED", true);
        return values;
    }

    public record UserRecord(Long id, String name, Integer age, Boolean enabled) {
    }

    public static final class UserBean {

        private Long id;

        private String name;

        private Integer age;

        private Boolean enabled;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }
    }
}
