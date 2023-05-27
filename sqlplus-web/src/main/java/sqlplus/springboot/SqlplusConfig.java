package sqlplus.springboot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import sqlplus.springboot.component.ExperimentManager;

import javax.annotation.PostConstruct;
import java.io.File;

@Component
public class SqlplusConfig {
    private final static Logger LOGGER = LoggerFactory.getLogger(SqlplusConfig.class);

    @Value("${sqlplus.home}")
    private String home;

    @Value("${experiment.data.path}")
    private String experimentDataPath;

    @Value("${experiment.mode:local}")
    private String experimentMode;

    @Value("${experiment.forwarding:false}")
    private String forwarding;

    @Value("${experiment.spark.master.host:localhost}")
    private String sparkMasterHost;

    @Value("${experiment.spark.master.port:7077}")
    private String sparkMasterPort;

    @Value("${experiment.spark.master.submission.port:6066}")
    private String sparkMasterSubmissionPort;

    @Value("${experiment.spark.master.web-ui.port:8080}")
    public String sparkMasterWebUIPort;

    @Value("${experiment.spark.driver.memory:1g}")
    private String sparkDriverMemory;

    @Value("${experiment.spark.driver.cores:1}")
    private String sparkDriverCores;

    @Value("${experiment.spark.executor.memory:2g}")
    private String sparkExecutorMemory;

    @Value("${experiment.spark.executor.cores:2}")
    private String sparkExecutorCores;

    @Value("${experiment.spark.default.parallelism:1}")
    private String sparkParallelism;

    @Value("${experiment.hdfs.host}")
    private String hdfsHost;

    @Value("${experiment.hdfs.port}")
    private String hdfsPort;

    @Value("${experiment.hdfs.path:/}")
    private String hdfsPath;

    @Value("${experiment.hdfs.user}")
    private String hdfsUser;

    @PostConstruct
    public void init() {
        if (home == null || home.equals("")) {
            home = System.getProperty("user.dir");
            LOGGER.warn("Property sqlplus.home is not set. Using " + home);
        }
    }

    public boolean isLocalMode() {
        return experimentMode.equalsIgnoreCase("local");
    }

    public boolean isForwarding() {
        return forwarding.equalsIgnoreCase("true");
    }

    public String getHome() {
        return home;
    }

    public String getLibHome() {
        return home + File.separator + "sqlplus-lib";
    }

    public String getLocalLibJarPath() {
        return getLibHome() + File.separator + "target" + File.separator + "sparksql-plus-lib.jar";
    }

    public String getRemoteLibJarPath() {
        return hdfsPath + File.separator + "sparksql-plus-lib.jar";
    }

    public String getExampleHome() {
        return home + File.separator + "sqlplus-example";
    }

    public String getLocalExampleJarPath() {
        return getExampleHome() + File.separator + "target" + File.separator + "sparksql-plus-example.jar";
    }

    public String getRemoteExampleJarPath() {
        return hdfsPath + File.separator + "sparksql-plus-example.jar";
    }

    public String getExperimentDataPath() {
        return experimentDataPath;
    }

    public String getSparkMasterHost() {
        return sparkMasterHost;
    }

    public String getSparkMasterPort() {
        return sparkMasterPort;
    }

    public String getSparkMasterSubmissionPort() {
        return sparkMasterSubmissionPort;
    }

    public String getSparkMasterWebUIPort() {
        return sparkMasterWebUIPort;
    }

    public String getSparkDriverMemory() {
        return sparkDriverMemory;
    }

    public String getSparkDriverCores() {
        return sparkDriverCores;
    }

    public String getSparkExecutorMemory() {
        return sparkExecutorMemory;
    }

    public String getSparkExecutorCores() {
        return sparkExecutorCores;
    }

    public String getSparkParallelism() {
        return sparkParallelism;
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public String getHdfsPort() {
        return hdfsPort;
    }

    public String getHdfsUser() {
        return hdfsUser;
    }
}
