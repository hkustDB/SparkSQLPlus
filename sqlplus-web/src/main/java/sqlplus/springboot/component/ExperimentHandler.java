package sqlplus.springboot.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

@Component
public class ExperimentHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentHandler.class);

    @Value("${experiment.spark.master.host:localhost}")
    public String masterHost;

    @Value("${experiment.spark.master.port:7077}")
    public String masterPort;

    @Value("${experiment.spark.master.submission.port:6066}")
    public String masterSubmissionPort;

    @Value("${experiment.spark.driver.memory:1g}")
    public String driverMemory;

    @Value("${experiment.spark.driver.cores:1}")
    public String driverCores;

    @Value("${experiment.spark.executor.memory:2g}")
    public String executorMemory;

    @Value("${experiment.spark.executor.cores:2}")
    public String executorCores;

    @Value("${experiment.spark.default.parallelism:1}")
    public String parallelism;

    @Value("${experiment.example-jar}")
    private String exampleJarPath;

    @Value("${experiment.lib-jar}")
    private String libJarPath;

    @Value("${experiment.data}")
    private String dataPath;

    private WebClient client;

    @PostConstruct
    public void init() {
        this.client = WebClient.builder()
                .baseUrl("http://" + masterHost + ":" + masterSubmissionPort + "/v1/submissions")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();

        if (exampleJarPath == null || exampleJarPath.equals(""))
            exampleJarPath = System.getProperty("user.dir") + File.separator + "sqlplus-example"
                    + File.separator + "target" + File.separator + "sparksql-plus-example.jar";

        if (libJarPath == null || libJarPath.equals(""))
            libJarPath = System.getProperty("user.dir") + File.separator + "sqlplus-lib"
                    + File.separator + "target" + File.separator + "sparksql-plus-lib.jar";

        if (dataPath == null || dataPath.equals(""))
            dataPath = System.getProperty("user.dir") + File.separator + "examples" + File.separator + "data";
    }

    public Optional<String> create(String className, String appName) {
        SparkSubmissionsCreateRequest request = new SparkSubmissionsCreateRequest();
        request.setAppResource(exampleJarPath);
        request.setAppArgs(Arrays.asList(dataPath));
        request.setMainClass(className);
        request.addSparkProperty("spark.master", "spark://" + masterHost + ":" + masterPort);
        request.addSparkProperty("spark.app.name", appName);
        request.addSparkProperty("spark.jars", exampleJarPath + "," + libJarPath);  // DO NOT remove the exampleJarPath here!
        request.addSparkProperty("spark.driver.memory", driverMemory);
        request.addSparkProperty("spark.driver.cores", driverCores);
        request.addSparkProperty("spark.executor.memory", executorMemory);
        request.addSparkProperty("spark.executor.cores", executorCores);
        request.addSparkProperty("spark.default.parallelism", parallelism);
        request.addSparkProperty("spark.eventLog.enabled", "false");
        request.addSparkProperty("spark.submit.deployMode", "cluster");
        request.addSparkProperty("spark.driver.supervise", "false");

        try {
            SparkSubmissionsCreateResponse response = client.post().uri("/create")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(SparkSubmissionsCreateResponse.class)
                    .block();
            if (response != null && response.success)
                return Optional.of(response.submissionId);
            else
                return Optional.empty();
        } catch (Exception e) {
            LOGGER.error("failed to call create", e);
            return Optional.empty();
        }
    }

    public Optional<String> status(String submissionId) {
        SparkSubmissionsStatusResponse response = client.get().uri("/status/{submissionId}", submissionId)
                .retrieve()
                .bodyToMono(SparkSubmissionsStatusResponse.class)
                .block();

        try {
            if (response != null && response.success) {
                return Optional.of(response.driverState);
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            LOGGER.error("failed to call status", e);
            return Optional.empty();
        }
    }

    public boolean kill(String submissionId) {
        try {
            SparkSubmissionsKillResponse response = client.post().uri("/kill/{submissionId}", submissionId)
                    .retrieve()
                    .bodyToMono(SparkSubmissionsKillResponse.class)
                    .block();
            return  response != null && response.success;
        } catch (Exception e) {
            LOGGER.error("failed to call kill", e);
            return false;
        }
    }

    public static class SparkSubmissionsCreateRequest {
        public String appResource;
        public Map<String, String> sparkProperties = new HashMap<>();
        public String clientSparkVersion = "3.0.1";
        public String mainClass;
        public Map<String, String> environmentVariables = new HashMap<>();
        public String action = "CreateSubmissionRequest";
        public List<String> appArgs = new ArrayList<>();

        public String getAppResource() {
            return appResource;
        }

        public Map<String, String> getSparkProperties() {
            return sparkProperties;
        }

        public String getClientSparkVersion() {
            return clientSparkVersion;
        }

        public String getMainClass() {
            return mainClass;
        }

        public Map<String, String> getEnvironmentVariables() {
            return environmentVariables;
        }

        public String getAction() {
            return action;
        }

        public List<String> getAppArgs() {
            return appArgs;
        }

        public void setAppArgs(List<String> appArgs) {
            this.appArgs = appArgs;
        }

        public void setAppResource(String appResource) {
            this.appResource = appResource;
        }

        public void setMainClass(String mainClass) {
            this.mainClass = mainClass;
        }

        public void addSparkProperty(String key, String value) {
            this.sparkProperties.put(key, value);
        }
    }

    public static class SparkSubmissionsCreateResponse {
        public String action;
        public String message;
        public String serverSparkVersion;
        public String submissionId;
        public boolean success;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getServerSparkVersion() {
            return serverSparkVersion;
        }

        public void setServerSparkVersion(String serverSparkVersion) {
            this.serverSparkVersion = serverSparkVersion;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }

    public static class SparkSubmissionsStatusResponse {
        public String action;
        public String driverState;
        public String serverSparkVersion;
        public String submissionId;
        public boolean success;
        public String workerHostPort;
        public String workerId;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getDriverState() {
            return driverState;
        }

        public void setDriverState(String driverState) {
            this.driverState = driverState;
        }

        public String getServerSparkVersion() {
            return serverSparkVersion;
        }

        public void setServerSparkVersion(String serverSparkVersion) {
            this.serverSparkVersion = serverSparkVersion;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getWorkerHostPort() {
            return workerHostPort;
        }

        public void setWorkerHostPort(String workerHostPort) {
            this.workerHostPort = workerHostPort;
        }

        public String getWorkerId() {
            return workerId;
        }

        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }
    }

    public static class SparkSubmissionsKillResponse {
        public String action;
        public String message;
        public String serverSparkVersion;
        public String submissionId;
        public boolean success;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getServerSparkVersion() {
            return serverSparkVersion;
        }

        public void setServerSparkVersion(String serverSparkVersion) {
            this.serverSparkVersion = serverSparkVersion;
        }

        public String getSubmissionId() {
            return submissionId;
        }

        public void setSubmissionId(String submissionId) {
            this.submissionId = submissionId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }
    }
}
