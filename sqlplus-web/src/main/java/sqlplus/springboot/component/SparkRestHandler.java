package sqlplus.springboot.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import sqlplus.springboot.SqlplusConfig;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class SparkRestHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger(SparkRestHandler.class);

    private final SqlplusConfig config;

    private WebClient client;

    @Autowired
    public SparkRestHandler(SqlplusConfig config) {
        this.config = config;
    }

    @PostConstruct
    public void init() {
        String host = config.isForwarding() ? "localhost" : config.getSparkMasterHost();
        this.client = WebClient.builder()
                .baseUrl("http://" + host + ":" + config.getSparkMasterSubmissionPort() + "/v1/submissions")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    public Optional<String> create(String className, String appName) {
        SparkSubmissionsCreateRequest request = new SparkSubmissionsCreateRequest();

        String exampleJarPath =
                config.isLocalMode() ? config.getLocalExampleJarPath() : "hdfs://" + config.getRemoteExampleJarPath();

        String libJarPath =
                config.isLocalMode() ? config.getLocalLibJarPath() : "hdfs://" + config.getRemoteLibJarPath();

        String dataPath =
                config.isLocalMode() ? config.getExperimentDataPath() : "hdfs://" + config.getExperimentDataPath();

        request.setAppResource(exampleJarPath);
        request.setAppArgs(Arrays.asList(dataPath));
        request.setMainClass(className);
        request.addSparkProperty("spark.master", "spark://" + config.getSparkMasterHost() + ":" + config.getSparkMasterPort());
        request.addSparkProperty("spark.app.name", appName);
        request.addSparkProperty("spark.jars", exampleJarPath + "," + libJarPath);  // DO NOT remove the exampleJarPath here!
        request.addSparkProperty("spark.driver.memory", config.getSparkDriverMemory());
        request.addSparkProperty("spark.driver.cores", config.getSparkDriverCores());
        request.addSparkProperty("spark.executor.memory", config.getSparkExecutorMemory());
        request.addSparkProperty("spark.executor.cores", config.getSparkExecutorCores());
        request.addSparkProperty("spark.default.parallelism", config.getSparkParallelism());
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
