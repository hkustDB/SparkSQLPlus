package sqlplus.springboot.component;

import sqlplus.springboot.util.ExperimentJarBuilder;
import sqlplus.springboot.util.ExperimentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class ExperimentManager {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentManager.class);

    private ExperimentStatus status = ExperimentStatus.STOPPED;

    @Value("${experiment.timeout:3600}")
    private int timeout;

    private final List<String> experimentNames = new ArrayList<>();
    private final Map<String, String> experimentStatus = new HashMap<>();
    private final Map<String, Double> experimentResults = new HashMap<>();

    private Optional<String> runningExperimentName = Optional.empty();

    private String runningExperimentSubmissionId = "";

    private long runningExperimentSubmitTimestamp = 0L;

    private final BlockingQueue<String> pendingExperiments = new LinkedBlockingQueue<>();

    private final ExperimentHandler handler;

    private final ExperimentResultRetriever retriever;

    @Autowired
    public ExperimentManager(ExperimentHandler handler, ExperimentResultRetriever retriever) {
        this.handler = handler;
        this.retriever = retriever;
    }

    @Scheduled(fixedDelay = 10000)
    public void schedule() throws InterruptedException {
        if (status == ExperimentStatus.RUNNING) {
            if (runningExperimentName.isPresent()) {
                // we have submitted an application
                // check whether it has already finished
                Optional<String> optDriverState = handler.status(runningExperimentSubmissionId);
                if (optDriverState.isPresent()) {
                    if (!optDriverState.get().equalsIgnoreCase("FINISHED")) {
                        long duration = System.currentTimeMillis() - runningExperimentSubmitTimestamp;
                        // check whether it has exceeded the time limit
                        if (duration >= timeout * 1000L) {
                            LOGGER.info(runningExperimentName.get() + " has run for more than " + timeout + " s.");
                            handler.kill(runningExperimentSubmissionId);
                            fail(runningExperimentName.get(), "timeout");
                            cleanRunningExperimentInfo();
                        }
                    } else {
                        // the application has finished, retrieve the result
                        Optional<Double> time = retriever.retrieve(runningExperimentName.get(), runningExperimentSubmitTimestamp);
                        if (time.isPresent()) {
                            LOGGER.info(runningExperimentName.get() + " running time: " + time);
                            success(runningExperimentName.get(), time.get());
                        } else {
                            LOGGER.info("can not retrieve result of " + runningExperimentName.get());
                            fail(runningExperimentName.get(), "error");
                        }
                        cleanRunningExperimentInfo();
                    }
                } else {
                    LOGGER.error("status failed");
                }
            } else {
                // currently no running application
                // try to take the next experiment from the queue
                String head = pendingExperiments.take();
                String classname = "";
                if (head.startsWith("CustomQuery")) {
                    Pattern pattern = Pattern.compile("CustomQuery(\\d+)-SparkSQL*");
                    Matcher matcher = pattern.matcher(head);
                    if (matcher.find()) {
                        String index = matcher.group(1);
                        classname = "sqlplus.example.custom.q" + index + "." + head.replaceAll("-", "");
                    } else {
                        fail(head, "invalid CustomQuery " + head);
                    }
                } else {
                    classname = "sqlplus.example." + head.replaceAll("-", "");
                }
                Optional<String> optSubmissionId = handler.create(
                        // experiment name: Query1-SparkSQLPlus, class name: cqc.example.Query1SparkSQLPlus
                        // experiment name: CustomQuery1-SparkSQLPlus, class name: cqc.example.custom.q1.CustomQuery1SparkSQLPlus
                        classname,
                        head);
                if (optSubmissionId.isPresent()) {
                    LOGGER.info("get submission id " + optSubmissionId.get());
                    setRunningExperimentInfo(optSubmissionId.get(), head);
                    begin(head);
                } else {
                    LOGGER.error("create failed");
                    cleanRunningExperimentInfo();
                    fail(head, "error");
                }
            }
        }
    }

    private synchronized void cleanRunningExperimentInfo() {
        runningExperimentSubmissionId = "";
        runningExperimentSubmitTimestamp = 0L;
        runningExperimentName = Optional.empty();
    }

    private synchronized void setRunningExperimentInfo(String submissionId, String experimentName) {
        runningExperimentSubmissionId = submissionId;
        runningExperimentSubmitTimestamp = System.currentTimeMillis();
        runningExperimentName = Optional.of(experimentName);
    }

    public ExperimentStatus getStatus() {
        return status;
    }

    public List<String> getExperimentNames() {
        return experimentNames;
    }

    public Map<String, String> getExperimentStatus() {
        return experimentStatus;
    }

    public Map<String, Double> getExperimentResults() {
        return experimentResults;
    }

    public synchronized void addPendingExperiment(String experiment) {
        if (status.equals(ExperimentStatus.STOPPED)) {
            // Query1, Query2, ...
            experimentNames.add(experiment);

            String sparkCqcExperiment = experiment + "-SparkSQLPlus";
            String sparkSqlExperiment = experiment + "-SparkSQL";
            pendingExperiments.add(sparkCqcExperiment);
            pendingExperiments.add(sparkSqlExperiment);
            experimentStatus.put(sparkCqcExperiment, "pending");
            experimentStatus.put(sparkSqlExperiment, "pending");
        } else {
            LOGGER.error("can not add experiments to a running manager");
        }
    }

    public synchronized void start() {
        if (status.equals(ExperimentStatus.STOPPED)) {
            boolean isBuildSuccess = ExperimentJarBuilder.build();
            if (!isBuildSuccess) {
                LOGGER.error("failed to build experiment jar.");
            } else {
                status = ExperimentStatus.RUNNING;
            }
        } else {
            LOGGER.error("can not add experiments to a running manager");
        }
    }

    private synchronized void begin(String name) {
        experimentStatus.put(name, "running");
    }

    private synchronized void success(String name, Double time) {
        experimentStatus.put(name, "finished");
        experimentResults.put(name, time);
    }

    private synchronized void fail(String name, String state) {
        // timeout or error
        experimentStatus.put(name, state);
    }
}