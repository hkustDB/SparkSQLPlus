package sqlplus.springboot.component;

import sqlplus.springboot.util.ExperimentJarBuilder;
import sqlplus.springboot.util.ExperimentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import sqlplus.springboot.util.ExperimentTaskState;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class ExperimentManager {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentManager.class);

    private ExperimentState experimentState = ExperimentState.STOPPED;

    @Value("${experiment.timeout:3600}")
    private int timeout;

    private final List<String> experimentNames = new ArrayList<>();
    private final Map<String, ExperimentTaskState> experimentTaskStates = new HashMap<>();
    private final Map<String, Double> experimentTaskResults = new HashMap<>();

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
    public synchronized void schedule() throws InterruptedException {
        if (experimentState == ExperimentState.RUNNING) {
            if (runningExperimentName.isPresent()) {
                // we have submitted an application
                // check whether it has already finished
                Optional<String> optDriverState = handler.status(runningExperimentSubmissionId);
                if (optDriverState.isPresent()) {
                    if (optDriverState.get().equalsIgnoreCase("FINISHED")) {
                        // the application has finished, retrieve the result
                        Optional<Double> time = retriever.retrieve(runningExperimentName.get(), runningExperimentSubmitTimestamp);
                        if (time.isPresent()) {
                            LOGGER.info(runningExperimentName.get() + " running time: " + time);
                            experimentTaskSuccess(runningExperimentName.get(), time.get());
                        } else {
                            LOGGER.info("can not retrieve result of " + runningExperimentName.get());
                            experimentTaskFail(runningExperimentName.get());
                        }
                        cleanRunningExperimentInfo();
                        if (pendingExperiments.isEmpty()) {
                            experimentState = this.experimentState.finish();
                        }
                    } else if (optDriverState.get().equalsIgnoreCase("KILLED")) {
                        experimentTaskFail(runningExperimentName.get());
                        cleanRunningExperimentInfo();
                        if (pendingExperiments.isEmpty()) {
                            experimentState = this.experimentState.finish();
                        }
                    } else if (optDriverState.get().equalsIgnoreCase("RUNNING")) {
                        long duration = System.currentTimeMillis() - runningExperimentSubmitTimestamp;
                        // check whether it has exceeded the time limit
                        if (duration >= timeout * 1000L) {
                            LOGGER.info(runningExperimentName.get() + " has run for more than " + timeout + " s.");
                            boolean killed = handler.kill(runningExperimentSubmissionId);
                            if (killed) {
                                experimentTaskTimeout(runningExperimentName.get());
                                cleanRunningExperimentInfo();
                                if (pendingExperiments.isEmpty()) {
                                    experimentState = this.experimentState.finish();
                                }
                            }
                        }
                    } else {
                        LOGGER.error("received unrecognized state " + optDriverState.get());
                    }
                } else {
                    LOGGER.error("status failed. check the connection with Spark master.");
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
                        LOGGER.error("invalid CustomQuery " + head);
                        experimentTaskFail(head);
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
                    experimentTaskBegin(head);
                } else {
                    LOGGER.error("create failed");
                    cleanRunningExperimentInfo();
                    experimentTaskFail(head);
                }
            }
        } else if (experimentState == ExperimentState.STOPPING) {
            if (runningExperimentName.isPresent()) {
                // we have submitted an application
                // check whether it has already finished
                Optional<String> optDriverState = handler.status(runningExperimentSubmissionId);
                if (optDriverState.isPresent()) {
                    if (optDriverState.get().equalsIgnoreCase("FINISHED")) {
                        // the application has finished, retrieve the result
                        Optional<Double> time = retriever.retrieve(runningExperimentName.get(), runningExperimentSubmitTimestamp);
                        if (time.isPresent()) {
                            LOGGER.info(runningExperimentName.get() + " running time: " + time);
                            experimentTaskSuccess(runningExperimentName.get(), time.get());
                        } else {
                            LOGGER.info("can not retrieve result of " + runningExperimentName.get());
                            experimentTaskFail(runningExperimentName.get());
                        }
                        cleanRunningExperimentInfo();
                        pendingExperiments.forEach(this::experimentTaskCancel);
                        pendingExperiments.clear();
                        experimentState = this.experimentState.terminate();
                    } else if (optDriverState.get().equalsIgnoreCase("KILLED")) {
                        experimentTaskFail(runningExperimentName.get());
                        cleanRunningExperimentInfo();
                        pendingExperiments.forEach(this::experimentTaskCancel);
                        pendingExperiments.clear();
                        experimentState = this.experimentState.terminate();
                    } else if (optDriverState.get().equalsIgnoreCase("RUNNING")) {
                        boolean killed = handler.kill(runningExperimentSubmissionId);
                        if (killed) {
                            experimentTaskFail(runningExperimentName.get());
                            cleanRunningExperimentInfo();
                            pendingExperiments.forEach(this::experimentTaskCancel);
                            pendingExperiments.clear();
                            experimentState = this.experimentState.terminate();
                        }
                    } else {
                        LOGGER.error("received unrecognized state " + optDriverState.get());
                    }
                } else {
                    LOGGER.error("status failed. check the connection with Spark master.");
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

    public ExperimentState getExperimentState() {
        return experimentState;
    }

    public List<String> getExperimentNames() {
        return experimentNames;
    }

    public Map<String, String> getExperimentTaskStates() {
        return experimentTaskStates.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().state));
    }

    public Map<String, Double> getExperimentTaskResults() {
        return experimentTaskResults;
    }

    public synchronized void clear() {
        if (experimentState.equals(ExperimentState.STOPPED)) {
            experimentNames.clear();
            pendingExperiments.clear();
            experimentTaskStates.clear();
            experimentTaskResults.clear();
            runningExperimentName = Optional.empty();
            runningExperimentSubmissionId = "";
            runningExperimentSubmitTimestamp = 0L;
        }
    }

    public synchronized void addPendingExperiment(String experiment) {
        if (experimentState.equals(ExperimentState.STOPPED)) {
            // Query1, Query2, ...
            experimentNames.add(experiment);

            String sparkCqcExperiment = experiment + "-SparkSQLPlus";
            String sparkSqlExperiment = experiment + "-SparkSQL";
            pendingExperiments.add(sparkCqcExperiment);
            pendingExperiments.add(sparkSqlExperiment);
            experimentTaskStates.put(sparkCqcExperiment, ExperimentTaskState.PENDING);
            experimentTaskStates.put(sparkSqlExperiment, ExperimentTaskState.PENDING);
        } else {
            LOGGER.error("can not add experiments to a running manager");
        }
    }

    public synchronized void start() {
        if (experimentState.equals(ExperimentState.STOPPED)) {
            experimentState = experimentState.compile();
            boolean isBuildSuccess = ExperimentJarBuilder.build();
            if (!isBuildSuccess) {
                experimentTaskStates.keySet().forEach(k -> experimentTaskStates.put(k, ExperimentTaskState.FAILED));
                experimentState = experimentState.fail();
                LOGGER.error("failed to build experiment jar.");
            } else {
                experimentState = experimentState.submit();
            }
        } else {
            LOGGER.error("experiment has been started.");
        }
    }

    public synchronized void stop() {
        if (experimentState.equals(ExperimentState.RUNNING)) {
            experimentState = experimentState.stop();
        }
    }

    private synchronized void experimentTaskBegin(String name) {
        experimentTaskStates.put(name, experimentTaskStates.get(name).submit());
    }

    private synchronized void experimentTaskSuccess(String name, Double time) {
        experimentTaskStates.put(name, experimentTaskStates.get(name).finish());
        experimentTaskResults.put(name, time);
    }

    private synchronized void experimentTaskFail(String name) {
        // timeout or error
        experimentTaskStates.put(name, experimentTaskStates.get(name).fail());
    }

    private synchronized void experimentTaskTimeout(String name) {
        // timeout or error
        experimentTaskStates.put(name, experimentTaskStates.get(name).timeout());
    }

    private synchronized void experimentTaskCancel(String name) {
        // timeout or error
        experimentTaskStates.put(name, experimentTaskStates.get(name).cancel());
    }
}