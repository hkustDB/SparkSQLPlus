package sqlplus.springboot.component;

import sqlplus.springboot.SqlplusConfig;
import sqlplus.springboot.dto.ExperimentStatusResponse;
import sqlplus.springboot.util.ExperimentJarBuilder;
import sqlplus.springboot.util.ExperimentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import sqlplus.springboot.util.ExperimentTaskState;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

    private final SqlplusConfig sqlplusConfig;

    private final SparkRestHandler sparkRestHandler;

    private final WebHdfsHandler webHdfsHandler;

    private final AbstractExperimentResultRetriever retriever;

    private Lock readLock;
    private Lock writeLock;

    @PostConstruct
    public void init() {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
    }

    @Autowired
    public ExperimentManager(SqlplusConfig sqlplusConfig, SparkRestHandler sparkRestHandler, WebHdfsHandler webHdfsHandler,
                             AbstractExperimentResultRetriever retriever) {
        this.sqlplusConfig = sqlplusConfig;
        this.sparkRestHandler = sparkRestHandler;
        this.webHdfsHandler = webHdfsHandler;
        this.retriever = retriever;
    }

    @Scheduled(fixedDelay = 10000)
    public synchronized void schedule() throws InterruptedException {
        writeLock.lock();
        try {
            if (experimentState == ExperimentState.RUNNING) {
                if (runningExperimentName.isPresent()) {
                    // we have submitted an application
                    // check whether it has already finished
                    Optional<String> optDriverState = sparkRestHandler.status(runningExperimentSubmissionId);
                    if (optDriverState.isPresent()) {
                        if (optDriverState.get().equalsIgnoreCase("FINISHED")) {
                            // the application has finished, retrieve the result
                            Optional<Double> time = retriever.retrieve(runningExperimentSubmissionId,
                                    runningExperimentName.get(), runningExperimentSubmitTimestamp);
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
                        } else if (optDriverState.get().equalsIgnoreCase("FAILED")) {
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
                                boolean killed = sparkRestHandler.kill(runningExperimentSubmissionId);
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
                    LOGGER.info("try to submit " + head);
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
                    Optional<String> optSubmissionId = sparkRestHandler.create(
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
                        if (pendingExperiments.isEmpty()) {
                            experimentState = this.experimentState.finish();
                        }
                    }
                }
            } else if (experimentState == ExperimentState.STOPPING) {
                if (runningExperimentName.isPresent()) {
                    // we have submitted an application
                    // check whether it has already finished
                    Optional<String> optDriverState = sparkRestHandler.status(runningExperimentSubmissionId);
                    if (optDriverState.isPresent()) {
                        if (optDriverState.get().equalsIgnoreCase("FINISHED")) {
                            // the application has finished, retrieve the result
                            Optional<Double> time = retriever.retrieve(runningExperimentSubmissionId,
                                    runningExperimentName.get(), runningExperimentSubmitTimestamp);
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
                        } else if (optDriverState.get().equalsIgnoreCase("FAILED")) {
                            experimentTaskFail(runningExperimentName.get());
                            cleanRunningExperimentInfo();
                            pendingExperiments.forEach(this::experimentTaskCancel);
                            pendingExperiments.clear();
                            experimentState = this.experimentState.terminate();
                        } else if (optDriverState.get().equalsIgnoreCase("RUNNING")) {
                            boolean killed = sparkRestHandler.kill(runningExperimentSubmissionId);
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
                } else {
                    // currently no running application
                    // mark all the pending tasks as Cancelled
                    cleanRunningExperimentInfo();
                    pendingExperiments.forEach(this::experimentTaskCancel);
                    pendingExperiments.clear();
                    experimentState = this.experimentState.terminate();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void cleanRunningExperimentInfo() {
        runningExperimentSubmissionId = "";
        runningExperimentSubmitTimestamp = 0L;
        runningExperimentName = Optional.empty();
    }

    private void setRunningExperimentInfo(String submissionId, String experimentName) {
        runningExperimentSubmissionId = submissionId;
        runningExperimentSubmitTimestamp = System.currentTimeMillis();
        runningExperimentName = Optional.of(experimentName);
    }

    public void fillExperimentStatusResponse(ExperimentStatusResponse response) {
        readLock.lock();
        try {
            response.setExperimentState(experimentState.state);
            response.setExperimentTaskNames(Collections.unmodifiableList(experimentNames));

            switch (experimentState) {
                case COMPILING:
                    response.setExperimentTaskStates(experimentTaskStates.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> ExperimentState.COMPILING.state)));
                    break;
                case STOPPING:
                    response.setExperimentTaskStates(experimentTaskStates.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                        if (e.getValue().equals(ExperimentTaskState.PENDING) || e.getValue().equals(ExperimentTaskState.RUNNING)) {
                            return ExperimentState.STOPPING.state;
                        } else {
                            return e.getValue().state;
                        }
                    })));
                    break;
                default:
                    response.setExperimentTaskStates(experimentTaskStates.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().state)));
            }

            response.setExperimentTaskResults(Collections.unmodifiableMap(experimentTaskResults));
        } finally {
            readLock.unlock();
        }
    }

    public void start(List<String> experiments) {
        writeLock.lock();
        try {
            if (experimentState.equals(ExperimentState.STOPPED)) {
                experimentNames.clear();
                pendingExperiments.clear();
                experimentTaskStates.clear();
                experimentTaskResults.clear();
                runningExperimentName = Optional.empty();
                runningExperimentSubmissionId = "";
                runningExperimentSubmitTimestamp = 0L;

                experiments.forEach(e -> {
                    experimentNames.add(e);
                    String sparkCqcExperiment = e + "-SparkSQLPlus";
                    String sparkSqlExperiment = e + "-SparkSQL";
                    pendingExperiments.add(sparkCqcExperiment);
                    pendingExperiments.add(sparkSqlExperiment);
                    experimentTaskStates.put(sparkCqcExperiment, ExperimentTaskState.PENDING);
                    experimentTaskStates.put(sparkSqlExperiment, ExperimentTaskState.PENDING);
                });

                // STOPPED -> COMPILING
                experimentState = experimentState.compile();
            } else {
                LOGGER.error("can not start experiments when current state is " + experimentState.state);
                return;
            }
        } finally {
            writeLock.unlock();
        }

        boolean isBuildSuccess = ExperimentJarBuilder.build();

        if (!sqlplusConfig.isLocalMode()) {
            // remote mode, upload the jars before execution
            webHdfsHandler.uploadJars();
        }

        writeLock.lock();
        try {
            if (!isBuildSuccess) {
                experimentTaskStates.keySet().forEach(k -> experimentTaskStates.put(k, ExperimentTaskState.FAILED));
                experimentState = experimentState.fail();
                LOGGER.error("failed to build experiment jar.");
            } else {
                experimentState = experimentState.submit();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void stop() {
        writeLock.lock();
        try {
            if (experimentState.equals(ExperimentState.RUNNING)) {
                experimentState = experimentState.stop();
            } else {
                LOGGER.error("can not stop experiments when current state is " + experimentState.state);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void experimentTaskBegin(String name) {
        LOGGER.info("mark " + name + " as running.");
        experimentTaskStates.put(name, experimentTaskStates.get(name).submit());
    }

    private void experimentTaskSuccess(String name, Double time) {
        LOGGER.info("mark " + name + " as success.");
        experimentTaskStates.put(name, experimentTaskStates.get(name).finish());
        experimentTaskResults.put(name, time);
    }

    private void experimentTaskFail(String name) {
        LOGGER.info("mark " + name + " as failed.");
        experimentTaskStates.put(name, experimentTaskStates.get(name).fail());
    }

    private void experimentTaskTimeout(String name) {
        LOGGER.info("mark " + name + " as timeout.");
        experimentTaskStates.put(name, experimentTaskStates.get(name).timeout());
    }

    private void experimentTaskCancel(String name) {
        LOGGER.info("mark " + name + " as cancelled.");
        experimentTaskStates.put(name, experimentTaskStates.get(name).cancel());
    }
}