package sqlplus.springboot.util;

public enum ExperimentStatus {
    RUNNING("running"), STOPPED("stopped");

    public String status;

    ExperimentStatus(String status) {
        this.status = status;
    }
}