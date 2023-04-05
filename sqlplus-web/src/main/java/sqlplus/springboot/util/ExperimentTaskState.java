package sqlplus.springboot.util;

public enum ExperimentTaskState {
    PENDING("Pending"), RUNNING("Running"), FINISHED("Finished"), TIMEOUT("Timeout"), FAILED("Failed"), CANCELLED("Cancelled");

    public String state;

    ExperimentTaskState(String state) {
        this.state = state;
    }

    public ExperimentTaskState submit() {
        if (this.equals(PENDING)) {
            return RUNNING;
        } else {
            throw new UnsupportedOperationException("submit on state " + this.state);
        }
    }

    public ExperimentTaskState cancel() {
        if (this.equals(PENDING)) {
            return CANCELLED;
        } else {
            throw new UnsupportedOperationException("submit on state " + this.state);
        }
    }

    public ExperimentTaskState finish() {
        if (this.equals(RUNNING)) {
            return FINISHED;
        } else {
            throw new UnsupportedOperationException("finish on state " + this.state);
        }
    }

    public ExperimentTaskState fail() {
        if (this.equals(RUNNING)) {
            return FAILED;
        } else {
            throw new UnsupportedOperationException("fail on state " + this.state);
        }
    }

    public ExperimentTaskState timeout() {
        if (this.equals(RUNNING)) {
            return TIMEOUT;
        } else {
            throw new UnsupportedOperationException("timeout on state " + this.state);
        }
    }
}