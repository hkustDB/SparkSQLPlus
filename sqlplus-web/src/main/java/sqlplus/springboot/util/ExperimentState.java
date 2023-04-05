package sqlplus.springboot.util;

public enum ExperimentState {
    STOPPED("Stopped"), COMPILING("Compiling"), COMPILATION_FAILED("Compilation Failed"), RUNNING("Running"), STOPPING("Stopping");

    public String state;

    ExperimentState(String state) {
        this.state = state;
    }

    public ExperimentState compile() {
        if (this.equals(STOPPED) || this.equals(COMPILATION_FAILED)) {
            return COMPILING;
        } else {
            throw new UnsupportedOperationException("compile on state " + this.state);
        }
    }

    public ExperimentState submit() {
        if (this.equals(COMPILING)) {
            return RUNNING;
        } else {
            throw new UnsupportedOperationException("submit on state " + this.state);
        }
    }

    public ExperimentState fail() {
        if (this.equals(COMPILING)) {
            return COMPILATION_FAILED;
        } else {
            throw new UnsupportedOperationException("fail on state " + this.state);
        }
    }

    public ExperimentState stop() {
        if (this.equals(RUNNING)) {
            return STOPPING;
        } else {
            throw new UnsupportedOperationException("stop on state " + this.state);
        }
    }

    public ExperimentState finish() {
        if (this.equals(RUNNING)) {
            return STOPPED;
        } else {
            throw new UnsupportedOperationException("finish on state " + this.state);
        }
    }

    public ExperimentState terminate() {
        if (this.equals(STOPPING)) {
            return STOPPED;
        } else {
            throw new UnsupportedOperationException("finish on state " + this.state);
        }
    }
}