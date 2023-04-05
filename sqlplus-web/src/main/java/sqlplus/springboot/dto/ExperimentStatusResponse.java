package sqlplus.springboot.dto;

import java.util.List;
import java.util.Map;

public class ExperimentStatusResponse {
    private String experimentState;
    private List<String> experimentTaskNames;
    private Map<String, String> experimentTaskStates;
    private Map<String, Double> experimentTaskResults;

    public String getExperimentState() {
        return experimentState;
    }

    public void setExperimentState(String experimentState) {
        this.experimentState = experimentState;
    }

    public List<String> getExperimentTaskNames() {
        return experimentTaskNames;
    }

    public void setExperimentTaskNames(List<String> experimentTaskNames) {
        this.experimentTaskNames = experimentTaskNames;
    }

    public Map<String, String> getExperimentTaskStates() {
        return experimentTaskStates;
    }

    public void setExperimentTaskStates(Map<String, String> experimentTaskStates) {
        this.experimentTaskStates = experimentTaskStates;
    }

    public Map<String, Double> getExperimentTaskResults() {
        return experimentTaskResults;
    }

    public void setExperimentTaskResults(Map<String, Double> experimentTaskResults) {
        this.experimentTaskResults = experimentTaskResults;
    }
}
