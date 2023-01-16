package sqlplus.springboot.dto;

import java.util.List;
import java.util.Map;

public class ExperimentStatusResponse {
    private String status;
    private List<String> experimentNames;
    private Map<String, String> experimentStatus;
    private Map<String, Double> experimentResults;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<String> getExperimentNames() {
        return experimentNames;
    }

    public void setExperimentNames(List<String> experimentNames) {
        this.experimentNames = experimentNames;
    }

    public Map<String, String> getExperimentStatus() {
        return experimentStatus;
    }

    public void setExperimentStatus(Map<String, String> experimentStatus) {
        this.experimentStatus = experimentStatus;
    }

    public Map<String, Double> getExperimentResults() {
        return experimentResults;
    }

    public void setExperimentResults(Map<String, Double> experimentResults) {
        this.experimentResults = experimentResults;
    }
}
