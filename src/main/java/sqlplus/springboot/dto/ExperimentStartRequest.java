package sqlplus.springboot.dto;

import java.util.List;

public class ExperimentStartRequest {
    private List<String> experiments;

    public List<String> getExperiments() {
        return experiments;
    }

    public void setExperiments(List<String> experiments) {
        this.experiments = experiments;
    }
}
