package sqlplus.springboot.component;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

@Component
@ConditionalOnProperty(value = "experiment.result.type", havingValue = "local")
public class ExperimentResultLocalRetriever extends AbstractExperimentResultRetriever {
    @Value("${experiment.result.path:/tmp/sparksql-plus.log}")
    private String resultLocation;

    @Override
    public Optional<Double> retrieve(String submissionId, String experimentName, long submitTimestamp) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(resultLocation))));
            String lastMatch = null;
            String line = reader.readLine();
            while (line != null) {
                if (line.contains(experimentName + " time: ")) {
                    lastMatch = line;
                }
                line = reader.readLine();
            }

            if (lastMatch == null) {
                // this happens when the app is killed or throws exception during execution
                return Optional.empty();
            } else {
                return extractExperimentResult(lastMatch, experimentName, submitTimestamp);
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public boolean validateExperimentResult(long logTimestamp, long submitTimestamp) {
        return logTimestamp > submitTimestamp;
    }
}
