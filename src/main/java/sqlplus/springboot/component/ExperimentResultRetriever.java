package sqlplus.springboot.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class ExperimentResultRetriever {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentResultRetriever.class);

    @Value("${experiment.result.path:/tmp/spark-cqc-demo.log}")
    private String resultLocation;

    public Optional<Double> retrieve(String experimentName, long submitTimestamp) {
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
                // parse the timestamp part, don't report the results of previous runs
                Pattern pattern = Pattern.compile("([0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]) INFO SparkSQLPlusExperiment: " + experimentName + " time: (.+)");
                Matcher matcher = pattern.matcher(lastMatch);
                if (matcher.find()) {
                    String g1 = matcher.group(1);
                    String g2 = matcher.group(2);
                    SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
                    long timestamp = format.parse(g1).getTime();
                    if (timestamp < submitTimestamp) {
                        return Optional.empty();
                    } else {
                        return Optional.of(Double.valueOf(g2));
                    }
                } else {
                    LOGGER.error("can not extract result from line: " + lastMatch);
                    return Optional.empty();
                }
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
