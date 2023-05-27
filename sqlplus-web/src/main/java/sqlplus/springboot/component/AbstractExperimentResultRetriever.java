package sqlplus.springboot.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractExperimentResultRetriever {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractExperimentResultRetriever.class);

    public abstract Optional<Double> retrieve(String submissionId, String experimentName, long submitTimestamp);

    protected Optional<Double> extractExperimentResult(String line, String experimentName, long submitTimestamp) {
        Pattern pattern = Pattern.compile("([0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]) INFO SparkSQLPlusExperiment: " + experimentName + " time: (.+)");
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            String g1 = matcher.group(1);
            String g2 = matcher.group(2);
            SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
            try {
                long logTimestamp = format.parse(g1).getTime();
                if (validateExperimentResult(logTimestamp, submitTimestamp)) {
                    return Optional.of(Double.valueOf(g2));
                } else {
                    return Optional.empty();
                }
            } catch (ParseException e) {
                LOGGER.error("can not extract result from line: " + line);
                return Optional.empty();
            }
        } else {
            LOGGER.error("can not extract result from line: " + line);
            return Optional.empty();
        }
    }

    public abstract boolean validateExperimentResult(long logTimestamp, long submitTimestamp);
}
