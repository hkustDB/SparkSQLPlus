package sqlplus.springboot.component;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import sqlplus.springboot.SqlplusConfig;

import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@ConditionalOnProperty(value = "experiment.result.type", havingValue = "web-ui", matchIfMissing = true)
public class ExperimentResultWebRetriever extends AbstractExperimentResultRetriever {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentResultWebRetriever.class);

    private final static String DIV_DRIVERS_CLASS = "aggregated-completedDrivers collapsible-table";
    private final static String DIV_LOG_CLASS = "log-content";

    private final SqlplusConfig config;

    @Autowired
    public ExperimentResultWebRetriever(SqlplusConfig config) {
        this.config = config;
    }

    @Override
    public Optional<Double> retrieve(String submissionId, String experimentName, long submitTimestamp) {
        Optional<String> optWorkerUrl = retrieveWorkerUrl(submissionId);

        return optWorkerUrl.flatMap(workerUrl -> retrieveRunningTime(workerUrl, submissionId, experimentName));
    }

    @Override
    public boolean validateExperimentResult(long logTimestamp, long submitTimestamp) {
        // the result from web-ui is always valid
        return true;
    }

    private Optional<String> retrieveWorkerUrl(String submissionId) {
        try {
            String host = config.isForwarding() ? "localhost" : config.getSparkMasterHost();

            Document document = Jsoup.connect("http://" + host + ":" + config.getSparkMasterWebUIPort()).get();
            Elements divs = document.getElementsByClass(DIV_DRIVERS_CLASS);
            if (divs.size() != 1) {
                LOGGER.error("class " + DIV_DRIVERS_CLASS + " is not unique");
                return Optional.empty();
            }
            Element div = divs.get(0);

            Elements tbodys = div.getElementsByTag("tbody");
            if (tbodys.size() != 1) {
                LOGGER.error("tag tbody is not unique");
                return Optional.empty();
            }
            Element tbody = tbodys.get(0);

            Elements trs = tbody.getElementsByTag("tr");
            Optional<Element> row = trs.stream().filter(tr -> extractSubmissionId(tr).equalsIgnoreCase(submissionId)).findAny();


            return row.map(this::extractWorkerUrl);
        } catch (Exception e) {
            LOGGER.error("failed to retrieve worker url for " + submissionId, e);
            return Optional.empty();
        }
    }

    private Optional<Double> retrieveRunningTime(String workerUrl, String submissionId, String experimentName) {

        if (config.isForwarding()) {
            // replace the host in workerUrl with "localhost" when using forwarding
            Pattern p = Pattern.compile("(^http://)(.*)(:\\d+)");
            Matcher m = p.matcher(workerUrl);
            if (m.find()) {
                workerUrl = m.replaceFirst("$1localhost$3");
                LOGGER.info("Using forwarding. Set workerUrl to " + workerUrl);
            }
        }

        String url = workerUrl + "/logPage?driverId=" + submissionId + "&logType=stdout";
        try {
            Document document = Jsoup.connect(url).get();
            Elements divs = document.getElementsByClass(DIV_LOG_CLASS);
            if (divs.size() != 1) {
                LOGGER.error("class " + DIV_LOG_CLASS + " is not unique");
                return Optional.empty();
            }
            Element div = divs.get(0);

            Elements pres = div.getElementsByTag("pre");
            if (pres.size() != 1) {
                LOGGER.error("tag pre is not unique");
                return Optional.empty();
            }
            Element pre = pres.get(0);

            String content = pre.text();
            Optional<String> optLine = Arrays.stream(content.split("\n"))
                    .map(String::trim)
                    .filter(s -> s.contains(experimentName + " time: "))
                    .findAny();

            return optLine.flatMap(line -> extractExperimentResult(line, experimentName, /* ignored */ 0L));
        } catch (Exception e) {
            LOGGER.error("failed to retrieve running time for " + submissionId + " from " + url, e);
            return Optional.empty();
        }
    }

    private String extractSubmissionId(Element tr) {
        return tr.child(0).text();
    }

    private String extractWorkerUrl(Element tr) {
        return tr.child(2).child(0).attr("href");
    }
}
