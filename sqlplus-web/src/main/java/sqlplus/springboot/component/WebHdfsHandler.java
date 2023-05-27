package sqlplus.springboot.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import sqlplus.springboot.SqlplusConfig;

import javax.annotation.PostConstruct;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class WebHdfsHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger(WebHdfsHandler.class);

    private final SqlplusConfig config;

    private WebClient client;

    public WebHdfsHandler(SqlplusConfig config) {
        this.config = config;
    }

    @PostConstruct
    public void init() {
        this.client = WebClient.builder().build();
    }

    public boolean uploadJars() {
        try {
            return uploadFile(config.getLocalLibJarPath(), config.getRemoteLibJarPath())
                    && uploadFile(config.getLocalExampleJarPath(), config.getRemoteExampleJarPath());
        } catch (Exception e) {
            LOGGER.error("failed to upload jars.", e);
            return false;
        }
    }

    private boolean uploadFile(String localFilePath, String remoteFilePath) {
        try {
            WebClient client = WebClient.builder().build();

            String host = config.isForwarding() ? "localhost" : config.getHdfsHost();
            String createUri = "http://" + host + ":" + config.getHdfsPort() + "/webhdfs/v1"
                    + remoteFilePath + "?op=CREATE&overwrite=true&user.name=" + config.getHdfsUser();
            ResponseEntity<Void> response = client.put().uri(createUri).retrieve().toBodilessEntity().block();
            String location = response.getHeaders().get("Location").get(0);

            if (config.isForwarding()) {
                // replace the host in Location with "localhost" when using forwarding
                Pattern p = Pattern.compile("(^http://)(.*)(:\\d+/)");
                Matcher m = p.matcher(location);
                if (m.find()) {
                    location = m.replaceFirst("$1localhost$3");
                    LOGGER.info("Using forwarding. Set location to " + location);
                }
            }

            client.put().uri(location)
                    .body(BodyInserters.fromResource(new FileSystemResource(localFilePath)))
                    .retrieve().toBodilessEntity().block();

            LOGGER.info("uploaded " + localFilePath + " to hdfs://" + remoteFilePath);
            return true;
        } catch (Exception e) {
            LOGGER.error("failed to upload " + localFilePath + " to hdfs://" + remoteFilePath, e);
            return false;
        }
    }
}
