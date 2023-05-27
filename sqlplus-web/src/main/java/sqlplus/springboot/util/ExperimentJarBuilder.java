package sqlplus.springboot.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ExperimentJarBuilder {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentJarBuilder.class);

    public static boolean build() {
        String command = "mvn clean package -pl sqlplus-example -am";
        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.redirectErrorStream(true);
            builder.command("sh", "-c", command);
            Process process = builder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                LOGGER.info(line);
            }
            int code = process.waitFor();
            return code == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
