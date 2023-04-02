package sqlplus.springboot.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.*;
import java.util.stream.Collectors;

public class CustomQueryManager {
    private final static Logger LOGGER = LoggerFactory.getLogger(CustomQueryManager.class);

    /**
     * list all the custom queries.
     * @param path the path to examples/query/custom
     * @return
     */
    public static List<String> list(String path) {
        File custom = new File(path);
        if (!custom.exists() || !custom.isDirectory()) {
            LOGGER.error(path + "is not a valid path to custom queries.");
            return new ArrayList<>();
        }

        File[] queries = custom.listFiles(children -> {
            if (children.isDirectory() && children.getName().matches("q\\d+")) {
                String name = children.getName();
                String queryName = name.replace("q", "CustomQuery");
                String sqlplusFileName = queryName + "SparkSQLPlus.scala";
                String sparkSqlFileName = queryName + "SparkSQL.scala";
                List<String> codes = Arrays.stream(Objects.requireNonNull(children.list())).collect(Collectors.toList());
                return codes.contains(sqlplusFileName) && codes.contains(sparkSqlFileName);
            } else {
                return false;
            }
        });

        assert queries != null;
        return Arrays.stream(queries).map(f -> f.getName().replace("q", "CustomQuery")).collect(Collectors.toList());
    }

    public static String assign(String path) {
        File custom = new File(path);
        if (!custom.exists()) {
            return "q1";
        }

        File[] exists = custom.listFiles(children -> children.getName().matches("q\\d+"));
        Set<String> names = Arrays.stream(Objects.requireNonNull(exists)).map(File::getName).collect(Collectors.toSet());

        int i = 1;
        while (names.contains("q" + i)) {
            i += 1;
        }

        return "q" + i;
    }
}
