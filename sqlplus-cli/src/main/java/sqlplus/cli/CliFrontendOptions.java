package sqlplus.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CliFrontendOptions {
    public final static Option HELP_OPTION =
            new Option(
                    "h",
                    "help",
                    false,
                    "Show the help message.");

    public final static Option DDL_OPTION =
            new Option(
                    "d",
                    "ddl",
                    true,
                    "Set the path to the ddl file.");

    public final static Option OUTPUT_OPTION =
            new Option(
                    "o",
                    "output",
                    true,
                    "Set the path to the output file.");

    public final static Option PACKAGE_NAME_OPTION =
            new Option(
                    "p",
                    "pkg",
                    true,
                    "Set the package name for the output object.");

    public final static Option OBJECT_NAME_OPTION =
            new Option(
                    "n",
                    "name",
                    true,
                    "Set the object name for the output object.");

    static {
        HELP_OPTION.setRequired(false);
        DDL_OPTION.setRequired(false);
        DDL_OPTION.setArgName("path");
        OUTPUT_OPTION.setRequired(false);
        OUTPUT_OPTION.setArgName("path");
        PACKAGE_NAME_OPTION.setRequired(false);
        PACKAGE_NAME_OPTION.setArgName("package name");
        OBJECT_NAME_OPTION.setRequired(false);
        OBJECT_NAME_OPTION.setArgName("object name");
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption(HELP_OPTION);
        options.addOption(DDL_OPTION);
        options.addOption(OUTPUT_OPTION);
        options.addOption(PACKAGE_NAME_OPTION);
        options.addOption(OBJECT_NAME_OPTION);
        return options;
    }
}
