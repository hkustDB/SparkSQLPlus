package sqlplus.cli;

import scala.collection.mutable.StringBuilder;
import sqlplus.catalog.CatalogManager;
import sqlplus.codegen.CodeGenerator;
import sqlplus.codegen.SparkSQLPlusExampleCodeGenerator;
import sqlplus.compile.CompileResult;
import sqlplus.compile.SqlPlusCompiler;
import sqlplus.convert.ConvertResult;
import sqlplus.convert.LogicalPlanConverter;
import sqlplus.expression.VariableManager;
import sqlplus.parser.SqlPlusParser;
import sqlplus.plan.SqlPlusPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.io.FileUtils;

import java.io.File;


public class CliFrontend {
    public static void printHelpMessage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setLeftPadding(5);
        formatter.setWidth(80);

        System.out.println("sparksql-plus compiles the input SQL file into SparkSQL+ code.");
        System.out.println("\nsyntax: sparksql-plus [OPTIONS] <query>");
        formatter.setSyntaxPrefix("  options:");
        formatter.printHelp("    ", CliFrontendOptions.getOptions());

        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        DefaultParser cliParser = new DefaultParser();
        CommandLine commandLine = cliParser.parse(CliFrontendOptions.getOptions(), args, true);

        if (commandLine.hasOption(CliFrontendOptions.HELP_OPTION.getOpt())) {
            printHelpMessage();
        } else {
            if (!commandLine.hasOption(CliFrontendOptions.DDL_OPTION.getOpt())) {
                System.out.println("Missing required argument: -d,--ddl <path>");
                printHelpMessage();
                return;
            }
            String ddlPath = commandLine.getOptionValue(CliFrontendOptions.DDL_OPTION.getOpt());

            if (!commandLine.hasOption(CliFrontendOptions.OUTPUT_OPTION.getOpt())) {
                System.out.println("Missing required argument: -o,--output <path>");
                printHelpMessage();
                return;
            }
            String outputPath = commandLine.getOptionValue(CliFrontendOptions.OUTPUT_OPTION.getOpt());

            String packageName = commandLine.hasOption(CliFrontendOptions.PACKAGE_NAME_OPTION.getOpt()) ?
                    commandLine.getOptionValue(CliFrontendOptions.PACKAGE_NAME_OPTION.getOpt()) :
                    "sqlplus.example";
            String objectName = commandLine.hasOption(CliFrontendOptions.OBJECT_NAME_OPTION.getOpt()) ?
                    commandLine.getOptionValue(CliFrontendOptions.OBJECT_NAME_OPTION.getOpt()) :
                    "SparkSQLPlusExample";
            String[] remainArgs = commandLine.getArgs();
            assert remainArgs.length == 1;
            String dmlPath = remainArgs[0];

            String ddlContent = FileUtils.readFileToString(new File(ddlPath));
            String dmlContent = FileUtils.readFileToString(new File(dmlPath));

            SqlNodeList nodeList = SqlPlusParser.parseDdl(ddlContent);
            CatalogManager catalogManager = new CatalogManager();
            catalogManager.register(nodeList);

            SqlNode sqlNode = SqlPlusParser.parseDml(dmlContent);

            SqlPlusPlanner sqlPlusPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode);

            VariableManager variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager);
            ConvertResult convertResult = converter.convert(logicalPlan);

            SqlPlusCompiler sqlPlusCompiler = new SqlPlusCompiler(variableManager);
            CompileResult compileResult = sqlPlusCompiler.compile(catalogManager, convertResult, true);
            CodeGenerator codeGenerator = new SparkSQLPlusExampleCodeGenerator(compileResult, packageName, objectName);
            StringBuilder builder = new StringBuilder();
            codeGenerator.generate(builder);

            FileUtils.writeStringToFile(new File(outputPath), builder.toString());
        }
    }
}
