package sqlplus.springboot.controller;

import scala.collection.JavaConverters.*;
import org.apache.commons.io.FileUtils;
import scala.Tuple3;
import scala.collection.mutable.StringBuilder;
import sqlplus.catalog.CatalogManager;
import sqlplus.codegen.CodeGenerator;
import sqlplus.codegen.SparkSQLExperimentCodeGenerator;
import sqlplus.codegen.SparkSQLPlusExampleCodeGenerator;
import sqlplus.codegen.SparkSQLPlusExperimentCodeGenerator;
import sqlplus.compile.CompileResult;
import sqlplus.compile.SqlPlusCompiler;
import sqlplus.convert.ConvertResult;
import sqlplus.convert.LogicalPlanConverter;
import sqlplus.expression.Variable;
import sqlplus.expression.VariableManager;
import sqlplus.graph.ComparisonHyperGraph;
import sqlplus.graph.JoinTree;
import sqlplus.graph.JoinTreeEdge;
import sqlplus.graph.Relation;
import sqlplus.parser.SqlPlusParser;
import sqlplus.parser.ddl.SqlCreateTable;
import sqlplus.plan.SqlPlusPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;
import sqlplus.plan.table.SqlPlusTable;
import sqlplus.springboot.dto.*;
import sqlplus.springboot.util.CustomQueryManager;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CompileController {
    private scala.collection.immutable.List<Variable> outputVariables = null;

    private List<Tuple2<JoinTree, ComparisonHyperGraph>> candidates = null;

    private List<Relation> relations = null;

    private CatalogManager catalogManager = null;

    private VariableManager variableManager = null;

    private scala.collection.immutable.List<SqlPlusTable> tables = null;

    private String sql = null;

    private CompileResult compileResult = null;

    private boolean isFullQuery = true;

    @PostMapping("/compile/submit")
    public Result submit(@RequestBody CompileSubmitRequest request) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            catalogManager = new CatalogManager();
            catalogManager.register(nodeList);
            storeSourceTables(nodeList);
            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());
            sql = request.getQuery();

            SqlPlusPlanner crownPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = crownPlanner.toLogicalPlan(sqlNode);

            variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager);
            Tuple3<scala.collection.immutable.List<Tuple2<JoinTree, ComparisonHyperGraph>>,
                                scala.collection.immutable.List<Variable>, String> runGyoResult = converter.runGyo(logicalPlan);
            outputVariables = runGyoResult._2();
            isFullQuery = runGyoResult._3().equalsIgnoreCase("full");
            if (!isFullQuery) {
                // is the query is non-full, we add DISTINCT keyword to SparkSQL explicitly
                sql = sql.replaceFirst("[s|S][e|E][l|L][e|E][c|C][t|T]", "SELECT DISTINCT");
            }

            candidates = scala.collection.JavaConverters.seqAsJavaList(
                    converter.candidatesWithLimit(runGyoResult._1(), 4));
            return mkSubmitResult(candidates);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeSourceTables(SqlNodeList nodeList) {
        ArrayList<SqlPlusTable> sourceTables = new ArrayList<>();
        //TODO: redundant code from catalogManager
        for (SqlNode node : nodeList) {
            SqlCreateTable createTable = (SqlCreateTable) node;
            SqlPlusTable table = new SqlPlusTable(createTable);
            sourceTables.add(table);
        }
        tables = scala.collection.JavaConverters.asScalaBuffer(sourceTables).toList();
    }

    private Result mkSubmitResult(List<Tuple2<JoinTree, ComparisonHyperGraph>> candidates) {
        Result result = new Result();
        result.setCode(200);
        CompileSubmitResponse response = new CompileSubmitResponse();
        response.setCandidates(candidates.stream().map(tuple -> {
            Candidate candidate = new Candidate();
            Tree tree = Tree.fromJoinTree(tuple._1);
            relations = extractRelations(tuple._1);
            Set<JoinTreeEdge> edges = scala.collection.JavaConverters.setAsJavaSet(tuple._1.getEdges());
            List<String> edgeStrings = edges.stream().map(JoinTreeEdge::mkUniformString).collect(Collectors.toList());
            List<String> sortedEdgeStrings = edgeStrings.stream().sorted().collect(Collectors.toList());
            HyperGraph hyperGraph = HyperGraph.fromComparisonHyperGraphAndRelations(tuple._2, sortedEdgeStrings);
            candidate.setTree(tree);
            candidate.setHyperGraph(hyperGraph);
            return candidate;
        }).collect(Collectors.toList()));
        result.setData(response);
        return result;
    }

    private List<Relation> extractRelations(JoinTree joinTree) {
        Set<Relation> set = new HashSet<>();
        scala.collection.JavaConverters.setAsJavaSet(joinTree.getEdges()).forEach(e -> {
            set.add(e.getSrc());
            set.add(e.getDst());
        });
        List<Relation> list = new ArrayList<>(set);
        list.sort(Comparator.comparingInt(Relation::getRelationId));
        return list;
    }

    @PostMapping("/compile/candidate")
    public Result candidate(@RequestBody CompileCandidateRequest request) {
        Result result = new Result();
        result.setCode(200);

        ConvertResult convertResult = new ConvertResult(
                scala.collection.JavaConverters.asScalaBuffer(relations).toList(),
                outputVariables,
                candidates.get(request.getIndex())._1,
                candidates.get(request.getIndex())._2
        );

        SqlPlusCompiler sqlPlusCompiler = new SqlPlusCompiler(variableManager);
        compileResult = sqlPlusCompiler.compile(catalogManager, convertResult);
        CodeGenerator codeGenerator = new SparkSQLPlusExampleCodeGenerator(compileResult);
        StringBuilder builder = new StringBuilder();
        codeGenerator.generate(builder);

        CompileCandidateResponse response = new CompileCandidateResponse();
        response.setCode(builder.toString());
        result.setData(response);
        return result;
    }

    @PostMapping("/compile/persist")
    public Result persist(@RequestBody CompilePersistRequest request) {
        Result result = new Result();
        result.setCode(200);

        String shortQueryName = CustomQueryManager.assign("examples/query/custom/");
        String queryName = shortQueryName.replace("q", "CustomQuery");
        String sqlplusClassname = queryName + "SparkSQLPlus";
        String sparkSqlClassname = queryName + "SparkSQL";

        String queryPath = "examples/query/custom/" + shortQueryName;
        File queryDirectory = new File(queryPath);
        queryDirectory.mkdirs();

        // generate sqlplus code
        CodeGenerator sqlplusCodeGen = new SparkSQLPlusExperimentCodeGenerator(compileResult, sqlplusClassname, queryName, shortQueryName);
        StringBuilder builder = new StringBuilder();
        sqlplusCodeGen.generate(builder);
        String sqlplusCodePath = queryPath + File.separator + sqlplusClassname + ".scala";
        try {
            FileUtils.writeStringToFile(new File(sqlplusCodePath), builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // generate SparkSQL code
        CodeGenerator sparkSqlCodeGen = new SparkSQLExperimentCodeGenerator(tables, sql, sparkSqlClassname, queryName, shortQueryName);
        builder.clear();
        sparkSqlCodeGen.generate(builder);
        String sparkSqlCodePath = queryPath + File.separator + sparkSqlClassname + ".scala";
        try {
            FileUtils.writeStringToFile(new File(sparkSqlCodePath), builder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        CompilePersistResponse response = new CompilePersistResponse();
        response.setName(queryName);
        response.setPath(queryPath);
        result.setData(response);
        return result;
    }
}
