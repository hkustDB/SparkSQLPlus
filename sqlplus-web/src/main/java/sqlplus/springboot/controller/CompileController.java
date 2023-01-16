package sqlplus.springboot.controller;

import sqlplus.catalog.CatalogManager;
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
import sqlplus.plan.SqlPlusPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;
import sqlplus.springboot.dto.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CompileController {
    Logger logger = LoggerFactory.getLogger(CompileController.class);

    private scala.collection.immutable.List<Variable> outputVariables = null;

    private List<Tuple2<JoinTree, ComparisonHyperGraph>> candidates = null;

    private List<Relation> relations = null;

    private CatalogManager catalogManager = null;

    private VariableManager variableManager = null;

    @PostMapping("/compile/submit")
    public Result submit(@RequestBody CompileSubmitRequest request) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            catalogManager = new CatalogManager();
            catalogManager.register(nodeList);
            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());

            SqlPlusPlanner crownPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = crownPlanner.toLogicalPlan(sqlNode);

            variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager);
            Tuple2<scala.collection.immutable.List<Tuple2<JoinTree, ComparisonHyperGraph>>, scala.collection.immutable.List<Variable>> runGyoResult = converter.runGyo(logicalPlan);
            outputVariables = runGyoResult._2;

            candidates = scala.collection.JavaConverters.seqAsJavaList(
                    converter.candidatesWithLimit(runGyoResult._1, 4));
            return mkSubmitResult(candidates);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
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
        String code = sqlPlusCompiler.compile(catalogManager, convertResult, "sqlplus.example", "SparkSQLPlusExample");
        CompileCandidateResponse response = new CompileCandidateResponse();
        response.setCode(code);
        result.setData(response);
        return result;
    }
}
