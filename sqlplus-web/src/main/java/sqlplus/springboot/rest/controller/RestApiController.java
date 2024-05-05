package sqlplus.springboot.rest.controller;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.springframework.web.bind.annotation.*;
import scala.collection.JavaConverters;
import sqlplus.catalog.CatalogManager;
import sqlplus.convert.ExtraCondition;
import sqlplus.convert.LogicalPlanConverter;
import sqlplus.convert.RunResult;
import sqlplus.expression.Expression;
import sqlplus.expression.Variable;
import sqlplus.expression.VariableManager;
import sqlplus.graph.*;
import sqlplus.parser.SqlPlusParser;
import sqlplus.plan.SqlPlusPlanner;
import sqlplus.plan.table.SqlPlusTable;
import sqlplus.springboot.dto.Result;
import sqlplus.springboot.rest.object.Comparison;
import sqlplus.springboot.rest.object.JoinTree;
import sqlplus.springboot.rest.object.JoinTreeEdge;
import sqlplus.springboot.rest.object.*;
import sqlplus.springboot.rest.request.ParseQueryRequest;
import sqlplus.springboot.rest.response.ParseQueryResponse;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1")
public class RestApiController {
    @PostMapping("/parse")
    public Result parseQuery(@RequestBody ParseQueryRequest request,
                             @RequestParam Optional<String> orderBy, @RequestParam Optional<Boolean> desc, @RequestParam Optional<Integer> limit,
                             @RequestParam Optional<Boolean> fixRootEnable, @RequestParam Optional<Boolean> pruneEnable) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            CatalogManager catalogManager = new CatalogManager();
            List<SqlPlusTable> tables = catalogManager.register(nodeList);

            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());

            SqlPlusPlanner sqlPlusPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode);

            VariableManager variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager, catalogManager);

            RunResult runResult;
            if (request.getPlan() == null) {
                runResult = converter.runAndSelect(logicalPlan, orderBy.orElse(""), desc.orElse(false), limit.orElse(Integer.MAX_VALUE), fixRootEnable.orElse(false), pruneEnable.orElse(false));
            } else {
                runResult = converter.runWithHint(logicalPlan, request.getPlan());
            }

            ParseQueryResponse response = new ParseQueryResponse();
            response.setTables(tables.stream()
                    .map(t -> new Table(t.getTableName(), Arrays.stream(t.getTableColumnNames()).collect(Collectors.toList())))
                    .collect(Collectors.toList()));

            List<JoinTree> joinTrees = JavaConverters.seqAsJavaList(runResult.candidates()).stream()
                    .map(t -> buildJoinTree(t._1(), t._2(), t._3()))
                    .collect(Collectors.toList());
            response.setJoinTrees(joinTrees);

            List<Computation> computations = JavaConverters.seqAsJavaList(runResult.computations()).stream()
                    .map(c -> new Computation(c._1.name(), c._2.format()))
                    .collect(Collectors.toList());
            response.setComputations(computations);

            response.setOutputVariables(JavaConverters.seqAsJavaList(runResult.outputVariables()).stream().map(Variable::name).collect(Collectors.toList()));
            response.setGroupByVariables(JavaConverters.seqAsJavaList(runResult.groupByVariables()).stream().map(Variable::name).collect(Collectors.toList()));
            List<Aggregation> aggregations = JavaConverters.seqAsJavaList(runResult.aggregations()).stream()
                    .map(t -> new Aggregation(t._1().name(), t._2(), JavaConverters.seqAsJavaList(t._3()).stream().map(Expression::format).collect(Collectors.toList())))
                    .collect(Collectors.toList());
            response.setAggregations(aggregations);

            if (runResult.optTopK().nonEmpty()) {
                TopK topK = new TopK();
                topK.setOrderByVariable(runResult.optTopK().get().sortBy().name());
                topK.setDesc(runResult.optTopK().get().isDesc());
                topK.setLimit(runResult.optTopK().get().limit());
                response.setTopK(topK);
            }

            response.setFull(runResult.isFull());
            response.setFreeConnex(runResult.isFreeConnex());

            Result result = new Result();
            result.setCode(200);
            result.setData(response);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JoinTree buildJoinTree(sqlplus.graph.JoinTree joinTree, ComparisonHyperGraph comparisonHyperGraph, scala.collection.immutable.List<ExtraCondition> extra) {
        JoinTree result = new JoinTree();
        Set<sqlplus.graph.JoinTreeEdge> joinTreeEdges = JavaConverters.setAsJavaSet(joinTree.edges());
        Set<Relation> relations = new HashSet<>();
        relations.add(joinTree.getRoot());
        joinTreeEdges.forEach(e -> {
            relations.add(e.getSrc());
            relations.add(e.getDst());
        });

        List<JoinTreeNode> nodes = relations.stream().map(r -> {
            if (r instanceof TableScanRelation) {
                return new TableScanJoinTreeNode((TableScanRelation) r);
            } else if (r instanceof AuxiliaryRelation) {
                return new AuxiliaryJoinTreeNode((AuxiliaryRelation) r);
            } else if (r instanceof AggregatedRelation) {
                return new AggregatedJoinTreeNode((AggregatedRelation) r);
            } else {
                return new BagJoinTreeNode((BagRelation) r);
            }
        }).collect(Collectors.toList());
        result.setNodes(nodes);

        List<JoinTreeEdge> edges = joinTreeEdges.stream()
                .map(e -> new JoinTreeEdge(e.getSrc().getRelationId(), e.getDst().getRelationId(), e.keyType().toString()))
                .collect(Collectors.toList());
        result.setEdges(edges);

        result.setRoot(joinTree.getRoot().getRelationId());

        List<Integer> subset = JavaConverters.setAsJavaSet(joinTree.getSubset()).stream().map(Relation::getRelationId).collect(Collectors.toList());
        result.setSubset(subset);

        result.setMaxFanout(joinTree.getMaxFanout());

        List<Comparison> comparisons = JavaConverters.setAsJavaSet(comparisonHyperGraph.getEdges()).stream().map(c -> {
            String op = c.op().getFuncName();
            List<JoinTreeEdge> path = JavaConverters.setAsJavaSet(c.getNodes()).stream()
                    .map(e -> new JoinTreeEdge(e.getSrc().getRelationId(), e.getDst().getRelationId(), e.keyType().toString()))
                    .collect(Collectors.toList());
            return new Comparison(op, path, c.left().format(), c.right().format(),
                    c.op().format(JavaConverters.asScalaBuffer(Arrays.asList(c.left(), c.right())).toList()));
        }).collect(Collectors.toList());
        result.setComparisons(comparisons);

        List<String> extraConditions = JavaConverters.seqAsJavaList(extra).stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        result.setExtraConditions(extraConditions);

        result.setFixRoot(joinTree.isFixRoot());

        return result;
    }
}
