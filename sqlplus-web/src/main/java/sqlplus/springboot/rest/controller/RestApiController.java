package sqlplus.springboot.rest.controller;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import sqlplus.catalog.CatalogManager;
import sqlplus.convert.*;
import sqlplus.expression.Expression;
import sqlplus.expression.Variable;
import sqlplus.expression.VariableManager;
import sqlplus.graph.*;
import sqlplus.parser.SqlPlusParser;
import sqlplus.plan.SqlPlusPlanner;
import sqlplus.plan.hint.HintNode;
import sqlplus.plan.table.SqlPlusTable;
import sqlplus.springboot.dto.Result;
import sqlplus.springboot.rest.object.Comparison;
import sqlplus.springboot.rest.object.JoinTree;
import sqlplus.springboot.rest.object.JoinTreeEdge;
import sqlplus.springboot.rest.object.*;
import sqlplus.springboot.rest.object.TopK;
import sqlplus.springboot.rest.request.ParseQueryRequest;
import sqlplus.springboot.rest.response.ParseQueryResponse;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1")
public class RestApiController {
    @Resource(name = "threadPoolTaskExecutor")
    ThreadPoolTaskExecutor executor;

    @PostMapping("/parse")
    public Result parseQuery(@RequestBody ParseQueryRequest request, @RequestParam Optional<Integer> timeout) {
        try {
            SqlNodeList nodeList = SqlPlusParser.parseDdl(request.getDdl());
            CatalogManager catalogManager = new CatalogManager();
            List<SqlPlusTable> tables = catalogManager.register(nodeList);

            SqlNode sqlNode = SqlPlusParser.parseDml(request.getQuery());

            SqlPlusPlanner sqlPlusPlanner = new SqlPlusPlanner(catalogManager);
            RelNode logicalPlan = sqlPlusPlanner.toLogicalPlan(sqlNode);

            VariableManager variableManager = new VariableManager();
            LogicalPlanConverter converter = new LogicalPlanConverter(variableManager, catalogManager);
            Context context = converter.traverseLogicalPlan(logicalPlan);

            Option<ConvertResult> optConvertResult;
            Option<HandleResult> optDryRunResult = converter.dryRun(context);
            boolean isAcyclic = optDryRunResult.nonEmpty();
            Future<ConvertResult> convertResultFuture = executor.submit(() -> {
                if (isAcyclic) {
                    return converter.convertAcyclic(context);
                } else {
                    return converter.convertCyclic(context);
                }
            });

            int maxTimeout = timeout.orElse(Integer.MAX_VALUE);
            try {
                optConvertResult = Some.apply(convertResultFuture.get(maxTimeout, TimeUnit.MILLISECONDS));
            } catch (InterruptedException | ExecutionException | TimeoutException e1) {
                optConvertResult = convertResultOnTimeout(converter, context, request.getPlan(), optDryRunResult);
            }

            Result result = new Result();
            if (optConvertResult.nonEmpty()) {
                ConvertResult convertResult = optConvertResult.get();
                ParseQueryResponse response = new ParseQueryResponse();
                response.setTables(tables.stream()
                        .map(t -> new Table(t.getTableName(), Arrays.stream(t.getTableColumnNames()).collect(Collectors.toList())))
                        .collect(Collectors.toList()));

                List<JoinTree> joinTrees = JavaConverters.seqAsJavaList(convertResult.candidates()).stream()
                        .map(t -> buildJoinTree(t._1(), t._2(), t._3(), request.getPlan()))
                        .collect(Collectors.toList());
                response.setJoinTrees(joinTrees);

                List<Computation> computations = JavaConverters.seqAsJavaList(convertResult.computations()).stream()
                        .map(c -> new Computation(c._1.name(), c._2.format()))
                        .collect(Collectors.toList());
                response.setComputations(computations);

                response.setOutputVariables(JavaConverters.seqAsJavaList(convertResult.outputVariables()).stream().map(Variable::name).collect(Collectors.toList()));
                response.setGroupByVariables(JavaConverters.seqAsJavaList(convertResult.groupByVariables()).stream().map(Variable::name).collect(Collectors.toList()));
                List<Aggregation> aggregations = JavaConverters.seqAsJavaList(convertResult.aggregations()).stream()
                        .map(t -> new Aggregation(t._1().name(), t._2(), JavaConverters.seqAsJavaList(t._3()).stream().map(Expression::format).collect(Collectors.toList())))
                        .collect(Collectors.toList());
                response.setAggregations(aggregations);

                if (convertResult.optTopK().nonEmpty()) {
                    TopK topK = new TopK();
                    topK.setOrderByVariable(convertResult.optTopK().get().sortBy().name());
                    topK.setDesc(convertResult.optTopK().get().isDesc());
                    topK.setLimit(convertResult.optTopK().get().limit());
                    response.setTopK(topK);
                }

                response.setFull(convertResult.isFull());

                result.setCode(200);
                result.setData(response);
                return result;
            } else {
                result.setCode(200);
                result.setData(null);
                result.setMessage(Result.FALLBACK);
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();

            Result result = new Result();
            result.setCode(200);
            result.setData(null);
            result.setMessage(Result.FALLBACK);
            return result;
        }
    }

    private void visitHintNode(HintNode node, Map<String, Integer> relationAliasToId, Map<Integer, List<Integer>> hintJoinOrders) {
        if (!node.getChildren().isEmpty()) {
            List<Integer> order = node.getChildren().stream().map(c -> relationAliasToId.get(c.getRelation())).collect(Collectors.toList());
            hintJoinOrders.put(relationAliasToId.get(node.getRelation()), order);

            node.getChildren().forEach(c -> visitHintNode(c, relationAliasToId, hintJoinOrders));
        } else {
            hintJoinOrders.put(relationAliasToId.get(node.getRelation()), new ArrayList<>());
        }
    }

    private Map<Integer, List<Integer>> extractHintJoinOrders(HintNode root, sqlplus.graph.JoinTree joinTree) {
        Set<sqlplus.graph.JoinTreeEdge> joinTreeEdges = JavaConverters.setAsJavaSet(joinTree.edges());
        Map<String, Integer> relationAliasToId = new HashMap<>();
        relationAliasToId.put(joinTree.getRoot().getTableDisplayName(), joinTree.getRoot().getRelationId());
        if (!joinTree.getRoot().getTableDisplayName().equals(joinTree.getRoot().getTableName())) {
            relationAliasToId.put(joinTree.getRoot().getTableName(), joinTree.getRoot().getRelationId());
        }
        joinTreeEdges.forEach(e -> {
            relationAliasToId.put(e.getSrc().getTableDisplayName(), e.getSrc().getRelationId());
            if (!e.getSrc().getTableDisplayName().equals(e.getSrc().getTableName())) {
                relationAliasToId.put(e.getSrc().getTableName(), e.getSrc().getRelationId());
            }

            relationAliasToId.put(e.getDst().getTableDisplayName(), e.getDst().getRelationId());
            if (!e.getDst().getTableDisplayName().equals(e.getDst().getTableName())) {
                relationAliasToId.put(e.getDst().getTableName(), e.getDst().getRelationId());
            }
        });

        Map<Integer, List<Integer>> hintJoinOrders = new HashMap<>();
        visitHintNode(root, relationAliasToId, hintJoinOrders);
        return hintJoinOrders;
    }

    private JoinTree buildJoinTree(sqlplus.graph.JoinTree joinTree, ComparisonHyperGraph comparisonHyperGraph, scala.collection.immutable.List<ExtraCondition> extra, HintNode hintNode) {
        JoinTree result = new JoinTree();
        Set<sqlplus.graph.JoinTreeEdge> joinTreeEdges = JavaConverters.setAsJavaSet(joinTree.edges());
        Set<Relation> relations = new HashSet<>();
        relations.add(joinTree.getRoot());
        joinTreeEdges.forEach(e -> {
            relations.add(e.getSrc());
            relations.add(e.getDst());
        });

        Map<Relation, scala.collection.immutable.List<String>> reserves = JavaConverters.mapAsJavaMapConverter(sqlplus.graph.JoinTree.computeReserveVariables(joinTree)).asJava();

        Map<Integer, List<Integer>> hintJoinOrders = null;
        if (hintNode != null) {
            hintJoinOrders = extractHintJoinOrders(hintNode, joinTree);
        }

        List<JoinTreeNode> nodes = new ArrayList<>();
        Iterator<Relation> iter = relations.iterator();
        while (iter.hasNext()) {
            Relation r = iter.next();
            List<Integer> order = new ArrayList<>();
            if (hintJoinOrders != null) {
                order = hintJoinOrders.get(r.getRelationId());
            }

            if (r instanceof TableScanRelation) {
                nodes.add(new TableScanJoinTreeNode((TableScanRelation) r, JavaConverters.seqAsJavaList(reserves.get(r)), order));
            } else if (r instanceof AuxiliaryRelation) {
                nodes.add(new AuxiliaryJoinTreeNode((AuxiliaryRelation) r, JavaConverters.seqAsJavaList(reserves.get(r)), order));
            } else if (r instanceof AggregatedRelation) {
                nodes.add(new AggregatedJoinTreeNode((AggregatedRelation) r, JavaConverters.seqAsJavaList(reserves.get(r)), order));
            } else {
                nodes.add(new BagJoinTreeNode((BagRelation) r, JavaConverters.seqAsJavaList(reserves.get(r)), order));
            }
        }
        result.setNodes(nodes);

        List<JoinTreeEdge> edges = joinTreeEdges.stream()
                .map(e -> new JoinTreeEdge(e.getSrc().getRelationId(), e.getDst().getRelationId(), e.keyType().toString()))
                .collect(Collectors.toList());
        result.setEdges(edges);

        result.setRoot(joinTree.getRoot().getRelationId());

        List<Integer> subset = JavaConverters.setAsJavaSet(joinTree.getSubset()).stream().map(Relation::getRelationId).collect(Collectors.toList());
        result.setSubset(subset);

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

    private Option<ConvertResult>  convertResultOnTimeout(LogicalPlanConverter converter, Context context,
                                                            HintNode hint, Option<HandleResult> optDryRunResult) {
        if (hint != null) {
            try {
                return Some.apply(converter.convertHint(context, hint));
            } catch (Exception e) {
                return optDryRunResult.map(result -> converter.convertHandleResult(context, result));
            }
        } else {
            return optDryRunResult.map(result -> converter.convertHandleResult(context, result));
        }
    }
}
