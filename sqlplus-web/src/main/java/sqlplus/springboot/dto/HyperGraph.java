package sqlplus.springboot.dto;

import sqlplus.graph.Comparison;
import sqlplus.graph.ComparisonHyperGraph;
import sqlplus.graph.JoinTreeEdge;

import java.util.*;
import java.util.stream.Collectors;

public class HyperGraph {
    private List<String> nodes;
    private List<List<String>> edges;

    private int degree;

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    public List<List<String>> getEdges() {
        return edges;
    }

    public void setEdges(List<List<String>> edges) {
        this.edges = edges;
    }

    public int getDegree() {
        return degree;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public static HyperGraph fromComparisonHyperGraphAndRelations(ComparisonHyperGraph comparisonHyperGraph, List<String> sortedEdges, Map<JoinTreeEdge, String> joinTreeEdgeToStringMap) {
        HyperGraph hyperGraph = new HyperGraph();
        hyperGraph.setDegree(comparisonHyperGraph.getDegree());
        // a node in HyperGraph is an edge in join tree
        hyperGraph.setNodes(sortedEdges);
        Set<Comparison> comparisons = scala.collection.JavaConverters.setAsJavaSet(comparisonHyperGraph.getEdges());
        Set<Comparison> nonSelfComparisons = comparisons.stream().filter(c -> {
           if (c.getNodes().size() > 1) {
               return true;
           } else {
               // comparison has only one JoinTreeEdge, check src and dst
               JoinTreeEdge joinTreeEdge = c.getNodes().head();
               return joinTreeEdge.getSrc().getRelationId() != joinTreeEdge.getDst().getRelationId();
           }
        }).collect(Collectors.toSet());
        List<List<String>> edges = (new ArrayList<>(nonSelfComparisons)).stream().map(c -> convertComparisonToStringList(c, joinTreeEdgeToStringMap)).collect(Collectors.toList());
        hyperGraph.setEdges(edges);

        return hyperGraph;
    }

    private static List<String> convertComparisonToStringList(Comparison comparison, Map<JoinTreeEdge, String> joinTreeEdgeToStringMap) {
        Set<JoinTreeEdge> joinTreeEdgeSet = scala.collection.JavaConverters.setAsJavaSet(comparison.getNodes());
        return (new ArrayList<>(joinTreeEdgeSet)).stream().map(joinTreeEdgeToStringMap::get).collect(Collectors.toList());
    }
}
