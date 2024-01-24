package sqlplus.springboot.rest.object;

import java.util.ArrayList;
import java.util.List;

public class JoinTree {
    List<JoinTreeNode> nodes;
    List<JoinTreeEdge> edges;
    int root;
    List<Integer> subset;
    int maxFanout;
    List<Comparison> comparisons = new ArrayList<>();
    List<List<String>> extraEqualConditions = new ArrayList<>();

    public List<JoinTreeNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<JoinTreeNode> nodes) {
        this.nodes = nodes;
    }

    public List<JoinTreeEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<JoinTreeEdge> edges) {
        this.edges = edges;
    }

    public int getRoot() {
        return root;
    }

    public void setRoot(int root) {
        this.root = root;
    }

    public List<Integer> getSubset() {
        return subset;
    }

    public void setSubset(List<Integer> subset) {
        this.subset = subset;
    }

    public int getMaxFanout() {
        return maxFanout;
    }

    public void setMaxFanout(int maxFanout) {
        this.maxFanout = maxFanout;
    }

    public List<Comparison> getComparisons() {
        return comparisons;
    }

    public void setComparisons(List<Comparison> comparisons) {
        this.comparisons = comparisons;
    }

    public List<List<String>> getExtraEqualConditions() {
        return extraEqualConditions;
    }

    public void setExtraEqualConditions(List<List<String>> extraEqualConditions) {
        this.extraEqualConditions = extraEqualConditions;
    }
}
