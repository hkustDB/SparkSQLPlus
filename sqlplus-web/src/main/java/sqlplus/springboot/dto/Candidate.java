package sqlplus.springboot.dto;

import java.util.Map;

public class Candidate {
    private Tree tree;
    private HyperGraph hyperGraph;
    private Map<String, Object> bags;

    public Tree getTree() {
        return tree;
    }

    public void setTree(Tree tree) {
        this.tree = tree;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    public void setHyperGraph(HyperGraph hyperGraph) {
        this.hyperGraph = hyperGraph;
    }

    public Map<String, Object> getBags() {
        return bags;
    }

    public void setBags(Map<String, Object> bags) {
        this.bags = bags;
    }
}
