package sqlplus.springboot.dto;

public class Candidate {
    private Tree tree;
    private HyperGraph hyperGraph;

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
}
