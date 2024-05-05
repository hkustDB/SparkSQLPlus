package sqlplus.plan.hint;

import java.util.List;

public class HintNode {
    String relation;
    List<HintNode> children;

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public List<HintNode> getChildren() {
        return children;
    }

    public void setChildren(List<HintNode> children) {
        this.children = children;
    }
}
