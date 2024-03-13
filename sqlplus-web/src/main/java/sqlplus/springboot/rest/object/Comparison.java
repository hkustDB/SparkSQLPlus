package sqlplus.springboot.rest.object;

import java.util.List;

public class Comparison {
    String op;
    List<JoinTreeEdge> path;
    String left;
    String right;
    String cond;

    public Comparison() {
    }

    public Comparison(String op, List<JoinTreeEdge> path, String left, String right, String cond) {
        this.op = op;
        this.path = path;
        this.left = left;
        this.right = right;
        this.cond = cond;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public List<JoinTreeEdge> getPath() {
        return path;
    }

    public void setPath(List<JoinTreeEdge> path) {
        this.path = path;
    }

    public String getLeft() {
        return left;
    }

    public void setLeft(String left) {
        this.left = left;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }

    public String getCond() {
        return cond;
    }

    public void setCond(String cond) {
        this.cond = cond;
    }
}
