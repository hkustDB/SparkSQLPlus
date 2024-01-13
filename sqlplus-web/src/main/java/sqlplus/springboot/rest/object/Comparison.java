package sqlplus.springboot.rest.object;

import java.util.List;

public class Comparison {
    String op;
    List<JoinTreeEdge> path;
    String left;
    String right;

    public Comparison() {
    }

    public Comparison(String op, List<JoinTreeEdge> path, String left, String right) {
        this.op = op;
        this.path = path;
        this.left = left;
        this.right = right;
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
}
