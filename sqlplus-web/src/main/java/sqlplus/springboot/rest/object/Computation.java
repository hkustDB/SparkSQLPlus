package sqlplus.springboot.rest.object;

public class Computation {
    String result;
    String expr;

    public Computation() {
    }

    public Computation(String result, String expr) {
        this.result = result;
        this.expr = expr;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
