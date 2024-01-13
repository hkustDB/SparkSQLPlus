package sqlplus.springboot.rest.object;

import java.util.List;

public class Aggregation {
    String result;
    String func;
    List<String> args;


    public Aggregation() {
    }

    public Aggregation(String result, String func, List<String> args) {

        this.result = result;
        this.func = func;
        this.args = args;
    }

    public String getFunc() {
        return func;
    }

    public void setFunc(String func) {
        this.func = func;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
