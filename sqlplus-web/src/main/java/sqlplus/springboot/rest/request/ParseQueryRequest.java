package sqlplus.springboot.rest.request;

import sqlplus.plan.hint.HintNode;

public class ParseQueryRequest {
    String ddl;
    String query;
    HintNode plan;

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public HintNode getPlan() {
        return plan;
    }

    public void setPlan(HintNode plan) {
        this.plan = plan;
    }
}
