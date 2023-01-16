package sqlplus.springboot.dto;

public class CompileSubmitRequest {
    private String ddl;
    private String query;

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
}
