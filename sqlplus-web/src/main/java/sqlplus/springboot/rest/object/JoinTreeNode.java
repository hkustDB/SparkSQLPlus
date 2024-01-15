package sqlplus.springboot.rest.object;

public class JoinTreeNode {
    int id;
    String type;
    String alias;

    public JoinTreeNode() {
    }

    public JoinTreeNode(int id, String type, String alias) {
        this.id = id;
        this.type = type;
        this.alias = alias;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
