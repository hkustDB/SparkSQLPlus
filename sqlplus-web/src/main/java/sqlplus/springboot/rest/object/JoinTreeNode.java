package sqlplus.springboot.rest.object;

import java.util.List;

public class JoinTreeNode {
    int id;
    String type;
    String alias;
    List<String> reserves;

    public JoinTreeNode() {
    }

    public JoinTreeNode(int id, String type, String alias, List<String> reserves) {
        this.id = id;
        this.type = type;
        this.alias = alias;
        this.reserves = reserves;
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

    public List<String> getReserves() {
        return reserves;
    }

    public void setReserves(List<String> reserves) {
        this.reserves = reserves;
    }
}
