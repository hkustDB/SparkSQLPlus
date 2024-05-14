package sqlplus.springboot.rest.object;

import java.util.List;

public class JoinTreeNode {
    int id;
    String type;
    String alias;
    List<String> reserves;
    List<Integer> hintJoinOrder;

    public JoinTreeNode() {
    }

    public JoinTreeNode(int id, String type, String alias, List<String> reserves, List<Integer> hintJoinOrder) {
        this.id = id;
        this.type = type;
        this.alias = alias;
        this.reserves = reserves;
        this.hintJoinOrder = hintJoinOrder;
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

    public List<Integer> getHintJoinOrder() {
        return hintJoinOrder;
    }

    public void setHintJoinOrder(List<Integer> hintJoinOrder) {
        this.hintJoinOrder = hintJoinOrder;
    }
}
