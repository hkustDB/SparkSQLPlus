package sqlplus.springboot.rest.object;

public class JoinTreeNode {
    int id;
    String type;

    public JoinTreeNode() {
    }

    public JoinTreeNode(int id, String type) {
        this.id = id;
        this.type = type;
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
}
