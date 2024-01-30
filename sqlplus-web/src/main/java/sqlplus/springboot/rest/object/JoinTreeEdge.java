package sqlplus.springboot.rest.object;

public class JoinTreeEdge {
    int src;
    int dst;
    String key;

    public JoinTreeEdge() {
    }

    public JoinTreeEdge(int src, int dst, String key) {
        this.src = src;
        this.dst = dst;
        this.key = key;
    }

    public int getSrc() {
        return src;
    }

    public void setSrc(int src) {
        this.src = src;
    }

    public int getDst() {
        return dst;
    }

    public void setDst(int dst) {
        this.dst = dst;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
