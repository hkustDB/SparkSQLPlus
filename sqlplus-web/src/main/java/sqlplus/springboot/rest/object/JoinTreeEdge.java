package sqlplus.springboot.rest.object;

public class JoinTreeEdge {
    int src;
    int dst;

    public JoinTreeEdge() {
    }

    public JoinTreeEdge(int src, int dst) {
        this.src = src;
        this.dst = dst;
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
}
