package sqlplus.springboot.rest.object;

public class TopK {
    String orderByVariable;
    boolean isDesc;
    int limit;

    public String getOrderByVariable() {
        return orderByVariable;
    }

    public void setOrderByVariable(String orderByVariable) {
        this.orderByVariable = orderByVariable;
    }

    public boolean isDesc() {
        return isDesc;
    }

    public void setDesc(boolean desc) {
        isDesc = desc;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
