package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.AggregatedRelation;

import java.util.List;
import java.util.stream.Collectors;

public class AggregatedJoinTreeNode extends JoinTreeNode {
    String source;
    List<String> columns;
    List<Integer> group;
    String func;

    public AggregatedJoinTreeNode(AggregatedRelation relation, List<String> reserve) {
        super(relation.getRelationId(), "AggregatedRelation", relation.getTableDisplayName(), reserve);
        this.source = relation.getTableName();
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
        this.group = JavaConverters.seqAsJavaList(relation.group()).stream().map(i -> (Integer)i).collect(Collectors.toList());
        this.func = relation.func();
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<Integer> getGroup() {
        return group;
    }

    public void setGroup(List<Integer> group) {
        this.group = group;
    }

    public String getFunc() {
        return func;
    }

    public void setFunc(String func) {
        this.func = func;
    }
}
