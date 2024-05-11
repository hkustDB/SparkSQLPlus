package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.AuxiliaryRelation;

import java.util.List;
import java.util.stream.Collectors;

public class AuxiliaryJoinTreeNode extends JoinTreeNode {
    int support;
    List<String> columns;

    public AuxiliaryJoinTreeNode(AuxiliaryRelation relation, List<String> reserve) {
        super(relation.getRelationId(), "AuxiliaryRelation", relation.getTableDisplayName(), reserve);
        this.support = relation.supportingRelation().getRelationId();
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
    }

    public int getSupport() {
        return support;
    }

    public void setSupport(int support) {
        this.support = support;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
