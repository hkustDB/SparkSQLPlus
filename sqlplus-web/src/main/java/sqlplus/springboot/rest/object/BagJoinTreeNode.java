package sqlplus.springboot.rest.object;

import scala.collection.JavaConverters;
import sqlplus.expression.Variable;
import sqlplus.graph.BagRelation;
import sqlplus.graph.Relation;

import java.util.List;
import java.util.stream.Collectors;

public class BagJoinTreeNode extends JoinTreeNode {
    List<String> internal;
    List<String> columns;

    public BagJoinTreeNode(BagRelation relation) {
        super(relation.getRelationId(), "BagRelation", relation.getTableDisplayName());
        this.internal = JavaConverters.seqAsJavaList(relation.getInternalRelations()).stream().map(Relation::getTableDisplayName).collect(Collectors.toList());
        this.columns = JavaConverters.seqAsJavaList(relation.getVariableList()).stream().map(Variable::name).collect(Collectors.toList());
    }

    public List<String> getInternal() {
        return internal;
    }

    public void setInternal(List<String> internal) {
        this.internal = internal;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}