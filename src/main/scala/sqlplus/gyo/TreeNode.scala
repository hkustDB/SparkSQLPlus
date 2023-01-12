package sqlplus.gyo

import sqlplus.graph.Relation

sealed trait TreeNode {
    def getRelation(): Relation
    def getChildren(): Set[TreeNode]
    def attach(child: TreeNode): TreeNode

    def attachAll(children: Set[TreeNode]): TreeNode

    /**
     * get a signature string that uniquely represents the TreeNode(with its Relation).
     * It is a string with the following format: $relationName-$variables-[$childrenSignatures]
     * The $variables is a comma-separated string like "Var1,Var4,Var5", sorted by variable name
     * The $childrenSignatures is a comma-separated string like "sig1,sig2,sig3". The "sigx" means
     * the signature of the xth children sorted by its relation name.
     *
     * @return the signature
     */
    def getSignature(): String = {
        val relationName = getRelation().getTableName()
        val variables = getRelation().getNodes().map(v => v.name).toList.sorted.mkString(",")
        val childrenSignatures = getChildren().toList.map(tn => tn.getSignature()).sorted.mkString(",")
        s"$relationName-$variables-[$childrenSignatures]"
    }
}

case class LeafTreeNode(relation: Relation) extends TreeNode {
    override def getRelation(): Relation = relation

    override def getChildren(): Set[TreeNode] = Set.empty

    override def attach(child: TreeNode): TreeNode =
        InternalTreeNode(relation, Set(child))

    override def attachAll(children: Set[TreeNode]): TreeNode =
        InternalTreeNode(relation, children)
}

case class InternalTreeNode(relation: Relation, children: Set[TreeNode]) extends TreeNode {
    override def getRelation(): Relation = relation

    override def getChildren(): Set[TreeNode] = children

    override def attach(child: TreeNode): TreeNode =
        InternalTreeNode(relation, children + child)

    override def attachAll(c: Set[TreeNode]): TreeNode =
        InternalTreeNode(relation, children ++ c)
}
