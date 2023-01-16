package sqlplus.gyo

import sqlplus.graph.{Relation, RelationalHyperGraph}

/**
 * A forest is a set of trees(represented by their root).
 */
class Forest(private val roots: Set[TreeNode], private val dict: Map[Relation, TreeNode]) {
    def removeTree(tree: TreeNode): Forest = {
        new Forest(roots - tree, dict - tree.getRelation())
    }

    def addTree(tree: TreeNode): Forest = {
        new Forest(roots + tree, dict + (tree.getRelation() -> tree))
    }

    def getSignature(): String = {
        roots.toList.map(t => t.getSignature()).sorted.mkString("#")
    }

    def getTree(relation: Relation): TreeNode =
        dict(relation)
}

object Forest {
    def create(graph: RelationalHyperGraph): Forest = {
        graph.edges.foldLeft(new Forest(Set.empty, Map.empty))((f,r) => f.addTree(LeafTreeNode(r)))
    }
}
