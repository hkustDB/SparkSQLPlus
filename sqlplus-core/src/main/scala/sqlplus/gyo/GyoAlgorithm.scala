package sqlplus.gyo

import sqlplus.expression.Variable
import sqlplus.ghd.GhdAlgorithm
import sqlplus.{graph, gyo}
import sqlplus.graph.{AuxiliaryRelation, JoinTree, JoinTreeEdge, Relation, RelationalHyperGraph}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GyoAlgorithm {
    /**
     *
     * @param hyperGraph
     * @param outputVariables
     * @return
     */
    def run(hyperGraph: RelationalHyperGraph, outputVariables: Set[Variable]): Option[GyoResult] = {
        val initState = new GyoState(hyperGraph, Forest.create(hyperGraph))

        def attach(parent: Relation, child: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = forest.getTree(parent)
            val yTreeNode = forest.getTree(child)

            if (child.isInstanceOf[AuxiliaryRelation] && child.getNodes().subsetOf(parent.getNodes())) {
                assert(yTreeNode.getChildren().nonEmpty)
                val newTreeNode = xTreeNode.attachAll(yTreeNode.getChildren())
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            } else {
                val newTreeNode = xTreeNode.attach(yTreeNode)
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            }
        }

        def replace(newRelation: Relation, originalRelation: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = gyo.LeafTreeNode(newRelation)
            val yTreeNode = forest.getTree(originalRelation)
            val newTreeNode = xTreeNode.attach(yTreeNode)
            val newForest = forest.removeTree(yTreeNode).addTree(newTreeNode)
            val newGraph = graph.removeHyperEdge(originalRelation).addHyperEdge(newRelation)
            new GyoState(newGraph, newForest)
        }

        def nextStates(gyoState: GyoState): Option[List[GyoState]] = {
            // reduce relations without output variables
            val remaining = gyoState.getRelationalHyperGraph.getEdges()
            val nonOutput = remaining.filter(r => r.getNodes().intersect(outputVariables).isEmpty)

            val list = ListBuffer.empty[(Relation, Relation)]
            val list2 = ListBuffer.empty[(Relation, Set[Variable])]
            for {
                x <- nonOutput
                y <- remaining
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes()))
                    list.append((x, y))
                else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remaining.forall(r => r == x || !r.getNodes().contains(v)))) {
                        // variables in diff belong to x only
                        list2.append((x, diff))
                    }
                }
            }

            val output = remaining.filter(r => r.getNodes().intersect(outputVariables).nonEmpty)
            val list3 = ListBuffer.empty[(Relation, Set[Variable])]
            output.foreach(r => {
                val variables = r.getNodes()
                // nonOutput variables that belongs to r only
                val removable = variables.filter(v => !outputVariables.contains(v) && remaining.forall(p => p == r || !p.getNodes().contains(v)))
                if (removable.nonEmpty) {
                    list3.append((r, removable))
                }
            })

            if (list.nonEmpty) {
                // some nonOutput relations are subset of another relation
                Some(list.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)))
            } else if (list2.nonEmpty) {
                Some(list2.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }))
            } else if (list3.nonEmpty) {
                Some(list3.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }))
            } else {
                None
            }
        }

        // get next state in step 2
        def nextStates2(stateAndSubset: (GyoState, Set[Relation])): Option[List[(GyoState, Set[Relation])]] = {
            val gyoState = stateAndSubset._1
            val subset = stateAndSubset._2
            val remaining = gyoState.getRelationalHyperGraph.getEdges()
            val list = ListBuffer.empty[(Relation, Relation)]
            val list2 = ListBuffer.empty[(Relation, Set[Variable])]

            for {
                x <- remaining
                y <- remaining
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes()))
                    list.append((x, y))
                else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remaining.forall(r => r == x || !r.getNodes().contains(v)))) {
                        // variables in diff belong to x only
                        list2.append((x, diff))
                    }
                }
            }

            if (list.nonEmpty) {
                Some(list.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)).map(s => (s, subset)))
            } else if (list2.nonEmpty) {
                Some(list2.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }).map(s => (s, subset)))
            } else {
                None
            }
        }

        // step 1
        val seenState = mutable.HashSet.empty[GyoState]
        val queue = mutable.Queue.empty[GyoState]
        val stableState = mutable.HashSet.empty[GyoState]
        queue.enqueue(initState)
        while (queue.nonEmpty) {
            val head = queue.dequeue()
            val next = nextStates(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenState.contains(p)) {
                        seenState.add(p)
                        queue.enqueue(p)
                    }
                })
            } else {
                stableState.add(head)
            }
        }

        if (stableState.isEmpty || stableState.forall(s => s.getRelationalHyperGraph.getEdges().exists(r => r.getNodes().exists(v => !outputVariables.contains(v))))) {
            return None
        }

        // step 2
        val finalStates = mutable.HashSet.empty[(GyoState, Set[Relation])]
        val seenState2 = mutable.HashSet.empty[GyoState]
        val queue2 = mutable.Queue.empty[(GyoState, Set[Relation])]
        stableState.foreach(ss => queue2.enqueue((ss, ss.getRelationalHyperGraph.getEdges())))
        while (queue2.nonEmpty) {
            val head = queue2.dequeue()
            val next = nextStates2(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenState2.contains(p._1)) {
                        seenState2.add(p._1)
                        queue2.enqueue(p)
                    }
                })
            } else {
                finalStates.add(head)
            }
        }

        if (finalStates.map(t => t._1).forall(fs => fs.getRelationalHyperGraph.getEdges().size == 1)) {
            val rootsAndSubset = finalStates.map(s => (s._1.getForest.getTree(s._1.getRelationalHyperGraph.getEdges().head), s._2)).toList
            Some(GyoResult(rootsAndSubset.map(ras => convertToJoinTreeWithHyperGraph(ras._1, ras._2))))
        } else {
            None
        }
    }

    private def convertToJoinTreeWithHyperGraph(root: TreeNode, subset: Set[Relation]): (JoinTree, RelationalHyperGraph) = {
        def fun(node: TreeNode, parent: Relation): (Set[JoinTreeEdge], Set[Relation]) = {
            node match {
                case InternalTreeNode(relation, children) =>
                    val childrenResult = children.map(n => fun(n, relation))
                        .foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, r) => (z._1 ++ r._1, z._2 ++ r._2))
                    (childrenResult._1 + new JoinTreeEdge(parent, relation), childrenResult._2 + relation)
                case LeafTreeNode(relation) =>
                    (Set(new JoinTreeEdge(parent, relation)), Set(relation))
            }
        }

        root match {
            case LeafTreeNode(relation) =>
                (JoinTree(relation, Set(), subset), RelationalHyperGraph.EMPTY.addHyperEdge(relation))
            case InternalTreeNode(relation, children) =>
                val childrenResult = children.map(n => fun(n, relation))
                    .foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, r) => (z._1 ++ r._1, z._2 ++ r._2))
                val graph = childrenResult._2.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r)).addHyperEdge(relation)
                (JoinTree(relation, childrenResult._1, subset), graph)
        }
    }
}
