package sqlplus.gyo

import sqlplus.expression.Variable
import sqlplus.graph._
import sqlplus.gyo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This algorithm is modeled as a BFS search of valid states. A state contains a forest and the remaining hyperGraph.
 */
class GyoAlgorithm {
    def run(hyperGraph: RelationalHyperGraph, outputVariables: Set[Variable], terminateIfNonFreeConnex: Boolean): Option[GyoResult] = {
        val initState = new GyoState(hyperGraph, Forest.create(hyperGraph))
        var isFreeConnex = true

        // attach the tree represented by $child to the tree represented by $parent
        // $child will be removed in hyperGraph
        def attach(parent: Relation, child: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = forest.getTree(parent)
            val yTreeNode = forest.getTree(child)

            // reduce the unnecessary AuxiliaryRelations here
            if (child.isInstanceOf[AuxiliaryRelation] && child.getNodes().subsetOf(parent.getNodes())) {
                // there is no need to attach an AuxiliaryRelation to a parent that contains all its variables
                // e.g., R(A,B,C) -> [B,C] -> S(B,C,D) = R(A,B,C) -> S(B,C,D)
                assert(yTreeNode.getChildren().nonEmpty)
                val newTreeNode = xTreeNode.attachAll(yTreeNode.getChildren())
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            } else {
                // general cases
                val newTreeNode = xTreeNode.attach(yTreeNode)
                val newForest = forest.removeTree(xTreeNode).removeTree(yTreeNode).addTree(newTreeNode)
                val newGraph = graph.removeHyperEdge(child)
                new GyoState(newGraph, newForest)
            }
        }

        // for hyperGraph: replace $originalRelation with $newRelation
        // for forest: create a new tree node for $newRelation and attach the original tree node to it
        def replace(newRelation: Relation, originalRelation: Relation, graph: RelationalHyperGraph, forest: Forest): GyoState = {
            val xTreeNode = gyo.LeafTreeNode(newRelation)
            val yTreeNode = forest.getTree(originalRelation)
            val newTreeNode = xTreeNode.attach(yTreeNode)
            val newForest = forest.removeTree(yTreeNode).addTree(newTreeNode)
            val newGraph = graph.removeHyperEdge(originalRelation).addHyperEdge(newRelation)
            new GyoState(newGraph, newForest)
        }

        // compute the reachable valid states in phase 1
        def phase1NextStates(gyoState: GyoState): Option[List[GyoState]] = {
            // reduce relations without output variables
            val remainingRelations = gyoState.getRelationalHyperGraph.getEdges()
            val nonOutputRelations = remainingRelations.filter(r => r.getNodes().intersect(outputVariables).isEmpty)

            val reductions = ListBuffer.empty[(Relation, Relation)]
            val removes = ListBuffer.empty[(Relation, Set[Variable])]
            for {
                x <- nonOutputRelations
                y <- remainingRelations
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes())) {
                    reductions.append((x, y))       // reduce x to y
                } else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remainingRelations.forall(r => r == x || !r.getNodes().contains(v)))) {
                        // variables in diff belong to x only
                        // create AuxiliaryRelation with variables x.getNodes() - diff
                        removes.append((x, diff))
                    }
                }
            }

            if (reductions.isEmpty && removes.isEmpty) {
                // try to find some non-output variables that belongs to one relation
                val outputRelations = remainingRelations.filter(r => r.getNodes().intersect(outputVariables).nonEmpty)
                outputRelations.foreach(r => {
                    val variables = r.getNodes()
                    // nonOutput variables that belongs to r only
                    val removable = variables.filter(v => !outputVariables.contains(v) && remainingRelations.forall(p => p == r || !p.getNodes().contains(v)))
                    if (removable.nonEmpty) {
                        removes.append((r, removable))
                    }
                })
            }

            if (reductions.nonEmpty) {
                // perform reduction first if we have relations like R(A,B,C) and S(B,C)
                Some(reductions.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)))
            } else if (removes.nonEmpty) {
                Some(removes.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }))
            } else {
                None
            }
        }

        def phase2NextStates(stateAndSubset: (GyoState, Set[Relation])): Option[List[(GyoState, Set[Relation])]] = {
            val gyoState = stateAndSubset._1
            val subset = stateAndSubset._2
            val remaining = gyoState.getRelationalHyperGraph.getEdges()
            val reductions = ListBuffer.empty[(Relation, Relation)]
            val removes = ListBuffer.empty[(Relation, Set[Variable])]

            for {
                x <- remaining
                y <- remaining
                if x != y && x.getNodes().intersect(y.getNodes()).nonEmpty
            } {
                if (x.getNodes().subsetOf(y.getNodes()))
                    reductions.append((x, y))
                else {
                    val diff = x.getNodes() -- y.getNodes()
                    if (diff.forall(v => remaining.forall(r => r == x || !r.getNodes().contains(v)))) {
                        removes.append((x, diff))
                    }
                }
            }

            if (reductions.nonEmpty) {
                Some(reductions.toList.map(p => attach(p._2, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)).map(s => (s, subset)))
            } else if (removes.nonEmpty) {
                Some(removes.toList.map(p => {
                    val newRelation = p._1.removeVariables(p._2)
                    replace(newRelation, p._1, gyoState.getRelationalHyperGraph, gyoState.getForest)
                }).map(s => (s, subset)))
            } else {
                None
            }
        }

        // phase 1
        val seenStates = mutable.HashSet.empty[GyoState]
        val queue1 = mutable.Queue.empty[GyoState]
        val stableStates = mutable.HashSet.empty[GyoState]
        queue1.enqueue(initState)
        while (queue1.nonEmpty) {
            val head = queue1.dequeue()
            val next = phase1NextStates(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenStates.contains(p)) {
                        seenStates.add(p)
                        queue1.enqueue(p)
                    }
                })
            } else {
                // a state is stable if we cannot find any next state from it
                stableStates.add(head)
            }
        }

        assert(stableStates.nonEmpty)

        // terminate if
        // 1. outputVariables.nonEmpty, and
        // 2. there is some non-output variables in stable state. e.g., R(A,B) x S(B,C), output = [A,C], and
        // 3. terminateIfNonFreeConnex is set.
        if (outputVariables.nonEmpty &&
            stableStates.exists(s => s.getRelationalHyperGraph.getEdges().exists(r => r.getNodes().exists(v => !outputVariables.contains(v))))) {
            isFreeConnex = false
            if (terminateIfNonFreeConnex)
                return None
        }

        // phase 2
        val finalStates = mutable.HashSet.empty[(GyoState, Set[Relation])]
        val queue2 = mutable.Queue.empty[(GyoState, Set[Relation])]
        seenStates.clear()
        // search final states from stable states
        stableStates.foreach(state => queue2.enqueue((state, state.getRelationalHyperGraph.getEdges())))
        while (queue2.nonEmpty) {
            val head = queue2.dequeue()
            val next = phase2NextStates(head)
            if (next.nonEmpty) {
                val list = next.get
                list.foreach(p => {
                    if (!seenStates.contains(p._1)) {
                        seenStates.add(p._1)
                        queue2.enqueue(p)
                    }
                })
            } else {
                finalStates.add(head)
            }
        }

        // in final state, there should be only one relation in hyperGraph
        if (finalStates.map(t => t._1).forall(fs => fs.getRelationalHyperGraph.getEdges().size == 1)) {
            val rootsAndSubset = finalStates.map(s => (s._1.getForest.getTree(s._1.getRelationalHyperGraph.getEdges().head), s._2)).toList
            Some(GyoResult(rootsAndSubset.map(ras => {
                val (root, edges, hyperGraph) = convertToJoinTreeWithHyperGraph(ras._1)
                val joinTree = JoinTree(root, edges, if (outputVariables.nonEmpty) ras._2.intersect(hyperGraph.getEdges()) else Set())
                (joinTree, hyperGraph)
            }), isFreeConnex))
        } else {
            None
        }
    }

    // traverse the tree nodes from the root, construct a JoinTree and RelationalHyperGraph
    private def convertToJoinTreeWithHyperGraph(root: TreeNode): (Relation, Set[JoinTreeEdge], RelationalHyperGraph) = {
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
                (relation, Set(), RelationalHyperGraph.EMPTY.addHyperEdge(relation))
            case InternalTreeNode(relation, children) =>
                val childrenResult = children.map(n => fun(n, relation))
                    .foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, r) => (z._1 ++ r._1, z._2 ++ r._2))
                val graph = childrenResult._2.foldLeft(RelationalHyperGraph.EMPTY)((g, r) => g.addHyperEdge(r)).addHyperEdge(relation)
                (relation, childrenResult._1, graph)
        }
    }
}
