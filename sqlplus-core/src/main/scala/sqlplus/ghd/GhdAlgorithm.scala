package sqlplus.ghd

import sqlplus.expression.{Variable, VariableManager}
import sqlplus.graph.{BagRelation, Relation, RelationalHyperGraph, TableScanRelation}
import sqlplus.types.IntDataType

/**
 * Use brute-force to compute a generalized hypertree decomposition for a given hypergraph.
 * This implementation is motivated by the GHDSolver in EmptyHeaded
 * (https://github.com/HazyResearch/EmptyHeaded/blob/master/query_compiler/src/main/scala/solver/GHDSolver.scala).
 */
class GhdAlgorithm {
    def run(hyperGraph: RelationalHyperGraph): RelationalHyperGraph = {
        val roots = decompose(hyperGraph.edges, Set.empty).flatMap(root => root.transform())
        // clear memoized values in GhdScoreAssigner before computing scores
        GhdScoreAssigner.clear()
        val pickedRoot = roots.toList.minBy(n => n.score)
        convertGhdNodeToHyperGraph(pickedRoot)
    }

    private def convertGhdNodeToHyperGraph(root: GhdNode): RelationalHyperGraph = {
        var hyperGraph = RelationalHyperGraph.EMPTY

        def visit(node: GhdNode): Unit = {
            if (node.relations.size == 1) {
                hyperGraph = hyperGraph.addHyperEdge(node.relations.head)
            } else {
                hyperGraph = hyperGraph.addHyperEdge(BagRelation.createFrom(node.relations))
            }
            node.childrenNode.foreach(visit)
        }

        visit(root)
        hyperGraph
    }

    private def decompose(edges: Set[Relation], parentVariables: Set[Variable]): Set[Bag] = {
        val sizes = (1 to edges.size).toSet
        sizes.flatMap(trySize => {
            edges.toList.combinations(trySize).flatMap(picked => {
                val remain = edges -- picked
                val pickedVariables = picked.flatMap(r => r.getVariableList()).toSet
                // consider only the maximal bag
                if (isMaximal(pickedVariables, remain)) {
                    if (isValid(pickedVariables, parentVariables, remain)) {
                        val partitions = getPartitions(remain)
                        val partitionBags = partitions.map(p => decompose(p, pickedVariables))
                        Set(new Bag(picked.toSet, partitionBags))
                    } else {
                        Set.empty[Bag]
                    }
                } else {
                    Set.empty[Bag]
                }
            })
        })
    }

    private def isMaximal(pickedVariables: Set[Variable], remain: Set[Relation]): Boolean = {
        // the picked bag is maximal if we can not find another relation in the remain set such that
        // its variables are all covered by the picked bag
        !remain.exists(r => r.getVariableList().forall(v => pickedVariables.contains(v)))
    }

    private def isValid(pickedVariables: Set[Variable], parentVariables: Set[Variable], remain: Set[Relation]) = {
        // check whether there exists some variable that lies in both parent and some children bags
        // if this is true, then some variables don't form a connected component in the tree
        // thus this is a invalid decomposition
        remain.forall(r => r.getVariableList().toSet.intersect(parentVariables).subsetOf(pickedVariables))
    }

    private def getPartitions(remain: Set[Relation]): Set[Set[Relation]] = {
        val dict = remain.flatMap(r => r.getVariableList().map(v => (v, r)))
            .groupBy(t => t._1).map(t => (t._1, t._2.map(x => x._2)))

        def partition(from: Set[Relation], seen: Set[Relation], result: Set[Relation]): Set[Relation] = {
            if (from.isEmpty) {
                result
            } else {
                val reachable = from.flatMap(r => r.getVariableList().flatMap(dict))
                partition(reachable -- seen, seen ++ from, result ++ from)
            }
        }

        def loop(available: Set[Relation], result: Set[Set[Relation]]): Set[Set[Relation]] = {
            if (available.isEmpty) {
                result
            } else {
                val start = available.head
                val newPartition = partition(Set(start), Set.empty, Set.empty)
                loop(available -- newPartition, result + newPartition)
            }
        }

        loop(remain, Set.empty)
    }
}
