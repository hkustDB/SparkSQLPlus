package sqlplus.ghd

import sqlplus.expression.Variable
import sqlplus.graph._

/**
 * Use brute-force to compute a generalized hypertree decomposition for a given hypergraph.
 * This implementation is motivated by the GHDSolver in EmptyHeaded
 * (https://github.com/HazyResearch/EmptyHeaded/blob/master/query_compiler/src/main/scala/solver/GHDSolver.scala).
 */
class GhdAlgorithm {
    private def convertGhdNode(root: GhdNode): (Set[JoinTreeEdge], Set[Relation], Relation) = {
        def toRelation(node: GhdNode): Relation = {
            if (node.relations.size == 1)
                node.relations.head
            else
                BagRelation.createFrom(node.relations)
        }

        def visit(node: GhdNode, parent: Option[Relation]): (Set[JoinTreeEdge], Set[Relation], Relation) = {
            val relation = toRelation(node)

            val result = node.childrenNode.foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, child) => {
                val childResult = visit(child, Some(relation))
                (z._1 ++ childResult._1, z._2 ++ childResult._2)
            })

            if (parent.nonEmpty) {
                (result._1 + new JoinTreeEdge(parent.get, relation), result._2 + relation, null)
            } else {
                // visiting the root node
                (result._1, result._2 + relation, relation)
            }
        }

        visit(root, None)
    }

    private def convertGhdNodeAndRemoveRedundancy(root: GhdNode,
                                                  auxiliaryRelationToConvertedComponent: Map[Relation, (Set[JoinTreeEdge], Set[Relation], Relation)]): (Set[JoinTreeEdge], Set[Relation], Relation) = {
        val auxiliaryRelations = auxiliaryRelationToConvertedComponent.keySet

        def visitChildren(childrenNode: Set[GhdNode], parent: Relation): (Set[JoinTreeEdge], Set[Relation]) = {
            childrenNode.foldLeft((Set.empty[JoinTreeEdge], Set.empty[Relation]))((z, child) => {
                val childResult = visit(child, Some(parent))
                (z._1 ++ childResult._1, z._2 ++ childResult._2)
            })
        }

        def visit(node: GhdNode, parent: Option[Relation]): (Set[JoinTreeEdge], Set[Relation], Relation) = {
            val (relation, childrenResult, additionalRelations, additionalEdges) = if (node.relations.intersect(auxiliaryRelations).nonEmpty) {
                val auxiliaries = node.relations.intersect(auxiliaryRelations)
                val nonAuxiliaries = node.relations.diff(auxiliaries)
                // if the variables in a auxiliary relation is fully covered by other non-auxiliary relations,
                // we can remove the auxiliary relation and append the supporting relation to this relation
                val removeAuxiliaries = auxiliaries.filter(r => r.getVariableList().toSet.subsetOf(nonAuxiliaries.flatMap(na => na.getVariableList())))
                val keepAuxiliaries = auxiliaries.diff(removeAuxiliaries)

                val remains = nonAuxiliaries ++ keepAuxiliaries
                val relation = if (remains.size == 1) remains.head else BagRelation.createFrom(remains)
                val childrenResult = visitChildren(node.childrenNode, relation)
                (relation, childrenResult, keepAuxiliaries,
                    removeAuxiliaries.map(r => new JoinTreeEdge(relation, auxiliaryRelationToConvertedComponent(r)._3))
                        ++ keepAuxiliaries.map(r => new JoinTreeEdge(relation, auxiliaryRelationToConvertedComponent(r)._3)))
            } else {
                val relation = if (node.relations.size == 1) node.relations.head else BagRelation.createFrom(node.relations)
                val childrenResult = visitChildren(node.childrenNode, relation)
                (relation, childrenResult, Set.empty, Set.empty)
            }

            if (parent.nonEmpty) {
                ((childrenResult._1 + new JoinTreeEdge(parent.get, relation)) ++ additionalEdges, (childrenResult._2 + relation) ++ additionalRelations, null)
            } else {
                (childrenResult._1 ++ additionalEdges, (childrenResult._2 + relation) ++ additionalRelations, relation)
            }
        }

        visit(root, None)
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

    def run(hyperGraph: RelationalHyperGraph, outputVariables: Set[Variable]): GhdResult = {
        val hyperEdges = hyperGraph.getEdges()

        // for outputVariables, the reachable is empty set
        // for nonOutputVariables, the reachable is the set of all relations that contains this variable
        val reachable: Map[Variable, Set[Relation]] = hyperEdges.flatMap(r => r.getVariableList()
            .map(v => (v, r))).groupBy(t => t._1)
            .map(kv => if (outputVariables.contains(kv._1)) (kv._1, Set.empty[Relation]) else (kv._1, kv._2.map(t => t._2)))

        // the relations that variables are all output variables
        val outputRelations = hyperEdges.filter(r => r.getVariableList().forall(v => outputVariables.contains(v)))
        val remainRelations = hyperEdges.diff(outputRelations)

        val components = findComponents(remainRelations, reachable)
        val componentAndOutputVariables = components.map(c => (c, c.flatMap(r => r.getVariableList()).intersect(outputVariables)))
        val componentDecompositionAndOutputVariables = componentAndOutputVariables.map(t => (decompose(t._1, t._2), t._2))
        val auxiliaryRelationToConvertedComponent: Map[Relation, (Set[JoinTreeEdge], Set[Relation], Relation)] =
            componentDecompositionAndOutputVariables.map(t => {
            val decompositions = t._1
            val componentOutputVariables = t._2
            val roots = decompositions.flatMap(b => b.transform())
            GhdScoreAssigner.clear()
            val pickedRoot = roots.toList.minBy(n => n.score)
            val convertResult = convertGhdNode(pickedRoot)
            // the root relation of the component should have all output variables
            assert(componentOutputVariables.subsetOf(convertResult._3.getVariableList().toSet))
            // create an auxiliary relation from the root relation
            val auxiliary = convertResult._3.project(componentOutputVariables)
            // here we do not add the JoinTreeEdge between the root relation and the auxiliary relation
            // because the auxiliary relation may be absorbed later
            (auxiliary, convertResult)
        }).toMap

        if (outputVariables.nonEmpty) {
            val outputAndAuxiliaryRelations = outputRelations ++ auxiliaryRelationToConvertedComponent.keySet
            val roots = decompose(outputAndAuxiliaryRelations, Set.empty).flatMap(b => b.transform())
            GhdScoreAssigner.clear()
            val minScore = roots.toList.map(n => n.score).min
            val pickedRoots = roots.toList.filter(n => Math.abs(n.score._1 - minScore._1) < 0.01 && n.score._2 == minScore._2)
            val convertResults = pickedRoots.map(pickedRoot => convertGhdNodeAndRemoveRedundancy(pickedRoot, auxiliaryRelationToConvertedComponent))

            val result = convertResults.map(convertResult => {
                val combined = auxiliaryRelationToConvertedComponent.values.foldLeft((convertResult._1, convertResult._2))((z, t) => {
                    (z._1 ++ t._1, z._2 ++ t._2)
                })
                val joinTree = JoinTree(convertResult._3, combined._1, convertResult._2, false)
                val hypergraph = combined._2.foldLeft(RelationalHyperGraph.EMPTY)((z, e) => z.addHyperEdge(e))
                (joinTree, hypergraph)
            })

            GhdResult(result)
        } else {
            assert(auxiliaryRelationToConvertedComponent.size == 1)
            assert(auxiliaryRelationToConvertedComponent.head._1.getNodes().isEmpty)
            val convertResult = auxiliaryRelationToConvertedComponent.head._2
            val joinTree = JoinTree(convertResult._3, convertResult._1, convertResult._2, false)
            val hypergraph = convertResult._2.foldLeft(RelationalHyperGraph.EMPTY)((z, e) => z.addHyperEdge(e))
            GhdResult(List((joinTree, hypergraph)))
        }
    }

    def findComponents(relations: Set[Relation], reachable: Map[Variable, Set[Relation]], result: List[Set[Relation]] = List.empty): List[Set[Relation]] = {
        def search(current: Set[Relation], component: Set[Relation]): Set[Relation] = {
            if (current.isEmpty)
                component
            else {
                // for each variable in each relation in current,
                // add all reachable relations to next if not visited
                val visited = component.union(current)
                val next = current.flatMap(r => r.getVariableList()).flatMap(reachable).diff(visited)
                search(next, visited)
            }
        }

        if (relations.isEmpty)
            result
        else {
            val picked = relations.head
            val component = search(Set(picked), Set.empty)
            findComponents(relations.diff(component), reachable, component :: result)
        }
    }
}