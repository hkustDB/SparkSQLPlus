package sqlplus.ghd

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.expression.VariableManager
import sqlplus.graph.{BagRelation, RelationalHyperGraph, TableScanRelation}
import sqlplus.types.IntDataType

class GhdAlgorithmTest {
    @Test
    def testTriangleFull(): Unit = {
        val algorithm = new GhdAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3")

        val r4 = new TableScanRelation("R4", List(v4, v5), "R4")
        val r5 = new TableScanRelation("R5", List(v5, v6), "R5")
        val r6 = new TableScanRelation("R6", List(v6, v4), "R6")

        val r7 = new TableScanRelation("R7", List(v3, v4), "R7")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)
            .addHyperEdge(r4).addHyperEdge(r5).addHyperEdge(r6).addHyperEdge(r7)

        val result = algorithm.run(hyperGraph, Set(v1, v2, v3, v4, v5, v6))
        assertTrue(result.joinTreeWithHyperGraphs.size == 1)

        val (joinTree, graph) = result.joinTreeWithHyperGraphs.head
        assertTrue(joinTree.subset.size == 3)
        assertTrue(joinTree.root == r7)
        joinTree.edges.foreach(e => {
            if (e.getSrc.isInstanceOf[TableScanRelation]) {
                val bag = e.getDst.asInstanceOf[BagRelation]
                val internal = bag.getInternalRelations
                assert(internal.size == 3)
                assertTrue((internal.contains(r1) && internal.contains(r2) && internal.contains(r3))
                    || (internal.contains(r4) && internal.contains(r5) && internal.contains(r6)))
            } else {
                val bag = e.getSrc.asInstanceOf[BagRelation]
                val internal = bag.getInternalRelations
                assert(internal.size == 3)
                assertTrue((internal.contains(r1) && internal.contains(r2) && internal.contains(r3))
                    || (internal.contains(r4) && internal.contains(r5) && internal.contains(r6)))
            }
        })

        assertTrue(graph.getEdges().size == 3)
        assertTrue(graph.getEdges().count(r => r.isInstanceOf[TableScanRelation]) == 1)
        assertTrue(graph.getEdges().count(r => r.isInstanceOf[BagRelation]) == 2)
    }

    @Test
    def testTriangleNonFull(): Unit = {
        val algorithm = new GhdAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3")

        val r4 = new TableScanRelation("R4", List(v4, v5), "R4")
        val r5 = new TableScanRelation("R5", List(v5, v6), "R5")
        val r6 = new TableScanRelation("R6", List(v6, v4), "R6")

        val r7 = new TableScanRelation("R7", List(v3, v4), "R7")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)
            .addHyperEdge(r4).addHyperEdge(r5).addHyperEdge(r6).addHyperEdge(r7)

        val result = algorithm.run(hyperGraph, Set(v3, v4))
        assertTrue(result.joinTreeWithHyperGraphs.size == 1)

        val (joinTree, graph) = result.joinTreeWithHyperGraphs.head
        assertTrue(joinTree.subset.size == 1)
        assertTrue(joinTree.subset.head == r7)
        assertTrue(joinTree.root == r7)
        joinTree.edges.foreach(e => {
            if (e.getSrc.isInstanceOf[TableScanRelation]) {
                val bag = e.getDst.asInstanceOf[BagRelation]
                val internal = bag.getInternalRelations
                assert(internal.size == 3)
                assertTrue((internal.contains(r1) && internal.contains(r2) && internal.contains(r3))
                    || (internal.contains(r4) && internal.contains(r5) && internal.contains(r6)))
            } else {
                val bag = e.getSrc.asInstanceOf[BagRelation]
                val internal = bag.getInternalRelations
                assert(internal.size == 3)
                assertTrue((internal.contains(r1) && internal.contains(r2) && internal.contains(r3))
                    || (internal.contains(r4) && internal.contains(r5) && internal.contains(r6)))
            }
        })

        assertTrue(graph.getEdges().size == 3)
        assertTrue(graph.getEdges().count(r => r.isInstanceOf[TableScanRelation]) == 1)
        assertTrue(graph.getEdges().count(r => r.isInstanceOf[BagRelation]) == 2)
    }
}
