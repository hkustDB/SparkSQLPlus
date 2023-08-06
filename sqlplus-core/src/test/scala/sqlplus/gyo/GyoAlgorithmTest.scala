package sqlplus.gyo

import org.junit.Assert.assertTrue
import org.junit.Test
import sqlplus.expression.VariableManager
import sqlplus.graph.{RelationalHyperGraph, TableScanRelation}
import sqlplus.types.IntDataType

class GyoAlgorithmTest {
    @Test
    def testLine3Full(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2, v3, v4)).get
        assertTrue(result.joinTreeWithHyperGraphs.size == 3)

        val joinTrees = result.joinTreeWithHyperGraphs.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.subset.size == 3))
        assertTrue(joinTrees.forall(t => t.subset.intersect(Set(r1, r2 ,r3)).size == 3))
        joinTrees.foreach(t => {
            assertTrue(t.root == r1 || t.root == r2 || t.root == r3)
            assertTrue(t.edges.exists(e => (e.getSrc == r1 && e.getDst == r2) || (e.getSrc == r2 && e.getDst == r1)))
            assertTrue(t.edges.exists(e => (e.getSrc == r2 && e.getDst == r3) || (e.getSrc == r3 && e.getDst == r2)))
        })
    }

    @Test
    def testLine3NonFull1(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2)).get
        assertTrue(result.joinTreeWithHyperGraphs.size == 1)

        val joinTrees = result.joinTreeWithHyperGraphs.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.subset.size == 1))
        assertTrue(joinTrees.forall(t => t.subset.head == r1))
        joinTrees.foreach(t => {
            assertTrue(t.root == r1)
            assertTrue(t.edges.exists(e => (e.getSrc == r1 && e.getDst == r2) || (e.getSrc == r2 && e.getDst == r1)))
            assertTrue(t.edges.exists(e => (e.getSrc == r2 && e.getDst == r3) || (e.getSrc == r3 && e.getDst == r2)))
        })
    }

    @Test
    def testLine3NonFull2(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v2, v3)).get
        assertTrue(result.joinTreeWithHyperGraphs.size == 1)

        val joinTrees = result.joinTreeWithHyperGraphs.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.subset.size == 1))
        assertTrue(joinTrees.forall(t => t.subset.head == r2))
        joinTrees.foreach(t => {
            assertTrue(t.root == r2)
            assertTrue(t.edges.exists(e => (e.getSrc == r1 && e.getDst == r2) || (e.getSrc == r2 && e.getDst == r1)))
            assertTrue(t.edges.exists(e => (e.getSrc == r2 && e.getDst == r3) || (e.getSrc == r3 && e.getDst == r2)))
        })
    }

    @Test
    def testNonFreeConnex(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v3, v4))
        assertTrue(result.isEmpty)
    }

    @Test
    def testCyclic(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1")
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2")
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3")

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2, v3))
        assertTrue(result.isEmpty)
    }
}
