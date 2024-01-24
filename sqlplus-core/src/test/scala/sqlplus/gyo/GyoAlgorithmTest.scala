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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(variableManager, hyperGraph, Set(v1, v2, v3, v4), true).head
        assertTrue(result.candidates.size == 3)

        val joinTrees = result.candidates.map(t => t._1)
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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(variableManager, hyperGraph, Set(v1, v2), true).head
        assertTrue(result.candidates.size == 1)

        val joinTrees = result.candidates.map(t => t._1)
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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(variableManager, hyperGraph, Set(v2, v3), true).head
        assertTrue(result.candidates.size == 1)

        val joinTrees = result.candidates.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.subset.size == 1))
        assertTrue(joinTrees.forall(t => t.subset.head == r2))
        joinTrees.foreach(t => {
            assertTrue(t.root == r2)
            assertTrue(t.edges.exists(e => (e.getSrc == r1 && e.getDst == r2) || (e.getSrc == r2 && e.getDst == r1)))
            assertTrue(t.edges.exists(e => (e.getSrc == r2 && e.getDst == r3) || (e.getSrc == r3 && e.getDst == r2)))
        })
    }

    @Test
    def testTerminateIfNonFreeConnex(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(variableManager, hyperGraph, Set(v1, v3, v4), true)
        assertTrue(result.isEmpty)
    }

    @Test
    def testNonFreeConnex(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty)
        val r4 = new TableScanRelation("R4", List(v4, v5), "R4", Set.empty)
        val r5 = new TableScanRelation("R5", List(v5, v6), "R5", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)
            .addHyperEdge(r4).addHyperEdge(r5)

        val result = algorithm.run(variableManager, hyperGraph, Set(v2, v4, v5), false).head
        assertTrue(result.candidates.size == 3)

        val jointrees = result.candidates.map(_._1).toSet
        // root must be r2, r3, or r4
        assertTrue(jointrees.count(t => t.root == r2) == 1)
        assertTrue(jointrees.count(t => t.root == r3) == 1)
        assertTrue(jointrees.count(t => t.root == r4) == 1)
    }

    @Test
    def testCyclic(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty)
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3", Set.empty)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(variableManager, hyperGraph, Set(v1, v2, v3), true)
        assertTrue(result.isEmpty)

        // the algorithm should terminate even if terminateIfNonFreeConnex is set to false
        val result2 = algorithm.run(variableManager, hyperGraph, Set(v1, v2, v3), false)
        assertTrue(result2.isEmpty)
    }
}
