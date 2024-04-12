package sqlplus.gyo

import org.junit.Assert.{assertFalse, assertTrue}
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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2, v3, v4), false)
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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2), false)
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

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v2, v3), false)
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
    def testNonFreeConnex(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 0)
        val r4 = new TableScanRelation("R4", List(v4, v5), "R4", Set.empty, 0)
        val r5 = new TableScanRelation("R5", List(v5, v6), "R5", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)
            .addHyperEdge(r4).addHyperEdge(r5)

        val result = algorithm.run(hyperGraph, Set(v2, v4, v5), false)
        assertTrue(result.candidates.size == 3)

        val jointrees = result.candidates.map(_._1).toSet
        // root must be r2, r3, or r4
        assertTrue(jointrees.count(t => t.root == r2) == 1)
        assertTrue(jointrees.count(t => t.root == r3) == 1)
        assertTrue(jointrees.count(t => t.root == r4) == 1)
    }

    @Test(expected = classOf[IllegalStateException])
    def testCyclic(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        val result = algorithm.run(hyperGraph, Set(v1, v2, v3), false)
    }

    @Test
    def testIsAcyclic1(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        assertTrue(algorithm.isAcyclic(hyperGraph))
    }

    @Test
    def testIsAcyclic2(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 0)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 0)
        val r3 = new TableScanRelation("R3", List(v3, v1), "R3", Set.empty, 0)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3)

        assertFalse(algorithm.isAcyclic(hyperGraph))
    }

    @Test
    def testPruning(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 6)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 2)
        val r3 = new TableScanRelation("R3", List(v3, v4), "R3", Set.empty, 3)
        val r4 = new TableScanRelation("R4", List(v4, v5), "R4", Set.empty, 4)
        val r5 = new TableScanRelation("R5", List(v5, v6), "R5", Set.empty, 5)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3).addHyperEdge(r4).addHyperEdge(r5)

        val result = algorithm.run(hyperGraph, Set(v2, v5), true)
        assertTrue(result.candidates.size == 1)

        val joinTrees = result.candidates.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.root == r2))
    }

    @Test
    def testPruning2(): Unit = {
        val algorithm = new GyoAlgorithm

        val variableManager = new VariableManager
        val v1 = variableManager.getNewVariable(IntDataType)
        val v2 = variableManager.getNewVariable(IntDataType)
        val v3 = variableManager.getNewVariable(IntDataType)
        val v4 = variableManager.getNewVariable(IntDataType)
        val v5 = variableManager.getNewVariable(IntDataType)
        val v6 = variableManager.getNewVariable(IntDataType)
        val v7 = variableManager.getNewVariable(IntDataType)

        val r1 = new TableScanRelation("R1", List(v1, v2), "R1", Set.empty, 999)
        val r2 = new TableScanRelation("R2", List(v2, v3), "R2", Set.empty, 2)
        val r3 = new TableScanRelation("R3", List(v2, v4), "R3", Set.empty, 3)
        val r4 = new TableScanRelation("R4", List(v2, v5), "R4", Set.empty, 4)
        val r5 = new TableScanRelation("R5", List(v2, v6), "R5", Set.empty, 5)
        val r6 = new TableScanRelation("R6", List(v2, v7), "R6", Set.empty, 6)

        val hyperGraph = RelationalHyperGraph.EMPTY.addHyperEdge(r1).addHyperEdge(r2).addHyperEdge(r3).addHyperEdge(r4).addHyperEdge(r5).addHyperEdge(r6)

        val result = algorithm.runWithFixRoot(hyperGraph, r1, true)
        val joinTrees = result.candidates.map(t => t._1)
        assertTrue(joinTrees.forall(t => t.getEdges().count(e => e.getSrc == t.root || e.getDst == t.root) < 4))
    }
}
