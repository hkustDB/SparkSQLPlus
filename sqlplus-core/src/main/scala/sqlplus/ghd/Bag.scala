package sqlplus.ghd

import sqlplus.graph.Relation

import scala.language.implicitConversions

/**
 *
 * @param relations the set of relations in this bag
 * @param childrenCandidates set of children GhdNodes for each partition
 */
class Bag(val relations: Set[Relation], val childrenCandidates: Set[Set[Bag]]) {
    private implicit class CrossProduct(xss: Set[Array[Bag]]) {
        def x(ys: Set[Bag]): Set[Array[Bag]] = for {xs <- xss; y <- ys} yield xs :+ y
    }

    def transform(assigner: GhdScoreAssigner): Set[GhdNode] = {
        val childrenNodes = childrenCandidates
                .foldLeft(Set(Array.empty[Bag]))((z, bag) => z x bag)
                .map(arr => arr.flatMap(bag => bag.transform(assigner)).toSet)
        childrenNodes.map(c => new GhdNode(relations, c, assigner))
    }
}
