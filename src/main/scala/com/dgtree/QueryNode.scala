package com.dgtree;

import scala.collection.mutable.ArrayBuffer

/** A QueryTree Node 
 *
 * @param dgTreeNode the dgTree node contained in the query tree node
 * @param sStar      datagraph ids covered by the feature graph of dgTreeNode
 * @param matches    matches of feature graph in query graph Q
 * @param score      ordering key for the query tree nodes
 */
class QNode(
    val dgTreeNode: DGTreeNode,
    var sStar: Set[Int] = null, 
    var matches: ArrayBuffer[ArrayBuffer[Int]] = null,  
    var score: Double = 0
    ) extends java.io.Serializable with Ordered[QNode] {

    def compare(that: QNode): Int = this.score.compareTo(that.score)
}
            
