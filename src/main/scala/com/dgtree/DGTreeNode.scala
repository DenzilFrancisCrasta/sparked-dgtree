package com.dgtree;

import scala.collection.mutable.ArrayBuffer

/** A node in the DGTree Index 
 *
 *  @param parentUUID parents unique id 
 *  @param fGraph   the feature graph
 *  @param growEdge edge which is used to grow the feature graph of
 *                  the current node from its parent feature graph
 *  @param edgeType type of the grow edge one of {OPEN, CLOSED}
 *  @param S        support of the feature graph i.e set of datagraphs
 *                  which contain this feature graph in them 
 *  @param SStar    temporary for growing the support S
 *  @param matches  key-value RDD from graph-ids in the support to the list 
 *                  of matches of the feature graph in the graph 
 *  @param children children of the current node in the tree index
 *  @param score    an ordering key for DGTree nodes
 *  @param matchesSize total number of matches of fGrpah across graphs in S  
 */
class DGTreeNode (
    val parentUID   : String, 
    var fGraph      : Graph,
    var growEdge    : Edge,
    var edgeType    : Int,
    var S           : Set[Int],
    var SStar       : Set[Int],
    var matches     : Map[Int, List[List[Int]]], 
    var score       : Double = 0.0,
    var matchesSize : Double = 0.0,
    var children    : ArrayBuffer[DGTreeNode] = ArrayBuffer[DGTreeNode]()
) extends java.io.Serializable with Ordered[DGTreeNode]{ 

    val nUUID = java.util.UUID.randomUUID.toString

    def compare(that: DGTreeNode): Int = this.score.compareTo(that.score)

    def calcScore(BIAS_SCORE: Double) = {

        val coveredDatagraphs            = S.size.toDouble 
        val exclusivelyCoveredDatagraphs = SStar.size.toDouble
        val avgMatches                   = matchesSize / coveredDatagraphs 
        val score                        = exclusivelyCoveredDatagraphs / avgMatches
        
        this.score = if (edgeType == 0) score + BIAS_SCORE else score
    } 

    override def toString = {
            val growEdge = "\n Grow Edge: " + this.growEdge.toString  + "\n"
            val sstar = this.SStar.mkString(",")

            val fGraph = "\n FGraph \n" +  this.fGraph.toString + "\n"

            val matches = "\n Matches \n" 
            val matches_desription = this.matches.map(t => {
                                            " M(g.id="+ t._1 + ") :" + t._2.map((s:List[Int]) => s.mkString(",")).mkString(" ")
                                     }).mkString("\n") + "\n"

        //  growEdge + fGraph + matches + matches_desription
            "id " + this.nUUID + "\n"+"parent id " + this.parentUID + growEdge + "\n"+ fGraph +"\n"+sstar 

    }
}
