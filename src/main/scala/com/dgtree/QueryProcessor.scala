package com.dgtree

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer 
import org.apache.spark.storage.StorageLevel

/** Searches the DGTree Index and returns datagraphs that are 
 *  subgraph isomorphic  to Q
 *
 *  @param dgTree the DGTreeIndex 
 */
class QueryProcessor (
    dgTree: ArrayBuffer[RDD[DGTreeNode]],
    dataGraphsMapRDD: RDD[(Int, Graph)] ) {

    val levels = new ArrayBuffer[RDD[QNode]]()

    def superGraphSearch(queryGraphsMapRDD: RDD[(Int, Graph)]) = {


        // each graph in SStar of root DGTree nodes are candidate graphs that need to be considered
        val graphStatsRDD = dataGraphsMapRDD.mapValues(graph => (graph.vertexCount, graph.edgeCount))
        val candidateGraphIDsRDD = dgTree(0).flatMap(node => node.SStar.map((_, node.nUUID))) 
        val candidateGraphsRDD = candidateGraphIDsRDD.join(graphStatsRDD) 

        

        //RDD contentes  ((QGrpahID, QGraph), (dGraphId, ( dNode.id, (VCount, ECount))) 
        val eligibleCandidateGraphsRDD = queryGraphsMapRDD.cartesian(candidateGraphsRDD)
                                                          .filter(cq => {
            val QGraph = cq._1._2 
            val dGraphVertexCount = cq._2._2._2._1
            val dGraphEdgeCount = cq._2._2._2._2
            
            QGraph.vertexCount >= dGraphVertexCount && QGraph.edgeCount >= dGraphEdgeCount
        
        })


        //println("Queries to be processed " + queryGraphsMapRDD.count)
        //println("Graph Database Size " + candidateGraphsRDD.count)
        println("eligibleCandidates " + eligibleCandidateGraphsRDD.count)



        /*
        levels += dgTree(0).map( dgTreeNode => {

            new QNode(dgTreeNode)
        })
        */
        
    
    }  


}
