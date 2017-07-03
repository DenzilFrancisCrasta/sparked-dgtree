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


        // each graph in SStar set of root DGTree nodes are candidates 
        val candidateGraphsRDD = dgTree(0).flatMap(node => node.SStar.map((node.nUUID, _))) 
        println(candidateGraphsRDD.count)




        /*
        levels += dgTree(0).map( dgTreeNode => {

            new QNode(dgTreeNode)
        })
        */
        
    
    }  


}
