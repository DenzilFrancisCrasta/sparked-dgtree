package com.dgtree;

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer 

class DGTree(
    dataGraphsMapRDD: RDD[(Int, Graph)]
    ) extends java.io.Serializable {

    val levels = new ArrayBuffer[RDD[DGTreeNode]]()
   
    /** Bootstrap the tree index building process */
    def bootstrap() = {

        val distinctNodeGutsRDD = dataGraphsMapRDD.values.flatMap( g => {
            for (i <- 0 until g.vertexLabels.size) 
                yield (g.vertexLabels(i),(g.id, List(i))) 
        }).groupByKey()

        val firstLevelNodesRDD = distinctNodeGutsRDD.map( nodeAndGuts => {

            val nodeLabel = nodeAndGuts._1

            // Feature Graph has one vertex whose label is provided by nodeLabel
            val fGraph = new Graph(0, 1, 0, Array(nodeLabel))

            // List of the graph id's which contain nodeLabel traditionally called support
            val support = nodeAndGuts._2.map(_._1).toSet
            val matches = nodeAndGuts._2.groupBy(kv => kv._1).mapValues(_.map(_._2).toList).map(identity)  

            // Add the DGTreeNode to the list of rootNodes being constructed
            new DGTreeNode(null, fGraph, null, 0, support, support, matches)
            
        }).persist()

        levels.append(firstLevelNodesRDD)

    } 

    def growNextLevel() = {

        println(levels.size)

        val currentLevelRDD = levels(levels.size -1)

        val matchesPerGraphIDMapRDD = currentLevelRDD.flatMap( node => {
            node.matches.map( graphIdAndMatches => ( graphIdAndMatches._1, (node.UID, graphIdAndMatches._2, node.fGraph) ) ) 
        })

        val matchesGraphMapRDD = matchesPerGraphIDMapRDD.join( dataGraphsMapRDD) 

        println("Count of matches graph map " + matchesGraphMapRDD.count())

        val nextLevelNodeGutsPhaseOneRDD = matchesGraphMapRDD.flatMap(graphAndMatches => {

           val parentId = graphAndMatches._2._1._1
           val matches  = graphAndMatches._2._1._2
           val fGraph   = graphAndMatches._2._1._3
           val G    = graphAndMatches._2._2

           matches.flatMap( matchG => { 

                  matchG.flatMap(fui => {

                       val neighbours = G.getNeighbours(fui) 

                       neighbours.map( vertexAndLabel => {

                                   val ui = matchG.indexOf(fui)

                                   val fuj = vertexAndLabel._1 
                                   val label  = vertexAndLabel._2 

                                   val index    = matchG.indexOf(fuj)
                                   val edgeType = if (index != -1) 0 else 1
                                   val uj       = if (index != -1) index else matchG.size

                                   if (uj > ui && !fGraph.isAnEdge(ui, uj, label)) {
                                          val e = new Edge(ui, uj, G.vertexLabels(fui), G.vertexLabels(fuj), label) 
                                          val newMatch = if (edgeType == 0){ matchG} else { matchG :+ fuj }
                                          ((e, parentId), (e, edgeType,  Set(G.id), List((G.id, newMatch)))) 
                                   }     
                                   else {
                                          ((null, "dummy"), (null, 0, null, null)) 
                                   }

                       }) 
                       
                    })

             })


        }).filter(_._1._1 != null)
          .reduceByKey((x, y) => (x._1, x._2, x._3.union(y._3), x._4 ++ y._4))

        println("Next level Node guts count " + nextLevelNodeGutsPhaseOneRDD.count())



        /*
        val children = new ArrayBuffer[DGTreeNode]()
        rootNodes.copyToBuffer(children)
        //rootNodes.foreach((node:DGTreeNode) =>  node.treeGrow(dataGraphsMapRDD))
        rootNodes(2).treeGrow(dataGraphsMapRDD)

        // create a dummy-root with the rootNodes as its children for single point entry
        new DGTreeNode(new Graph(-1, 0, 0, Array()), null, 0, null, null, null, children)
        */
    
    }

}






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
    val parentUID: String, 
    var fGraph: Graph,
    var growEdge: Edge,
    var edgeType: Int,
    var S: Set[Int],
    var SStar: Set[Int],
    var matches: Map[Int, List[List[Int]]], 
    var children: ArrayBuffer[DGTreeNode] = ArrayBuffer[DGTreeNode](),
    var score: Double = 0.0,
    var matchesSize: Double = 0.0
) extends java.io.Serializable { 

    val UID = java.util.UUID.randomUUID()
}

/*
    def growMatches(sRDD: RDD[Int], 
                    dataGraphsMapRDD: RDD[(Int, Graph)],
                    parentMatchesMapRDD: RDD[(Int, List[Int])],
                    gPlusGrowEdge: Edge) : RDD[(Int, List[Int])] = {

        val sGraphRDD = sRDD.map(g => (g, g))
                            .join(dataGraphsMapRDD)
                            .map(kv => (kv._1, kv._2._2))

        val gPlusMatches = sGraphRDD.join(parentMatchesMapRDD).flatMap( kv => {
                     
                // alias the datagraph belonging to the support S
                val G = kv._2._1 

                // alias the match of fGraph in G  
                val matchG = kv._2._2

                // for every matched vertex in G 
                matchG.flatMap(fui => {

                       val neighbours = G.getNeighbours(fui) 
                       neighbours.map( vertexAndLabel => {

                                   val ui = matchG.indexOf(fui)
                                   val fuj = vertexAndLabel._1 
                                   val label  = vertexAndLabel._2 
                                   val index    = matchG.indexOf(fuj)
                                   val edgeType = if (index != -1) 0 else 1

                                   val uj       = if (index != -1) index else matchG.size

                                   val candidateGrowEdge = new Edge(ui, uj, G.vertexLabels(fui), G.vertexLabels(fuj), label) 

                                   if (uj > ui && !fGraph.isAnEdge(ui, uj, label) && candidateGrowEdge == gPlusGrowEdge) {
                                      
                                          val newMatch = if (edgeType == 0){ matchG} else { matchG :+ fuj }
                                          (kv._1, newMatch) 
                                   }     
                                   else {
                                          // a null key-value pair which will be filtered out. 
                                          (-1, null) 
                                   }

                               }).filter(_._1 != -1) 
                       
                    })
                 
                })

        return gPlusMatches
    }   

    */
    /** instantiate a DGTree node from the guts 
     */
/*
    def makeNodeFromGuts(guts: (Edge, Int, Set[Int], Set[Int], Double, Double),
                         dataGraphsMapRDD: RDD[(Int, Graph)]) : DGTreeNode = {

        // convert the S and SStar sets into RDDs
        val sRDD     = S.filter(guts._4.contains(_)).persist()
        val sStarRDD = S.filter(guts._3.contains(_)).persist()

        // predicate to determine if featureGraph has grown by a vertex or not
        val extraVertex = if (guts._2 == 0 || guts._1 == null) false else true

        // create the clone of the parents fGraph with an extraVertex added based on the predicate 
        val fGraph = this.fGraph.getSuccessor(extraVertex)

        if (extraVertex) {
            val index = fGraph.vertexCount-1
            fGraph.vertexLabels(index) = guts._1.yLabel 
            fGraph.adjacencyList(index) = Nil 
        }

        // add the grow edge to the feature graph 
        var gPlusMatches: RDD[(Int, List[Int])] = null
        if ( guts._1 != null) {
            fGraph.addUndirectedEdge(guts._1.x, guts._1.y, guts._1.edgeLabel)

            // build the matches by growing matches of the parent
            val parentMatchesMapRDD = matches.filter( kv => guts._4.contains(kv._1))
            gPlusMatches = growMatches(sRDD, dataGraphsMapRDD, parentMatchesMapRDD, guts._1)
            gPlusMatches.persist()
        }
        
        new DGTreeNode(fGraph, guts._1, guts._2, sRDD, sStarRDD, gPlusMatches)
    }
*/
    /** Grows the tree rooted at this node */

/*
    def treeGrow(dataGraphsMapRDD: RDD[(Int, Graph)]): Unit = {

        var gutsMapRDD = candidateFeatures(dataGraphsMapRDD).persist()
        var C = this.SStar.collect().toSet

        val childQueue = new ArrayBuffer[DGTreeNode]()

        while (!C.isEmpty) {

            val bestFeatureTuple = bestFeature(C, gutsMapRDD) 
            val gPlusGuts = bestFeatureTuple._1 
            gutsMapRDD = bestFeatureTuple._2
            val gPlus = makeNodeFromGuts(gPlusGuts, dataGraphsMapRDD) 

            if (gPlusGuts._3.size > 1) {
               childQueue.append(gPlus)
            }
            else {
                    val graphId = gPlusGuts._3.head
                    //println("graph id in leaf " + graphId)
                    //gPlus.fGraph = dataGraphsMapRDD.filter(kv => kv._1 == graphId).values.collect()(0) 
                    gPlus.S = gPlus.SStar
            }

            if (!gPlusGuts._3.isEmpty) 
               children.append(gPlus) 

            //println(gPlus)
            //gPlus.fGraph.render(this.hashCode.toString, "./images")

            //println("Before child is chosen C = " + C.size)
            C = C diff gPlusGuts._3 
            //println("After child is chosen C  = " + C.size)

        }

        gutsMapRDD.unpersist()
        childQueue.par.foreach(_.treeGrow(dataGraphsMapRDD))

        // discard RDDs which wont be used beyond this point
        this.SStar.unpersist()
        this.matches.unpersist()

    }
*/
    /** Best candidate child node which has the highest score 
     */
/*
    def bestFeature(C: Set[Int], 
                    candidateGutsMapRDD: RDD[(Edge, (Edge, Int, Set[Int], Set[Int], Double, Double))] 
                   ): ((Edge, Int, Set[Int], Set[Int], Double, Double),RDD[(Edge, (Edge, Int, Set[Int], Set[Int], Double, Double))]) = {

        val prunedGutsMapRDD = candidateGutsMapRDD.mapValues(guts => {

                      val prunedSStar = guts._3.intersect(C)
                      // calculate score for the candidate 
                      val BIAS_SCORE = 100000
                      val exclusivelyCoveredDatagraphs = prunedSStar.size 
                      val coveredDatagraphs = guts._4.size 
                      val avgMatches: Double = guts._5.toDouble / coveredDatagraphs.toDouble 
                      val score: Double  = exclusivelyCoveredDatagraphs.toDouble / avgMatches
                      val finalScore = if (guts._2 == 0 && score != 0) score + BIAS_SCORE else score

                      (guts._1, guts._2, prunedSStar, guts._4, guts._5, finalScore)
                })


        //println("Before reducing")
        //C.foreach(print) 
        //prunedGutsMapRDD.collect().foreach(println)

        var gPlus = prunedGutsMapRDD.values.reduce((g1, g2) => if (g1._6 >= g2._6) g1 else g2)

        // if the score is zero then we have got a datagraph that is a subset of another datagraph.
        if (gPlus._6 == 0) {
            // modify the guts to use a NULL grow edge
            gPlus = (null, 0, C, C, 0.0, 0.0)
        }

        //println("After reducing")
        //prunedGutsMapRDD.collect().foreach(println)
        //val poppedGutsMapRDD = prunedGutsMapRDD.filter(_._1 != gPlus._1)
        //println("popped guts count " + poppedGutsMapRDD.count())
        (gPlus, prunedGutsMapRDD)
    }
*/
    /** Candidate child nodes to choose from */ 
/*
    def candidateFeatures( dataGraphsMapRDD: RDD[(Int, Graph)]) : RDD[(Edge, (Edge, Int, Set[Int], Set[Int], Double, Double))] =  {

        val supportStarMapRDD = SStar.map(g => (g, g))
                                    .join(dataGraphsMapRDD)
                                    .mapValues(kv => kv._2)


        val phaseOneCandidateGutsMapRDD = supportStarMapRDD.join(matches).flatMap(kv => {

                // alias the datagraph belonging to the support S
                val G = kv._2._1 

                // alias the match of fGraph in G  
                val matchG = kv._2._2

                // for every matched vertex in G 
                matchG.flatMap(fui => {

                       val neighbours = G.getNeighbours(fui) 
                       neighbours.map( vertexAndLabel => {

                                   val ui = matchG.indexOf(fui)

                                   val fuj = vertexAndLabel._1 
                                   val label  = vertexAndLabel._2 

                                   val index    = matchG.indexOf(fuj)
                                   val edgeType = if (index != -1) 0 else 1
                                   val uj       = if (index != -1) index else matchG.size

                                   if (uj > ui && !fGraph.isAnEdge(ui, uj, label)) {
                                          val e = new Edge(ui, uj, G.vertexLabels(fui), G.vertexLabels(fuj), label) 
                                          (e, (e, edgeType,  Set(G.id))) 
                                   }     
                                   else {
                                          (null, (null, 0, null)) 
                                   }

                               }).filter(_._1 != null) 
                       
                    })

             }).reduceByKey((x, y) => (x._1, x._2, x._3.union(y._3)))



        val supportMapRDD = S.map(g => (g, g))
                                    .join(dataGraphsMapRDD)
                                    .mapValues(kv => kv._2)


        val phaseTwoCandidateGutsMapRDD  = supportMapRDD.join(matches).flatMap(kv => {

                // alias the datagraph belonging to the support S
                val G = kv._2._1 

                // alias the match of fGraph in G  
                val matchG = kv._2._2

                // for every matched vertex in G 
                matchG.flatMap(fui => {

                       val neighbours = G.getNeighbours(fui) 
                       neighbours.map( vertexAndLabel => {

                                   val ui = matchG.indexOf(fui)

                                   val fuj = vertexAndLabel._1 
                                   val label  = vertexAndLabel._2 

                                   val index    = matchG.indexOf(fuj)

                                   val edgeType = if (index != -1) 0 else 1

                                   val uj       = if (index != -1) index else matchG.size

                                   if (uj > ui && !fGraph.isAnEdge(ui, uj, label)) {
                                      
                                          val e = new Edge(ui, uj, G.vertexLabels(fui), G.vertexLabels(fuj), label) 
                                          val newMatch = if (edgeType == 0){ matchG} else { matchG :+ fuj }

                                          (e, ( Set(G.id), 1.0)) 
                                   }     
                                   else {
                                          // a null key-value pair which will be filtered out. 
                                          (null, ( null, -0.0)) 
                                   }

                               }).filter(_._1 != null) 
                       
                    })

             }).reduceByKey((x, y) => (x._1.union(y._1), x._2 + y._2))


        // merge the guts of the candidate DGTree Nodes computed in phase 1 and phase 2
        val candidateGutsMapRDD = phaseOneCandidateGutsMapRDD.join( phaseTwoCandidateGutsMapRDD)

        candidateGutsMapRDD.mapValues( guts => {
                    val growEdge = guts._1._1
                    val edgeType = guts._1._2
                    val sStarSet = guts._1._3
                    val sSet = guts._2._1
                    val matchesSize = guts._2._2

                    // calculate score for the candidate 
                    val BIAS_SCORE = 100000
                    val exclusivelyCoveredDatagraphs = sStarSet.size 
                    val coveredDatagraphs = sSet.size 
                    val avgMatches: Double = matchesSize.toDouble / coveredDatagraphs.toDouble 
                    val score: Double  = exclusivelyCoveredDatagraphs.toDouble / avgMatches
                    val finalScore = if (edgeType == 0) score + BIAS_SCORE else score

                    (growEdge, edgeType, sStarSet, sSet, matchesSize, finalScore)
                
                })

    }
*/
    /** Score for the current node */ 
/*
    def updateScore() = {
        val BIAS_SCORE = 100000
        val exclusivelyCoveredDatagraphs = SStar.count() 
        val coveredDatagraphs = S.count() 
        val avgMatches: Double = matchesSize / coveredDatagraphs 
        val score: Double  = exclusivelyCoveredDatagraphs / avgMatches
        this.score = if (edgeType == 0) score + BIAS_SCORE else score

    } 

    override def toString = {
            val growEdge = "\n Grow Edge: " + this.growEdge.toString  + "\n"

            val fGraph = "\n FGraph \n" +  this.fGraph.toString + "\n"

            val matches = "\n Matches \n" 
            val matches_desription = this.matches.groupByKey().collect().map(t => {
                                            " M(g.id="+ t._1 + ") :" + t._2.map((s:List[Int]) => s.mkString(",")).mkString(" ")
                                     }).mkString("\n") + "\n"

            growEdge + fGraph + matches + matches_desription

    }
    */
