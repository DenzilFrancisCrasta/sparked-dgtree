package com.dgtree;

import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.collection.mutable.ArrayBuffer 
import org.apache.spark.storage.StorageLevel

class DGTree(
    dataGraphsMapRDD: RDD[(Int, Graph)]
    ) {

    type PhaseOneGuts = RDD[((Edge, String), (Edge, String, Int,  Set[Int], Graph))] 
    type PhaseTwoGuts = RDD[((Edge, String), (Set[Int], ArrayBuffer[(Int, List[Int])]))] 
    type GraphMatches = RDD[(Int, (String, List[List[Int]], Graph))]
    type DataGraphMatches = RDD[(Int, ((String, List[List[Int]], Graph), Graph))]
    type MergedGuts = RDD[((Edge, String), ((Edge, String, Int,  Set[Int], Graph),  (Set[Int], ArrayBuffer[(Int, List[Int])])))]

    val BIAS_SCORE = dataGraphsMapRDD.count()

    val levels = new ArrayBuffer[RDD[DGTreeNode]]()
   
    /** Bootstrap the tree index building process */
    def bootstrap() = {
        
        val BIAS_SCORE = this.BIAS_SCORE

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
            val matchesSize = nodeAndGuts._2.size
            val matches = nodeAndGuts._2.groupBy(kv => kv._1).mapValues(_.map(_._2).toList).map(identity)  

            // Add the DGTreeNode to the list of rootNodes being constructed
            val newNode = new DGTreeNode(null, fGraph, null, 0, support, support, matches, 0, matchesSize)
            newNode.calcScore(BIAS_SCORE)
            //println(newNode.nUUID)
            ("dummy", newNode)

        }).groupByKey()
          .mapValues( nodes => {
                 val pq = new PriorityQueue[DGTreeNode]()
                 pq ++= nodes
                 pq
          })

        val toCover = dataGraphsMapRDD.keys.map(("dummy", _))
                                      .groupByKey()
                                      .mapValues(ids => (ids.toSet, new Graph(0, 1, 0, Array("dummy")))) 

        val sievedFirstLevel = sieveChildren(firstLevelNodesRDD.join(toCover))
        sievedFirstLevel.persist(StorageLevel.MEMORY_AND_DISK)

        println("First level count " + sievedFirstLevel.count)
        levels.append(sievedFirstLevel)

    } 

    def getfilteredMatches(levelRDD: RDD[DGTreeNode],
                           filterBy: Int
                          ): GraphMatches = {

        levelRDD.flatMap( node => {
            // determine to filter by S set or SStar set 
            val filterMaster = if (filterBy == 0) node.S else node.SStar
            node.matches.filter(aMatch => filterMaster.contains(aMatch._1))
                        .map( graphIdAndMatches => (graphIdAndMatches._1, (node.nUUID,  graphIdAndMatches._2, node.fGraph))) 
        })
    
    } 


    def growPhaseOneGuts( graphMatchesRDD: DataGraphMatches): PhaseOneGuts = {

        val BIAS_SCORE = this.BIAS_SCORE

        graphMatchesRDD.flatMap(graphAndMatches => {

           val parentId = graphAndMatches._2._1._1
           val matches  = graphAndMatches._2._1._2
           val fGraph   = graphAndMatches._2._1._3
           val G        = graphAndMatches._2._2

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
                                       //  val newMatch = if (edgeType == 0){ matchG} else { matchG :+ fuj }
                                       //  ((e, parentId), (e, parentId, edgeType,  Set(G.id), List((G.id, newMatch)))) 
                                       ((e, parentId), (e, parentId, edgeType,  Set(G.id), fGraph)) 
                                   }     
                                   else {
                                          ((null, "dummy"), (null, "dummy", 0, null, null)) 
                                   }

                       }) 
                       
                    })

             })


        }).filter(_._1._1 != null)
          .reduceByKey((x, y) => (x._1, x._2, x._3, x._4.union(y._4), x._5))
    
    }

    def growPhaseTwoGuts( graphMatchesRDD: DataGraphMatches): PhaseTwoGuts = {

        val BIAS_SCORE = this.BIAS_SCORE

        graphMatchesRDD.flatMap(graphAndMatches => {

           val parentId = graphAndMatches._2._1._1
           val matches  = graphAndMatches._2._1._2
           val fGraph   = graphAndMatches._2._1._3
           val G        = graphAndMatches._2._2

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
                                       val nM = new ArrayBuffer[(Int, List[Int])](1)
                                       nM.append((G.id, newMatch))
                                       ((e, parentId), (Set(G.id), nM)) 
                                   }     
                                   else {
                                          ((null, "dummy"), (null, null)) 
                                   }

                       }) 
                       
                    })

             })

        }).filter(_._1._1 != null)
          .reduceByKey((x, y) => (x._1.union(y._1), x._2 ++= y._2))
    
    }

    def makeNodesFromGuts( gutsRDD: MergedGuts): RDD[(String, DGTreeNode)] = {

        val BIAS_SCORE = this.BIAS_SCORE
    
        val nodesMapRDD = gutsRDD.mapValues( guts => {
            val growEdge = guts._1._1
            val parentId = guts._1._2
            val edgeType = guts._1._3
            val sStarSet = guts._1._4
            val parentfGraph = guts._1._5
            val sSet = guts._2._1
            val matchesSize = guts._2._2.size

            //  https://issues.scala-lang.org/browse/SI-7005 Map#mapValues is not serializable 
            //  therefore we need to use map(identity)
            val matches = guts._2._2.groupBy(_._1).mapValues(_.map(_._2).toList).map(identity)
                    
            // calculate score for the candidate 
            val exclusivelyCoveredDatagraphs = sStarSet.size 
            val coveredDatagraphs = sSet.size 
            val avgMatches: Double = matchesSize.toDouble / coveredDatagraphs.toDouble 
            val score: Double  = exclusivelyCoveredDatagraphs.toDouble / avgMatches
            val finalScore = if (edgeType == 0) score + BIAS_SCORE else score

            val extraVertex = if (edgeType == 0) false else true

            // create the clone of the parents fGraph with an extraVertex added based on the predicate 
            val fGraph = parentfGraph.getSuccessor(extraVertex)

            if (extraVertex) {
                val index = fGraph.vertexCount-1
                fGraph.vertexLabels(index) = growEdge.yLabel 
                fGraph.adjacencyList(index) = Nil 
                fGraph.addUndirectedEdge(growEdge.x, growEdge.y, growEdge.edgeLabel)
            }

            new DGTreeNode(parentId, fGraph, growEdge, edgeType, sSet, sStarSet, matches, finalScore, matchesSize)
        })

        // remap to make the key as parentId instead of the (growEdge, ParentID) tuple
        val nodesPerParentRDD = nodesMapRDD.map(keyAndNode => (keyAndNode._1._2, keyAndNode._2))
        nodesPerParentRDD
    }

    def candidateFeatures(levelRDD: RDD[DGTreeNode]): RDD[(String, PriorityQueue[DGTreeNode])] = {


        val phaseOneFilteredMatches = getfilteredMatches(levelRDD, 1)
        val phaseOneIterableRDD = phaseOneFilteredMatches.join( dataGraphsMapRDD) 

        //println("Count of matches graph map " + phaseOneIterableRDD.count())

        val phaseOneGutsRDD = growPhaseOneGuts(phaseOneIterableRDD)
        //println("Next level Node Phase One guts count " + phaseOneGutsRDD.count())

        val phaseTwoIterableRDD = getfilteredMatches(levelRDD, 0).join( dataGraphsMapRDD) 
        val phaseTwoGutsRDD = growPhaseTwoGuts(phaseTwoIterableRDD)
        //println("Next level Node Phase Two guts count " + phaseTwoGutsRDD.count())

        val nextLevelGutsRDD = phaseOneGutsRDD.join(phaseTwoGutsRDD)

        val nexLevelNodesRDD = makeNodesFromGuts(nextLevelGutsRDD)

        val nodesPQueueMapRDD = nexLevelNodesRDD.groupBy(_._1)
                                                .mapValues(stringAndNodes => {
                                                    val pq = new PriorityQueue[DGTreeNode]()
                                                    pq ++= stringAndNodes.map(_._2)
                                                    pq
                                                })

        //println("Next level Node  guts count " + nodesPQueueMapRDD.count())

        nodesPQueueMapRDD

    }

    def sieveChildren(nodePQueueRDD: RDD[(String, (PriorityQueue[DGTreeNode], (Set[Int], Graph)))]): RDD[DGTreeNode] = {

        val BIAS_SCORE = this.BIAS_SCORE

        nodePQueueRDD.flatMap(kv => {
                val parentID = kv._1
                val pQueueAndSStar = kv._2
                val pQueue = pQueueAndSStar._1
                var C = pQueueAndSStar._2._1
                var parentFGraph = pQueueAndSStar._2._2

                //println("to be covered graph size " + C.size)
                val sievedChildren = new ArrayBuffer[DGTreeNode]()

                while (!C.isEmpty) {
                    //println(C.size)

                    var bestChildNode:DGTreeNode = null

                    if (!pQueue.isEmpty) {

                        bestChildNode = pQueue.dequeue

                        while (! bestChildNode.SStar.subsetOf(C)) {

                            // Lazy update the SStar to be consistent with uncovered Datagraphs in C
                            bestChildNode.SStar = bestChildNode.SStar.intersect(C)

                            if (!bestChildNode.SStar.isEmpty) {
                                // update the score of the node and add it back to the Priority Queue
                                bestChildNode.calcScore(BIAS_SCORE)
                                pQueue += bestChildNode
                            }

                            bestChildNode = pQueue.dequeue 
                        }
                    }
                    else {
                        // Priority Queue is empty implying that the datagraph to be covered is a 
                        // subset of some other datagraph therefore we need to use a null grow edge
                        println("NULL GROW EDGE DETECTED : Size of C is " + C.size) 

                        bestChildNode = new DGTreeNode (parentID, parentFGraph, null, 0, C, C, null) 
                    
                    }

                    // add the chosen childnode to the final list of children
                    sievedChildren += bestChildNode
                    C = C.diff(bestChildNode.SStar)
                }

                sievedChildren
        }).persist(StorageLevel.MEMORY_AND_DISK)
    
    }

    def growNextLevel() = {

        println("Number of Levels Generated :" + levels.size)

        val lastLevelRDD = levels(levels.size -1)


        val candidateMapRDD = candidateFeatures(lastLevelRDD)

        println("candidate keys")
        //candidateMapRDD.keys.collect().foreach(println)

        /* create a MapRDD with key as parent node id and value as a tuple of 
         * its candidate children in a priority queue and the SStar of the parent 
         */

        val parentSStarMapRDD = lastLevelRDD.map(node => (node.nUUID, (node.SStar, node.fGraph)))
        //println("parent SStar Map Count "+ parentSStarMapRDD.count)
        //println("sstart join  keys")
        //parentSStarMapRDD.keys.collect().foreach(println)

        val candidateChildrenRDD = candidateMapRDD.join(parentSStarMapRDD)


        //println("After joining with SStar" + candidateChildrenRDD.count)
        levels += sieveChildren( candidateChildrenRDD )

        println("Added sieved children level count" + levels(levels.size-1).count)

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
    var score: Double = 0.0,
    var matchesSize: Double = 0.0,
    var children: ArrayBuffer[DGTreeNode] = ArrayBuffer[DGTreeNode]()
) extends java.io.Serializable with Ordered[DGTreeNode]{ 

    val nUUID = java.util.UUID.randomUUID.toString

    def compare(that: DGTreeNode): Int = this.score.compareTo(that.score)

    def calcScore(BIAS_SCORE: Double) = {
        val exclusivelyCoveredDatagraphs = SStar.size.toDouble
        val coveredDatagraphs = S.size.toDouble 
        val avgMatches  = matchesSize / coveredDatagraphs 
        val score  = exclusivelyCoveredDatagraphs / avgMatches
        this.score = if (edgeType == 0) score + BIAS_SCORE else score
    } 
}


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
