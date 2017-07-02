package com.dgtree;

import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import org.apache.spark.storage.StorageLevel

/** Tree-Index for fast supergraph search 
 *
 *  Nodes in each level of the tree index stored in RDDs with parent ids pointing to the parent
 *  Tree is built iteratively level by level starting from a bootstrapped layer of single node feature graphs 
 *  until all feature graphs grow to represent each datagraph as a leaf node
 *
 *  @param dataGraphsMapRDD PairRDD of datagraphs in the database with graph-id as the key
 */
class DGTree(
    dataGraphsMapRDD: RDD[(Int, Graph)]
    ) {

    // useful type definitions 
    type PhaseOneGuts     = RDD[( (Edge, String), (Edge, String, Int,  Set[Int], Graph))] 
    type PhaseTwoGuts     = RDD[( (Edge, String), (Set[Int], ArrayBuffer[(Int, List[Int])]) )] 
    type MatchesGuts      = RDD[( (Edge, String), ArrayBuffer[(Int, List[Int])] )] 
    type GraphMatches     = RDD[( Int, ( String, List[List[Int]], Graph ) )]
    type DataGraphMatches = RDD[( Int, ( (String, List[List[Int]], Graph), Graph ) )]
    type MergedGuts       = RDD[((Edge, String), ((Edge, String, Int,  Set[Int], Graph),  (Set[Int], ArrayBuffer[(Int, List[Int])] )))]

    // Bias score for closed grow-edges
    val BIAS_SCORE = dataGraphsMapRDD.count()

    val levels = new ArrayBuffer[RDD[DGTreeNode]]()
   
    /** Bootstrap the tree index building process 
     *   
     *  Builds the first level nodes which have a single nodes 
     *  as a feature-graph. These single nodes are all distinct nodes in the 
     *  graph database. 
     * 
     *  The distinct single nodes are then sieved to find a subset that covers all
     *  data-graphs in the database.
     */
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
            val growEdge = new Edge(0, 0, "", "", 0)
            val support = nodeAndGuts._2.map(_._1).toSet
            val matchesSize = nodeAndGuts._2.size
            val matches = nodeAndGuts._2.groupBy(kv => kv._1).mapValues(_.map(_._2).toList).map(identity)  

            // Add the DGTreeNode to the list of rootNodes being constructed
            val newNode = new DGTreeNode(null, fGraph, growEdge, 0, support, support, matches, 0, matchesSize)
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

    def saveDGTreetoFile(savePath: String) {
        var i = 0
        levels.foreach(nodeRDD => {
            nodeRDD.map(n => {n.matches=null
                n       
            }).saveAsObjectFile(savePath+"/"+"level"+"_"+i)
            i+=1
        }) 
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

    def makeNodesFromGuts( gutsRDD: MergedGuts): (RDD[(String, DGTreeNode)], MatchesGuts) = {

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
            //val matches = guts._2._2.groupBy(_._1).mapValues(_.map(_._2).toList).map(identity)
            val matches = guts._2._2

                    
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
            }
            if (growEdge != null)
                fGraph.addUndirectedEdge(growEdge.x, growEdge.y, growEdge.edgeLabel)

            val newNode = new DGTreeNode(parentId, fGraph, growEdge, edgeType, sSet, sStarSet, null, finalScore, matchesSize)

           // if (matches.size == 1) 
            //   println("MATCHES SIZE 1 " + newNode)

            (newNode, matches)
        })

        val matchesRDD = nodesMapRDD.map(kv => (kv._1, kv._2._2))
        // remap to make the key as parentId instead of the (growEdge, ParentID) tuple
        val nodesPerParentRDD = nodesMapRDD.map(keyAndNode => (keyAndNode._1._2, keyAndNode._2._1))
        (nodesPerParentRDD, matchesRDD)
    }

    def candidateFeatures(
            levelRDD: RDD[DGTreeNode]
            ):(RDD[(String, PriorityQueue[DGTreeNode])], MatchesGuts) = {


        val phaseOneFilteredMatches = getfilteredMatches(levelRDD, 1)
        val phaseOneIterableRDD = phaseOneFilteredMatches.join( dataGraphsMapRDD) 

        //println("Count of matches graph map " + phaseOneIterableRDD.count())

        val phaseOneGutsRDD = growPhaseOneGuts(phaseOneIterableRDD)
        //println("Next level Node Phase One guts count " + phaseOneGutsRDD.count())

        val phaseTwoIterableRDD = getfilteredMatches(levelRDD, 0).join( dataGraphsMapRDD) 
        //phaseTwoIterableRDD.persist(StorageLevel.MEMORY_AND_DISK)

        val phaseTwoGutsRDD = growPhaseTwoGuts(phaseTwoIterableRDD)
        //println("Next level Node Phase Two guts count " + phaseTwoGutsRDD.count())

        val nextLevelGutsRDD = phaseOneGutsRDD.join(phaseTwoGutsRDD)

        val nodesAndMatches = makeNodesFromGuts(nextLevelGutsRDD)
        val nexLevelNodesRDD = nodesAndMatches._1
        val matches = nodesAndMatches._2

        val nodesPQueueMapRDD = nexLevelNodesRDD.groupBy(_._1)
                                                .mapValues(stringAndNodes => {
                                                    val pq = new PriorityQueue[DGTreeNode]()
                                                    pq ++= stringAndNodes.map(_._2)
                                                    pq
                                                })

        //println("Next level Node  guts count " + nodesPQueueMapRDD.count())

        (nodesPQueueMapRDD, matches)

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
                    //print(C.size + " ")

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

                            if (!pQueue.isEmpty)
                                bestChildNode = pQueue.dequeue 
                            else {
                                bestChildNode = new DGTreeNode (parentID, parentFGraph, null, 0, C, C, null) 
                            }
                        }
                    }
                    else {
                        // Priority Queue is empty implying that the datagraph to be covered is a 
                        // subset of some other datagraph therefore we need to use a null grow edge
                        //println("NULL GROW EDGE DETECTED : Size of C is " + C.size) 

                        bestChildNode = new DGTreeNode (parentID, parentFGraph, null, 0, C, C, null) 
                    
                    }



                    if (bestChildNode.SStar.size == 1) 
                        bestChildNode.S = bestChildNode.SStar

                    // add the chosen childnode to the final list of children
                    sievedChildren += bestChildNode
                    C = C.diff(bestChildNode.SStar)
                }

                sievedChildren
        }).persist(StorageLevel.MEMORY_AND_DISK)
    
    }

    def lazyGrowMatches(graphMatchesRDD: DataGraphMatches): MatchesGuts = {

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
                                       ((e, parentId), nM) 
                                   }     
                                   else {
                                          ((null, "dummy"), null) 
                                   }

                       }) 
                       
                    })

             })

        }).filter(_._1._1 != null)
          .reduceByKey((x, y) => x ++= y)
    }

    def treeGrow() = {

        bootstrap()

        var lastLevelRDD = levels(levels.size -1).filter( node => node.SStar.size > 1 && node.growEdge != null)


        // if all nodes in the last level are leaf nodes then stop
        while ( ! lastLevelRDD.isEmpty ) {

            print("Generating Level :" + levels.size)
            println("leaf nodes :" + (levels(levels.size-1).count - lastLevelRDD.count))
            print(" non leaf nodes " + lastLevelRDD.count)

            val candidatesAndPhaseTwoInput = candidateFeatures(lastLevelRDD)
            val candidateMapRDD  = candidatesAndPhaseTwoInput._1 
            val matchesRDD = candidatesAndPhaseTwoInput._2
            matchesRDD.persist(StorageLevel.MEMORY_AND_DISK)

            val parentSStarMapRDD = lastLevelRDD.map(node => (node.nUUID, (node.SStar, node.fGraph)))

            val candidateChildrenRDD = candidateMapRDD.join(parentSStarMapRDD)


            val sievedChildren = sieveChildren( candidateChildrenRDD )

            val sievedMapChildren = sievedChildren.map(node => ((node.growEdge, node.parentUID), node)) 

            val sievedAndMatchesMapRDD = sievedMapChildren.leftOuterJoin(matchesRDD)
            val matchesMergedChildrenRDD = sievedAndMatchesMapRDD.mapValues(nodeAndMatches => {

                    val node     = nodeAndMatches._1
                    val matches  = nodeAndMatches._2.getOrElse(new ArrayBuffer[(Int, List[Int])])

                    if (node.growEdge != null && node.S.size > 1) {
                        val restructuredMatches = matches.groupBy(_._1).mapValues(_.map(_._2).toList).map(identity)
                        node.matches = restructuredMatches
                    }

                    node 
                    
            }).values 

            matchesMergedChildrenRDD.persist(StorageLevel.MEMORY_AND_DISK)


            levels += matchesMergedChildrenRDD 

            println(" sieved children count " + matchesMergedChildrenRDD.count)
            
            lastLevelRDD = levels(levels.size -1).filter(node => node.SStar.size > 1 && node.growEdge != null)

       }
       

    }

}


