package com.dgtree;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import collection.mutable._
import org.apache.hadoop.io.{Text,LongWritable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import scala.io.StdIn.readInt

object DGTreeApp {

    val APPNAME = "Distributed DGTree Application"


    def DGTreeConstruct(dataGraphsRDD: RDD[Graph]): DGTreeNode = {
        /** FIX-ME factor me out into a seperate Factory Method */

        val dataGraphsMapRDD = dataGraphsRDD.map(g => (g.id, g)).persist()

        // for each distinct node label in the datagraphs, store the datagraphs that contain it 
        val nodeGraphMapRDD = dataGraphsRDD.flatMap(g => g.vertexLabels.distinct.map(s =>(s , g.id))).groupByKey().persist()

        // unique node labels across the dataset
        val uniqueNodeRDD = nodeGraphMapRDD.keys
       

        // Generate the cursory root nodes. One each for distinct nodeLabel in the datagraphs 
        val rootNodes = uniqueNodeRDD.collect().map(node => {
                     val nodeLabel = node

                     // RDD of the graph id's which contain nodeLabel traditionally called support
                     val support = nodeGraphMapRDD.filter(t => t._1 == node).values.flatMap((t:collection.Iterable[Int])=> t)

                     // Feature Graph has one vertex whose label is provided by nodeLabel
                     val fGraph = new Graph(0, 1, 0, Array(nodeLabel))
                    
                     val matches = support.map((_, nodeLabel))
                                          .join(dataGraphsMapRDD)
                                          .flatMap(nodeAndGraph => {
                                       
                                       // local variables for clarity 
                                       val label = nodeAndGraph._2._1 
                                       val graph = nodeAndGraph._2._2
                                       val vertexLabels = graph.vertexLabels

                                       // find all occurences/matches of label in vertexLabels
                                       // and wrap each occurence index in a List for 
                                       // subsequent operations to grow the match
                                       val listOfMatches = for (i <- 0 until vertexLabels.size if vertexLabels(i) == label) yield List(i)
                                       
                                       // return graph-id as key and the matches as the value
                                       listOfMatches.map((m:List[Int]) => (nodeAndGraph._1, m))
                                   })

                     matches.persist()
                     support.persist()

                     // Add the DGTreeNode to the list of rootNodes being constructed
                     new DGTreeNode(fGraph, null, 0, support, support, matches)
                })


        /* 
        println("Unique Node Labels Count " + uniqueNodeRDD.count())
        uniqueNodeRDD.collect().foreach(( s:String) => print(s +" "))
        println("")

        println("Containment Map")
        nodeGraphMapRDD.collect().foreach(x => {
                print("[g.id=" + x._1 + "]: ")
                println(x._2.mkString(","))
                println("")
                })
                */

        val children = new ArrayBuffer[DGTreeNode]()
        rootNodes.copyToBuffer(children)
        //rootNodes.foreach((node:DGTreeNode) =>  node.treeGrow(dataGraphsMapRDD))
        rootNodes(2).treeGrow(dataGraphsMapRDD)

        // create a dummy-root with the rootNodes as its children for single point entry
        new DGTreeNode(new Graph(-1, 0, 0, Array()), null, 0, null, null, null, children)
    } 

    def main(args : Array[String]) {  

        // Initialize spark context
        val conf = new SparkConf().setAppName(APPNAME)
        conf.set("spark.scheduler.mode", "FAIR")
        val sc   = new SparkContext(conf)

        Logger.getRootLogger().setLevel(Level.ERROR)

        // Generate RDD of string representations of data-graphs 
        val dataFile   = args(0)
        /*
        val totalLines = sc.wholeTextFiles(dataFile)
        val graphData  = totalLines.flatMap(x => x._2.split("#"))
        */

        val textFormatConf = new Configuration()
        textFormatConf.set("textinputformat.record.delimiter","#")
        val graphData = sc.newAPIHadoopFile(dataFile, 
                                            classOf[TextInputFormat], 
                                            classOf[LongWritable], 
                                            classOf[Text], 
                                            textFormatConf).map(_._2.toString)
                          

        // prune out any null invalid graphs from the datagraphsRDD 
        val dataGraphsRDD = graphData.map(Graph.makeGraph).filter(_.id != -1)
        val dataGraphsMapRDD = dataGraphsRDD.map(g => (g.id, g)).persist()
        println(dataGraphsMapRDD.count())

        //dataGraphsRDD.collect().foreach( g => g.render(g.id.toString, "images"))
       
        //val dgtree = DGTreeConstruct(dataGraphsRDD) 

        //println("DGTree Root Children Count : " + dgtree.children.size)
        /*
        dgtree.children.foreach(gr => {
                   gr.fGraph.printVertices() 
                   print("Support :") 
                   gr.S.collect().foreach( (s:Int) => print(s +" "))
                   println("")

                   println("Matches")
                   gr.matches.groupByKey().collect().foreach(t => {
                             print("M(g.id="+ t._1 + ") :")
                             t._2.foreach((s:List[Int]) => print(s.mkString(",") + " "))
                             println("") 
                           })
                   println("")
                   println("")
                } )

        println("Raw Graph String")
        println(graphData.take(2)(1))

        println("Total Datagraphs Parsed " + dataGraphsRDD.count())
        println("A sample Datagraph")
        println(dataGraphsRDD.take(2)(1))
        */
        
        /*

        val queryFile   = args(1)
        val queryGraphStrings = sc.newAPIHadoopFile(queryFile,
                                            classOf[TextInputFormat], 
                                            classOf[LongWritable], 
                                            classOf[Text], 
                                            textFormatConf).map(_._2.toString)
        val queryGraphs = queryGraphStrings.map(DGTreeApp.makeGraph).filter(_.id != -1)

        val queryTree = new QueryTree(dgtree, dataGraphsRDD)
        */
        //val results = queryGraphs.collect().par.map( Q => queryTree.superGraphSearch(Q))


        //val i = readInt()
        

        sc.stop()
    }
}
