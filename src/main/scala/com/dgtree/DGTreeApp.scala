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
    val GRAPH_DELIMITTER = "#"
    val INVALID_GRAPH_ID = -1

    def main(args : Array[String]) {  

        // Initialize spark context
        val conf = new SparkConf().setAppName(APPNAME)
        conf.set("spark.scheduler.mode", "FAIR")
        val sc   = new SparkContext(conf)

        Logger.getRootLogger().setLevel(Level.ERROR)

        // Generate RDD of string representations of data-graphs 
        val dataFile   = args(0)

        val textFormatConf = new Configuration()
        textFormatConf.set("textinputformat.record.delimiter", GRAPH_DELIMITTER)
        val graphStringsRDD = sc.newAPIHadoopFile(dataFile, 
                                            classOf[TextInputFormat], 
                                            classOf[LongWritable], 
                                            classOf[Text], 
                                            textFormatConf).map(_._2.toString)
                          

        // prune out any null invalid graphs from the datagraphsRDD 
        val dataGraphsMapRDD = graphStringsRDD.map(Graph.makeGraph)
                                              .filter(_.id != INVALID_GRAPH_ID)
                                              .map(g => (g.id, g))
                                              .persist()

        //println(dataGraphsMapRDD.count())

        // bootstrap the tree index 
        val dgTree = new DGTree(dataGraphsMapRDD)
        dgTree.bootstrap()

        println("First Level Root Node counts " + dgTree.levels(0).count())































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
