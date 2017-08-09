package com.dgtree;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
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
    var sc : SparkContext = null
    


    def main(args : Array[String]) {  

        // Initialize spark context
        val conf = new SparkConf().setAppName(APPNAME)
        conf.set("spark.scheduler.mode", "FAIR")
        conf.registerKryoClasses(Array(classOf[DGTreeNode], classOf[Edge], classOf[Graph]))
        sc   = new SparkContext(conf)

        Logger.getRootLogger().setLevel(Level.ERROR)

        val dataFile   = args(0)
        val savePath   = args(1)
        val queryGraphFile = args(2)

        val renderEngine = if (args.size > 3) args(3) else "dot" 


        // Generate RDD of string representations of data-graphs 
        val textFormatConf = new Configuration()
        textFormatConf.set("textinputformat.record.delimiter", GRAPH_DELIMITTER)

        val graphStringsRDD = sc.newAPIHadoopFile(dataFile, 
                                            classOf[TextInputFormat], 
                                            classOf[LongWritable], 
                                            classOf[Text], 
                                            textFormatConf).map(_._2.toString)

        val queryGraphStringRDD = sc.newAPIHadoopFile(queryGraphFile, 
                                            classOf[TextInputFormat], 
                                            classOf[LongWritable], 
                                            classOf[Text], 
                                            textFormatConf).map(_._2.toString)
                          

        // prune out any null invalid graphs from the datagraphsRDD 
        val dataGraphsMapRDD = graphStringsRDD.map(Graph.makeGraph)
                                              .filter(_.id != INVALID_GRAPH_ID)
                                              .keyBy(g => g.id)
                                              .persist(StorageLevel.MEMORY_AND_DISK)

        val queryGraphsMapRDD = queryGraphStringRDD.map(Graph.makeGraph)
                                              .filter(_.id != INVALID_GRAPH_ID)
                                              .map(g => (g.id, g))
                                              .persist(StorageLevel.MEMORY_AND_DISK)


        println("Graph Database Size " + dataGraphsMapRDD.count())


        // Build the DGTree Index 
        val dgTree = new DGTree(dataGraphsMapRDD)
        dgTree.treeGrow()

        //dgTree.saveDGTreetoFile(savePath)

        dgTree.renderTree(renderEngine)
        queryGraphsMapRDD.values.zipWithIndex.foreach(gi => gi._1.render("Query_"+gi._2, "images/", renderEngine))

        // Initialize a query processor to process supergraph search queries
        val processor = new QueryProcessor(dgTree.levels, dataGraphsMapRDD)
        processor.superGraphSearch(queryGraphsMapRDD)

        // loading and verification of save data
        // val levelCount = dgTree.levels.size
        //val levels = dgTree.loadDGTreeFromFile(sc, savePath,levelCount)
        sc.stop()
    }
}
