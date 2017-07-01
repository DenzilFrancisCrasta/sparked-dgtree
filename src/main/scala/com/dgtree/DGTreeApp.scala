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
    

    def loadDGTreeFromFile(savePath : String,levelCount : Int) : ArrayBuffer[RDD[DGTreeNode]]  = {
        val levels = new ArrayBuffer[RDD[DGTreeNode]]()
        for(i <- 0 to levelCount-1) {
            levels += sc.objectFile[DGTreeNode](savePath+"/level_"+i)
        }
        var i = 0
        levels.foreach(nodeRDD =>  {
              println("level_"+i+":  "+nodeRDD.count()+" nodes")
              println("First Node : "+nodeRDD.take(1)(0).fGraph)
              i+=1
        })
        levels
    }

    def main(args : Array[String]) {  

        // Initialize spark context
        val conf = new SparkConf().setAppName(APPNAME)
        conf.set("spark.scheduler.mode", "FAIR")
        //conf.set("spark.driver.memory", "6g")
        //conf.set("spark.executor.memory", "6g")
        conf.registerKryoClasses(Array(classOf[DGTreeNode], classOf[Edge], classOf[Graph]))
        sc   = new SparkContext(conf)

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
                                              .persist(StorageLevel.MEMORY_AND_DISK)

        //println(dataGraphsMapRDD.count())

        // bootstrap the tree index 
        val dgTree = new DGTree(dataGraphsMapRDD)
        dgTree.bootstrap()
        dgTree.treeGrow()
        val savePath = args(1)
        dgTree.saveDGTreetoFile(savePath)
        //loading and verification of save data
        val levelCount: Int = dgTree.levels.size
        val levels : ArrayBuffer[RDD[DGTreeNode]] = loadDGTreeFromFile(savePath,levelCount)
        sc.stop()
    }
}
