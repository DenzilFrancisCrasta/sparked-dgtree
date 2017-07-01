package com.dgtree;

import collection.mutable.Map
/** A data-graph in the input graph database 
 * 
 *  @param id unique id of the data-graph
 *  @param vertexCount number of vertices in the graph
 *  @param edgeCount number of edges in the graph
 */
class Graph(val id: Int, 
            val vertexCount: Int, 
            val edgeCount: Int, 
            val vertexLabels: Array[String]) extends java.io.Serializable {
    
    var adjacencyList =  Array.fill(vertexCount){List[(Int, Int)]()}

    /** Adds an edge between vertices with index a and b
     *
     *  @param a index of first vertex
     *  @param b index of second vertex 
     *  @param valence the valency or label on the edge
     */
    def addUndirectedEdge(a: Int, b: Int, edgeLabel: Int) = {
        adjacencyList(a) :+= (b, edgeLabel) 
        adjacencyList(b) :+= (a, edgeLabel) 
    }

    def getSuccessor(extraVertex: Boolean): Graph = {
        val vertexCount = if(extraVertex) this.vertexCount + 1 else this.vertexCount 
        val edgeCount = this.edgeCount + 1

        // copy the vertex labels 
        val vertexLabels = new Array[String](vertexCount)
        Array.copy(this.vertexLabels, 0, vertexLabels, 0, this.vertexLabels.size)

        val successor = new Graph(-1, vertexCount, edgeCount, vertexLabels)

        //successor.adjacencyList = new Array[List[(Int, Int)]](vertexCount)
        Array.copy(this.adjacencyList, 0, successor.adjacencyList, 0, this.adjacencyList.size)

        return successor
    
    }

    /** list of neighbours of vertex 
     * 
     *  @param vertex index of vertex whose neighbour list is desired
     */
    def getNeighbours(vertex: Int) : List[(Int, Int)] = adjacencyList(vertex) 

    def isAnEdge(a: Int, b: Int, edgeLabel: Int): Boolean = {
        if (a >= adjacencyList.size) false else adjacencyList(a).contains((b, edgeLabel))
    }

    def printVertices() =  vertexLabels.foreach{ println}

    /** creates a string representation of the adjacency matrix */
    override def toString =  { 
        val adjacencyListString = adjacencyList.map( _.map( e => e._1.toString + "[" + e._2.toString +"]").mkString(" ")).mkString("\n") 
        val vertexLabelString = vertexLabels.mkString(",")
        vertexLabelString + "\n" + adjacencyListString
    }

    /** renders the graph using graphviz library */
    def render(filename: String, directory: String)  = { 
        val gv = new com.liangdp.graphviz4s.Graph()
        var k = 0 
        for ( v <- vertexLabels) {
            gv.node(k.toString(),label=v,attrs=Map("shape"->"plaintext"))
                k = k+1 
        }   

        for ( i <- 0 to adjacencyList.length-1) {
            for ( e <- adjacencyList(i) )  {
                if( e._1 > i) {
                    val col = new StringBuilder("\"black")
                        for (_ <- 2 to e._2 ) { 
                            col.append(":white:black")
                        }
                    col.append("\"")
                        gv.edge(i.toString(),e._1.toString(),attrs=Map("color"->col.toString()))
                }
            }
        }   
        println(gv.source())
        gv.render(engine="neato", format="png", fileName=filename, directory = directory)
    }   

}

object Graph {

    /** A Factory Method to instantiate a Graph from its String Representation */
    def makeGraph(s: String) = {
        if (s.length > 0) {
            // extract graph properties from its string representation
            val sList = s.split("\n")
            val id           = sList(0).toInt
            val vertexCount  = sList(1).toInt
            val vertexLabels = sList.slice(2, 2+vertexCount)
            val edgeCount    = sList(2+vertexCount).toInt 

            // initialize a new data-graph with the properties extracted above
            val g = new Graph(id, vertexCount, edgeCount, vertexLabels)

            // parse the adjacency list from its string representation  
            val adjacencyList = sList.slice(3+vertexCount, sList.length)
            val adjacencyMap = adjacencyList.map(_.split(" ").map(_.toInt))

            adjacencyMap.foreach { e:Array[Int] => g.addUndirectedEdge(e(0), e(1), e(2)) } 

            g // return the newly constructed graph
        }
        else {
            // return an invalid graph if the string representation is empty
            new Graph(-1, 0, 0, Array[String]())
        }

    }

}
