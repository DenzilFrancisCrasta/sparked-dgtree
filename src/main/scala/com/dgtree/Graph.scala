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
        adjacencyList.map( _.map( e => e._1.toString + "[" + e._2.toString +"]").mkString(" ")).mkString("\n") 
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
