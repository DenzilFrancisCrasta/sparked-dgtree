package com.dgtree;

/** Representation for the grow-edge 
 *  @param x index of the first vertex
 *  @param y index of the second vertex
 *  @param xLabel label of the first vertex
 *  @param yLabel label of the second vertex
 *  @param edgeLabel label of the second vertex
 */
class Edge  ( 
    val x: Int,
    val y: Int,
    val xLabel : String,
    val yLabel : String,
    val edgeLabel: Int ) extends java.io.Serializable {

    override def toString = xLabel +"(" +x+") --["+ edgeLabel+"]-- "+ yLabel +"("+y+")" 

    override def equals(e: Any): Boolean = {
      e match {
          case e: Edge => if (this.x == e.x 
                              && this.y == e.y 
                              && this.xLabel == e.xLabel 
                              && this.yLabel == e.yLabel 
                              && this.edgeLabel == e.edgeLabel) 
                              true 
                          else 
                              false
          case _ => false
      }
    }

    override def hashCode = (xLabel+"#"+yLabel+"#"+edgeLabel.toString).hashCode 
}
