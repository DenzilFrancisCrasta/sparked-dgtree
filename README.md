# Distributed-DGTree
DGTree : A tree index to facilitate scalable supergraph search queries on large graph databases. Given a query graph and a  large database of graphs, supergraph search involves find the list of datagraphs contained in the query graph.

We provide a distributed implemntation of DGTree (DOI: 10.1109/ICDE.2016.7498237) in Apache Spark. 
Distributed tree construction is achieved using a level-order RDD representation of the tree.
