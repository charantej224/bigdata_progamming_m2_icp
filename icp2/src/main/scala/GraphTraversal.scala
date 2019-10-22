import org.apache.spark.{SparkConf, SparkContext}

object GraphTraversal {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph Traversals").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2, 3, 5, 6, 7), 2 -> List(1, 3, 4, 6, 7), 3 -> List(1, 2), 4 -> List(2, 5, 7), 5 -> List(1, 6, 7), 6 -> List(1, 2, 5, 7), 7 -> List(1, 2, 4, 5, 6))


    def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {

      def breadthFirst(elems: List[Vertex], visited: List[List[Vertex]]): List[List[Vertex]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          breadthFirst(newNeighbors, newNeighbors :: visited)
      }
      breadthFirst(List(start), List(List(start))).reverse
    }

    def DFS(start: Vertex, g: Graph): List[Vertex] = {

      def depthFirst(v: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours: List[Vertex] = g(v).filterNot(visited.contains)
          neighbours.foldLeft(v :: visited)((b, a) => depthFirst(a, b))
        }
      }

      depthFirst(start, List()).reverse
    }

    val bfsresult = BFS(1, g)
    val dfsresult = DFS(1, g)
    println("breadth first")
    bfsresult.flatten.foreach(print(_, "\t"))
    println("\ndepth first")
    dfsresult.foreach(print(_, "\t"))

  }


}
