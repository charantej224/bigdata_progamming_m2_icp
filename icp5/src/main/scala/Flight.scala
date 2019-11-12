import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Flight {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph_M2_ICP5").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("Graph_M2_ICP5").config(conf = conf).getOrCreate()

    val vertices = Array((1L, ("SFO")), (2L, ("ORD")), (3L, ("DFW")), (4L, ("KAN")), (5L, ("VIR")))
    val edges = Array(Edge(1L, 2L, 1800), Edge(2L, 3L, 800), Edge(3L, 1L, 1400), Edge(4L, 5L, 3200))

    val verticesRdd = sparkContext.parallelize(vertices)
    val edgesRdd = sparkContext.parallelize(edges)

    verticesRdd.collect().foreach(println)
    edgesRdd.collect().foreach(println)

    val graph = Graph(verticesRdd, edgesRdd)

    graph.vertices.collect().foreach(println)
    graph.edges.collect().foreach(println)

    graph.edges.filter({ case Edge(src, dst, miles) => miles > 1000 }).collect().foreach(println)
    graph.vertices.filter({ case (id, name) => id > 1L }).collect().foreach(println)

    graph.connectedComponents().edges.foreach(println)
    graph.stronglyConnectedComponents(3).edges.foreach(println)

    graph.triplets.collect().foreach(println)
    graph.degrees.collect().foreach(println)

  }
}