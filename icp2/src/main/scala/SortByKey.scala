import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BreadthFirstSearch").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 7, 9, 6, 25, 12, 10).sortWith((e1, e2) => e1 < e2))
    rdd.collect().foreach(println)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 7, 9, 6, 25, 12, 10).sorted)
    rdd1.collect().foreach(println)
    val rdd2 = sc.parallelize(List(1, 2, 3, 4, 5, 7, 9, 6, 25, 12, 10).sortBy(x => x))
    rdd2.collect().foreach(println)
  }
}
