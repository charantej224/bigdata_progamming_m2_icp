import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("breadthfirstsearch").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1, 2, 3, 4, 5))

    val intList = List(1, 2, 3, 4, 5)

    def repeat(a: Int, list: List[Int]): List[Int] = {
      intList.foldLeft(a :: list)((b, a) => repeat(a, b))
    }

    repeat(1, intList)
  }
}
