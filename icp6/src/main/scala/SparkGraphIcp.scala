import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphIcp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph_M2_ICP5").setMaster("local[2]")
    val sparkSession = SparkSession.builder().appName("Graph_M2_ICP6").config(conf = conf).getOrCreate()

    // Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val tripsDataFrame = sparkSession.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("Datasets/201508_trip_data.csv")

    val stationDataFrame = sparkSession.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("Datasets/201508_station_data.csv")
    

    val vertices = stationDataFrame
      .withColumnRenamed("name", "id")
      .distinct()

    val edges = tripsDataFrame
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
      .withColumnRenamed("Trip ID", "tripid")
      .withColumnRenamed("Trip ID", "tripid")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("Start Terminal", "StartTerminal")
      .withColumnRenamed("End Terminal", "EndTerminal")
      .withColumnRenamed("Bike #", "bike")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    val directedGraph = GraphFrame(vertices, edges)
    directedGraph.cache()

    println("edges showing 5 sets of data")
    directedGraph.edges.show(5)
    println("vertices showing 5 sets of data")
    directedGraph.vertices.show(5)

    // Triangle Count
    val stationTraingleCount = directedGraph.triangleCount.run()
    stationTraingleCount.select("id", "count").show()


    // Shortest Path
    val shortPath = directedGraph.shortestPaths.landmarks(Seq("Japantown", "Santa Clara County Civic Center")).run
    shortPath.show()


    //Page Rank
    val stationPageRank = directedGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()


    //Saving to File
    directedGraph.vertices.write.csv("vertices")
    directedGraph.edges.write.csv("edges")


    //Label propogation
    val lpa = directedGraph.labelPropagation.maxIter(5).run()
    lpa.select("id", "label").show()


    // BFS
    val pathBFS = directedGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 15").run()
    pathBFS.show()
    println("BFS ")

  }

}
