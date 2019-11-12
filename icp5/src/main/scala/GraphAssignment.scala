import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{concat, lit}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.desc

object GraphAssignment {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Graph_M2_ICP5").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("Graph_M2_ICP5").config(conf = conf).getOrCreate()

    //1. Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.
    val tripsDataFrame = sparkSession.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("DataSets/201508_trip_data.csv")

    val stationDataFrame = sparkSession.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("DataSets/201508_station_data.csv")

    // Alternatives is drop duplicates using dataframe, but hanlding it from the others.
    // tripsDataFrame.dropDuplicates()

    // Printing the Schema
    tripsDataFrame.printSchema()
    stationDataFrame.printSchema()

    //2.Concatenate chunks into list & convert to DataFrame
    stationDataFrame.select(concat(stationDataFrame("lat"), lit(" "), stationDataFrame("long"))).toDF().show(5, false)

    // 3.Remove duplicates
    // 6.Create vertices
    // 7.Show some vertices
    val stationVertices = stationDataFrame
      .withColumnRenamed("name", "id")
      .distinct()

    // 4.Name Columns
    // 8. Show some edges
    val tripEdges = tripsDataFrame
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

    //5.Output DataFrame
    tripEdges.show(10)

    //Creating the graphframe
    val directedGraph = GraphFrame(stationVertices, tripEdges)
    directedGraph.cache()

    directedGraph.edges
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)


    directedGraph.edges
      .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .show(10)



    // 9.Vertex in-Degree
    val inDeg = directedGraph.inDegrees
    inDeg.orderBy(desc("inDegree")).show(5, false)
    // 10.Vertex out-Degree
    val outDeg = directedGraph.outDegrees
    outDeg.orderBy(desc("outDegree")).show(5, false)

    // 11.Apply the motif findings.
    val motifs = directedGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)").show(5, false)

  }
}
