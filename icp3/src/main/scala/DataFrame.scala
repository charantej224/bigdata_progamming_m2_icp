import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFrame {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("ICP3 - DataFrame & SQL") // optional and will be autogenerated if not specified
      .master("local[*]")
      .getOrCreate()
    val sqlContext = sparkSession.sqlContext
    val dataframe = sqlContext.read.option("header", "true").option("inferSchema", "true").csv("survey.csv")
    dataframe.show(15)
    dataframe.createOrReplaceTempView("survey")
    dataframe.write.mode("overwrite").format("csv").save("saved_file")
    println(dataframe.count())
    dataframe.dropDuplicates()
    println(dataframe.count())

    val df1 = dataframe.limit(10)
    val df2 = dataframe.limit(10)

    df1.join(df2, df1.col("Country").equalTo(df2.col("Country")), "inner").show()
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    val unionDf = df1.union(df2)
    unionDf.orderBy("Country").show()

    dataframe.groupBy(col("treatment"), col("Country")).avg("Age").show()

    val joinedDataFrame = df1.join(df2, df1.col("Country").equalTo(df2.col("Country")), "inner")
    joinedDataFrame.show()

    sparkSession.sql("select df1.Age,df1.Country from df1 inner join df2 on df1.Country == df2.Country and df1.Gender == df2.Gender").show()

    dataframe.groupBy("Country").mean("Age").show()
    sparkSession.sql("SELECT max(`Age`) FROM survey").show()
    sparkSession.sql("SELECT min(`Age`) FROM survey").show()
    sparkSession.sql("SELECT avg(`Age`) FROM survey").show()
    sparkSession.sql("SELECT avg(`Age`),`Country` FROM survey group by `Age`,`Country`").show()
    println(dataframe.take(13).last)
  }
}
