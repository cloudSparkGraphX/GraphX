import com.mongodb.spark.MongoSpark

/**
  * Created by Shifang on 2017/10/22.
  */
object Main {
  def main(args: Array[String]): Unit = {

    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/myNewDB.myNewCollection1")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/myNewDB.myNewCollection1")
      .getOrCreate()

    val rdd = MongoSpark.load(spark)
    println("####################     count:"+rdd.count)
    println("####################     first:"+rdd.first)

  }

}
