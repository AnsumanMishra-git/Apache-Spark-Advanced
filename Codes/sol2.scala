import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object w12_p3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConfig = new SparkConf()
    .set("spark.app.name","my App")
    .set("spark.master","local[2]")
    
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  import spark.implicits._

  case class matchStats(MatchNumber: Int , Batsman: String , Team: String , RunsScored: Int , StrikeRate: Double)
  case class Batsman2019(Batsman :String , Team: String )
  
  val fileA_RDD = spark.sparkContext.textFile("C:/Users/hp/Documents/File A.txt")
  val fileB_RDD =  spark.sparkContext.textFile("C:/Users/hp/Documents/File B.txt")
  
  val fileA = fileA_RDD.map(x=>x.split(" ")).map(fields=> matchStats(fields(0).toInt,fields(1),fields(2),fields(3).toInt,fields(4).toDouble))
  val fileB = fileB_RDD.map(x=>x.split(" ")).map(fields=> Batsman2019(fields(0),fields(1)))
  
  val fileA_DF = fileA.toDF()
  val fileB_DF = fileB.toDF()
  
  val fileA_new = fileA_DF.groupBy("Batsman").agg(avg("RunsScored").as("Average")).select("Batsman","Average").sort(desc("Average"))
  val joined = fileA_new.join(broadcast(fileB_DF),fileA_new.col("Batsman") === fileB_DF.col("Batsman"),"inner").drop(fileB_DF.col("Batsman"))
  
  joined.show()
  spark.stop()
}
