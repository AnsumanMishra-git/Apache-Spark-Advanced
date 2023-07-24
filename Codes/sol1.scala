import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame , Dataset}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object w12_p1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConfig = new SparkConf()
    .set("spark.app.name","my App")
    .set("spark.master","local[2]")
    
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  import spark.implicits._
  
  val deptDF = spark.read
  .option("inferSchema",true)
  .format("json")
  .option("path","C:/Users/hp/Documents/Data Engineering/dept.json")
  .load()
  
  val empDF = spark.read
  .option("inferSchema",true)
  .format("json")
  .option("path","C:/Users/hp/Documents/Data Engineering/employee.json")
  .load()
  
  val joinedDF = empDF.join(deptDF,deptDF.col("deptid")=== empDF.col("deptid"),"right")

  val joinedNewDF = joinedDF.drop(empDF.col("deptid"))
  val countDF = joinedNewDF.groupBy("deptid").agg(count("id").as("count"))
  
  val resDF = joinedNewDF.join(countDF,countDF.col("deptid")=== joinedNewDF.col("deptid"),"Left").drop(countDF.col("deptid")).dropDuplicates("deptName")
  resDF.select("deptid","deptName","count").show()
  
  spark.stop()
}
