import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object w12_p2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConfig = new SparkConf()
    .set("spark.app.name","my App")
    .set("spark.master","local[2]")
    
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  import spark.implicits._
  case class rating(userId: Int , movieId: Int , rating: Int , timestamp: Int)
  case class movie(id :Int , name: String , genre: String)
  
  val ratingsRDD = spark.sparkContext.textFile("C:/Users/hp/Documents/Data Engineering/ratings.dat")
  val moviesRDD =  spark.sparkContext.textFile("C:/Users/hp/Documents/Data Engineering/movies.dat")

  val ratingsSplit = ratingsRDD.map(x=>x.split("::"))
  val moviesSplit = moviesRDD.map(x=>x.split("::"))
  
  //map it to case class and convert to DF
  val ratingsDF = ratingsSplit.map(x=>(rating(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt))).toDF()
  val moviesDF = moviesSplit.map(x=>(movie(x(0).toInt,x(1),x(2)))).toDF()
  
  // join both the DF
  val joinedDF = moviesDF.join(broadcast(ratingsDF),ratingsDF.col("movieId") === moviesDF.col("id"),"right").drop("movieId","userId","timestamp","genre")
  
  // group them on id and perform aggregate functions
  val groupDF = joinedDF.groupBy("id").agg(count("name").as("ratingsCount"),avg("rating").as("avgRating"))
  
  val df = joinedDF.join(groupDF,groupDF.col("id") === joinedDF.col("id"),"inner").drop(groupDF.col("id")).dropDuplicates("id")
  
  df.select("id","name","ratingsCount","avgRating").where("ratingsCount>1000 and avgRating>4.5").sort(desc("avgRating")).show
  spark.stop()
}
