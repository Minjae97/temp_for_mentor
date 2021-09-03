import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object FirstDemo {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]"). setAppName("FirstDemo")
//
//    val sc = new SparkContext(conf)
//
//    val rdd = sc.parallelize(Array(5, 10, 30))
//
//    println(rdd.reduce(_+_))
    val logFile = "C:\\Users\\WenMin\\Desktop\\Kang\\Career\\Holland_Barrett\\scala_spark_project\\src\\main\\scala\\README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
