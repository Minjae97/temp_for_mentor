import org.apache.spark.sql.SparkSession
trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("H&B_Assignment")
      .getOrCreate()
  }
}

// When a class is extended with the SparkSessionWrapper,
// It will have access to the session via the spark variable.
// Starting and stopping the SparkSession is expensive and our code will run faster if we only create one SparkSession.