import org.apache.spark.sql.functions.{trim, col, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import java.io._




object MainObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("assignment").master("local[*]").getOrCreate()

//    val conf = new SparkConf().setMaster("local[*]"). setAppName("assignment")
//
//    val spark = new SparkContext(conf)
    val user_profile_schema = StructType(Array(
      StructField("#id", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("registered", StringType, nullable = true),
    ))

    val user_profile_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(user_profile_schema)
      .load("src/main/scala/data/r-userid-profile.tsv")

    def user_df_removeInvalidUsers()(df: DataFrame): DataFrame = {
      df.filter(df("#id").startsWith("user_"))
    }

    def user_df_dropNulls()(df: DataFrame): DataFrame = {
      df.na.drop(Seq("gender","age", "country", "registered"))
    }

    def user_df_dropDuplicateID()(df: DataFrame): DataFrame = {
      df.dropDuplicates("#id")
    }


    def user_df_removeWhiteSpace()(df: DataFrame): DataFrame = {
      df.withColumn("country", trim(col("country")))
      df.withColumn("registered",trim(col("registered")))
    }

    def user_df_removeGenderNull()(df: DataFrame): DataFrame = {
      df.filter(!df("gender").isNull)
    }

   val user_profile_df_cleaned = user_profile_df.transform(user_df_removeInvalidUsers())
      .transform(user_df_dropNulls())
      .transform(user_df_dropDuplicateID())
      .transform(user_df_removeGenderNull())
      .transform(user_df_removeWhiteSpace())

    user_profile_df_cleaned.show(5)





//    ==================================================================================================
    val music_df_schema = StructType(Array(
      StructField("userid", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("artid", StringType, nullable = true),
      StructField("artname", StringType, nullable = true),
      StructField("traid", StringType, nullable = true),
      StructField("traname", StringType, nullable = true)
    ))
    val music_df = spark.read.format("csv")
      .option("delimiter", "\t")
      .schema(music_df_schema)
      .load("src/main/scala/data/r-userid-timestamp-artid-artname-traid-traname.tsv")

    def music_df_removeInvalidUsers()(df: DataFrame): DataFrame = {
      df.filter(df("userid").startsWith("user_"))
    }

    def music_df_dropNulls()(df: DataFrame): DataFrame = {
      df.na.drop(Seq("artid","artname", "traid", "traname"))
    }

    def music_df_dropDuplicate()(df: DataFrame): DataFrame = {
      df.dropDuplicates()
    }

    def music_df_removeWhiteSpace()(df: DataFrame): DataFrame = {
      df.withColumn("artid", trim(col("artid")))
      df.withColumn("artname",trim(col("artname")))
      df.withColumn("traid",trim(col("traid")))
      df.withColumn("traname",trim(col("traname")))
    }

    val music_df_cleaned = music_df.transform(music_df_removeInvalidUsers())
      .transform(music_df_dropNulls())
      .transform(music_df_dropDuplicate())
      .transform(music_df_removeWhiteSpace())


//    ====================================================================================================
//    Metrics
    val pw = new PrintWriter(new File("Q1.txt" ))
    val Q1ans = music_df_cleaned.dropDuplicates("artname", "traid").count()
    pw.write("Q: Bob would like to know how many tracks were listened to in total.\n" + Q1ans)
    pw.close()

    val pw2 = new PrintWriter(new File("Q2.txt" ))
    val Q2ans = user_profile_df_cleaned.dropDuplicates("#id").count()
    pw2.write("Q: Bob would like to know how many tracks were listened to in total.\n" + Q2ans)
    pw2.close()

    val pw3 = new PrintWriter(new File("Q2.txt" ))
    val Q3ans = user_profile_df_cleaned.dropDuplicates("#id").count()
    pw3.write("Q: Bob would like to know how many tracks were listened to in total.\n" + Q3ans)
    pw3.close()
  }
}
