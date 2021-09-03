//package reading
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//import java.io.File
//
//object DataFrameCsvFileReader {
//  case class Config(file: File, separator: Char, hasHeader: Boolean = false)
//}
//
//class DataFrameCsvFileReader(spark: SparkSession, config: DataFrameCsvFileReader.Config) extends _root_.DataFrameReader {
//
//  override def read(): DataFrame = {
//    spark.read
//      .option("header", config.hasHeader.toString.toLowerCase)
//      .option("sep", config.separator.toString)
//      .csv(config.file.getAbsolutePath)
//  }
//
//}