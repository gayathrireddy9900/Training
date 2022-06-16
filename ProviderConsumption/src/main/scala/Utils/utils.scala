package Utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object utils {
  def Createspark():SparkSession={
    val spark = SparkSession.builder()
      .appName("SparkByExamples.com")
      .master("local")
      .getOrCreate()
    spark



    }
  def Readproperties(path:String): Config={
    val Conf=Paths.get(path).toFile()
    val myconfig=ConfigFactory.parseFile(Conf)
    myconfig

  }
}