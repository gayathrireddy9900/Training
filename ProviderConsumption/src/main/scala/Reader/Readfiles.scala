package Reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object Readfiles {

  def csvfile(path:String,spark:SparkSession): DataFrame={
    val df=spark.read.format("csv").option("header","true").load(path)
    df

  }

}
