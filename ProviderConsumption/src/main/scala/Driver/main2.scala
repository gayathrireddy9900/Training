package Driver

import Reader.Readfiles.csvfile
import Transformation.Transform.{sparkTransform, transformation}
import Utils.Constants
import Utils.utils.Readproperties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.explode

object main2 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", Constants.hadoop_home_dir)

    //calling SparkSeeion
    val spark = Utils.utils.Createspark()
    //log level
    spark.sparkContext.setLogLevel("ERROR")

    //calling file path
    val confpath = Readproperties(Constants.confpath)
    val ADD = confpath.getString("ADD")
    val Provloc = confpath.getString("ProvLoc")
    val Provider = confpath.getString("Provider")
    val spclity = confpath.getString("spclity")
    /*val Employee = confpath.getString("Employee")*/

    //calling csvfile
    val addr = csvfile(ADD, spark)
    val prov_loc = csvfile(Provloc, spark)
    val provider = csvfile(Provider, spark)
    val spclty = csvfile(spclity, spark)
    /*val employeedf=csvfile(Employee,spark)*/

    //Creating tables
    addr.createOrReplaceTempView("addr")
    prov_loc.createOrReplaceTempView("prov_loc")
    provider.createOrReplaceTempView("provider")
    spclty.createOrReplaceTempView("spclty")
    /*employeedf.createOrReplaceTempView("Employee")*/


   /* val df_new = employeedf.withColumn("Experience",split(col("Experience"),"\\|")).withColumn("Experience",explode(col("Experience")))
    df_new.show(false)
*/
    val trans = Readproperties(Constants.sqlpath)
     val out = transformation("sql2", spark, trans)
     out.show()

   /* val output = sparkTransform(addr, prov_loc, provider, spclty, spark)
    println("GoodRecords")
    val GoodRecords = output._2
    GoodRecords.show(false)
    println("Bad Records")
    val BadRecords = output._1
    BadRecords.show(false)*/

  }
}