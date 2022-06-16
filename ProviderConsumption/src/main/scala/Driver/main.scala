package Driver

import Reader.Readfiles.csvfile
import Utils.Constants
import Utils.utils.Readproperties

object main {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", Constants.hadoop_home_dir)

    //calling SparkSeeion
    val spark = Utils.utils.Createspark()

    //log level
    spark.sparkContext.setLogLevel("ERROR")

    //calling property
    val path = Readproperties(Constants.confpath)
    val loc = path.getString("loc")
    val spc = path.getString("spc")

    //calling csv files
    val locdf = csvfile(loc, spark)
    val spcdf = csvfile(spc, spark)

    locdf.createOrReplaceTempView("FDL_PROV_LOC")
    spcdf.createOrReplaceTempView("FDL_PROV_SPCLTY")


    val trans = Utils.utils.Readproperties(Constants.sqlpath)
    val out = Transformation.Transform.transformation("sql1", spark, trans)
    out.show()

    Writer.writeoutput.res(path.getString("outpath"), out)
  }
}
