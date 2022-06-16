package Writer

import org.apache.spark.sql.DataFrame

object writeoutput {
  def res(path: String, file: DataFrame):Unit = {
    file.write.format("parquet").save(path)
  }
}