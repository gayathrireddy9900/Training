package Transformation

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, current_date, datediff, floor, from_unixtime, isnull, lit, to_date, unix_timestamp, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Transform {

def transformation(name:String,spark: SparkSession,prop:Config):DataFrame={
  val sql=prop.getString(name)
  val df=spark.sql(sql)
  df

}

  def sparkTransform(addr: DataFrame, prov_loc: DataFrame, provider: DataFrame, spclty: DataFrame,
                     spark: SparkSession): (DataFrame,DataFrame) = {

    val df = prov_loc.join(provider, prov_loc("prov_id") === provider("prov_id"), "left")
      .join(addr, addr("ADDR_ID") === prov_loc("ADDR_ID") and addr("CORP_ENT_CD") === prov_loc("CORP_ENT_CD"), "left")
      .join(spclty, spclty("SPCLTY_CD") === prov_loc("SPCLTY_CODE") and spclty("CORP_ENT_CD") === prov_loc("CORP_ENT_CD"), "left")

    val df1 = df.select(provider("PROV_ID"), prov_loc("PROV_LOC_ID"), prov_loc("BILG_LOC_ID"), provider("PROV_CLS_CD"), provider("GEND_CD") as "Gender"
      , from_unixtime(unix_timestamp(provider("DOB"), "dd-MM-yyyy"), "yyyy-MM-dd") as "DOB"
      , floor(datediff(to_date(current_date), to_date(from_unixtime(unix_timestamp(provider("DOB"), "dd-MM-yyyy"), "yyyy-MM-dd"))) / 365.25) as "AGE"
      , provider("FIRST_NAME"), provider("LAST_NAME"), provider("ORGZN_NAME") as "PROV_ORGNZ_NAME"
      , addr("ADDR_LN_1_TXT"), addr("ADDR_LN_2_TXT"), addr("CTY_CD"), addr("PHN_NBR"), addr("ST_PRVNC_CD"), addr("POSTL_CD"), spclty("SPLCTY_DESC"))


    var df2 = prov_loc.as("l1").join(prov_loc.as("l2"), col("l1.BILG_LOC_ID") === col("l2.PROV_LOC_ID"), "left")
      .join(provider.as("p"), col("p.prov_id") === col("l2.prov_id"), "left")
    df2 = df2.select(col("l1.prov_id") as "ProvId", col("l1.PROV_LOC_ID") as "locID", col("p.FIRST_NAME"), col("p.last_name"), col("p.orgzn_name"))
    var df3 = df2.withColumn("BILG_PROV_NAME", when(isnull(col("ORGZN_NAME")), functions.concat(col("FIRST_NAME"), lit(" "), col("LAST_NAME"))).otherwise(col("ORGZN_NAME")))
    df3 = df3.select("provid", "locid", "BILG_PROV_NAME")
    var finalDF = df1.join(df3, df1("PROV_LOC_ID") === df3("locid"), "inner").orderBy("PROV_ID")
    finalDF = finalDF.drop("provid", "locid")
    finalDF



    val bad = finalDF.filter(isnull(finalDF("ADDR_LN_1_TXT")) or isnull(finalDF("SPLCTY_DESC")) or (isnull(finalDF("FIRST_NAME"))
      and isnull(finalDF("LAST_NAME")) and isnull(finalDF("PROV_ORGNZ_NAME"))))

    val badrec=bad.withColumn("ERROR_DESCRIPTION",
      when(isnull(col("ADDR_LN_1_TXT")),"Invalid address")
        .when(isnull(col("SPLCTY_DESC")),"Invalid Specialty")
        .when(isnull(col("FIRST_NAME")),"Invalid Firstname")
        .when(isnull(col("LAST_NAME")),"Invalid Lastname")
        .when(isnull(col("PROV_ORGNZ_NAME")),"Invalid Prov_org_name")
    )
    val good =finalDF.join(bad,finalDF("PROV_ID") === bad("PROV_ID"),"leftanti")

    (badrec, good)



  }}