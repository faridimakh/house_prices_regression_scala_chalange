package packfar

import org.apache.spark.sql.{DataFrame, SparkSession}

class Begining_spark_Local_Import() {
  def get_local_spark_session(): SparkSession = {
    SparkSession.builder.master("local[*]").appName("monlocalSpark").getOrCreate()
  }

  def importDF(csv_pah: String): DataFrame = {
    this.get_local_spark_session().read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(csv_pah)
  }
}
