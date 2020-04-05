package configs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

trait step1 extends step0 {
  //get a sparak session:-----------------------------------------------------------------------------------------------
  private lazy val session: SparkSession = new SparkSession.Builder().appName(saprk_name).master(spark_master)
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()

  //get set stop log from SparkSession:---------------------------------------------------------------------------------
  session.sparkContext.setLogLevel(log_stop)

  //get a loader csv from SparkSession:---------------------------------------------------------------------------------
//  lazy val spark_csv_reader: DataFrameReader = session.read.format("csv").option("header", "true").option("inferSchema", "true")
lazy val spark_csv_reader: DataFrameReader = session.read.format("com.databricks.spark.csv").option("header", "true")
  .option("inferSchema", "true")
  .option("nullValue", "NA")
  .option("treatEmptyValuesAsNulls", "true")
  //function to select columns:-----------------------------------------------------------------------------------------
  def select_cols_By_Type(df: DataFrame, column_Type: DataType): DataFrame = {
    val cols = df.schema.toList
      .filter(x => x.dataType == column_Type)
      .map(c => col(c.name))
    df.select(cols: _*)
  }

  //function to save DataFrame:-----------------------------------------------------------------------------------------
  def save_df(df: DataFrame,
              nb_partition: Int = 1,
              format_saving: String = "com.databricks.spark.csv",
              path: String = path_to_save_predictions,
              namedf: String="test_predictions"): Unit = {
    df.coalesce(nb_partition).write.mode(SaveMode.Overwrite).format(format_saving).option("header", "true")
      .save(path.concat("/") + namedf)
  }


}

