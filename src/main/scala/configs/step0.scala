package configs

import com.typesafe.config.{Config, ConfigFactory}

trait step0 {
  //load configuration path infos:
  private final val myconf: Config = ConfigFactory.load()
  val saprk_name: String = myconf.getString("session.name")
  val spark_master: String = myconf.getString("session.master")
  val log_stop: String = myconf.getString("session.stop_log")
  //----------------------------------------------------
  val data_path: String = myconf.getString("input.path")
  val train_csv: String = myconf.getString("input.train")
  val test_csv: String = myconf.getString("input.test")
  //----------------------------------------------------
  val path_to_save_model: String = myconf.getString("output.path_to_save_model")
  val path_to_save_predictions: String = myconf.getString("output.path_to_save_predictions")
  //--------------------------------------------------------------------------------------------------------------------
  val target: String = "SalePrice"
  val predictors_assembled: String = "features"
  val label: String = "label"
  val prediction: String = "prediction"
  val metric: String = "rmse"
  val prefix_string_type: String = "_indexed"
  val id = "Id"
  //--------------------------------------------------------------------------------------------------------------------
  val na_replace_map = Map("NA" -> "nr", "None" -> "nr")
  val model_save_as = "rfmodel"

}
























//    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
//    .config("spark.debug.maxToStringFields", "100")


