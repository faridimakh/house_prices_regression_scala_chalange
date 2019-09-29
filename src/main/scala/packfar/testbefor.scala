package packfar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{FloatType, IntegerType}


object testbefor extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")

  //import my data
  val df_brut_train: DataFrame = beginspark.importDF("Data/train.csv")
  val df_brut_test: DataFrame = beginspark.importDF("Data/test.csv") //to get data consistency between train and test

  val df_ID_train = df_brut_train.select("Id")
  val df_ID_test = df_brut_test.select("Id")
  val df_cible = df_brut_train.select("SalePrice").cast_all_columns_to_numeric(FloatType)

  //  extracrion des type de variable numeric
  val test_features = df_brut_train.select_cols_By_Type(IntegerType).join_df2_by_index(df_brut_train.select("GarageYrBlt", "LotFrontage", "MasVnrArea"))
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold", "SalePrice").cast_all_columns_to_numeric(FloatType)
  val train_features = df_brut_test.select_cols_by_names(test_features.columns.toList).cast_all_columns_to_numeric(FloatType).
    join_df2_by_index(df_cible)

  //****************************************************************************************************************************************************************
  val list_datas_regularized = regularisation_training_compared_to_testing(train_features, test_features, nb_classe = 150) //list of dataframes
  //*******************ici le dataframe d'entrainement est regularisé avec le kmean en fonction du test:datas_regularized.head  **************************************
  val train = list_datas_regularized.head
  val test = list_datas_regularized(1)

  //---------------------------------------------------------------
  // Load the workflow back
  val myRFmodel = CrossValidatorModel.load("model/RF_model")
  //---------------------------------------------------------------
//evaluation
  Array("mse", "rmse", "r2", "mae").map(x=>println(x+" :"+new RegressionEvaluator().setMetricName(x).evaluate(myRFmodel.transform(train))))
  //---------------------------------------------------------------
//  save prdictions
  import java.io._
  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete)
    file.delete
  }
  delete(new java.io.File("/home/farid/Bureau/kaggHousPrice/result_RF_reuse.csv"))
  df_ID_test.join_df2_by_index(myRFmodel.transform(test)
    .select("prediction"))
    .coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("result_RF_reuse.csv")

  spark.stop()

}