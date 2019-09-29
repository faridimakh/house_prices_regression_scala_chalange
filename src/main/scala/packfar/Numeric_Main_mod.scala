package packfar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{FloatType, IntegerType}

object Numeric_Main_mod extends App {
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
  //random forst model buld and tuning:
  //on aura besoin des variable suivantes par la suite
  val numTrees : Array[Int] = Array(50,100,500,1000,3000)
  val MaxBins : Array[Int]= Array(2,3,8,12,15)
  val maxDepth: Array[Int] = Array(1,3,5,10,12,15,20,30)
  val numFolds = 10

  val model = new RandomForestRegressor()
    .setFeaturesCol("features")
    .setLabelCol("label")
//---------------------------------------------------------------
  val paramGrid = new ParamGridBuilder()
    .addGrid(model.numTrees, numTrees)
    .addGrid(model.maxBins, MaxBins)
    .addGrid(model.maxDepth,maxDepth)
    .build()

  val cv = new CrossValidator()
    .setEvaluator(new RegressionEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(numFolds)
    .setEstimator(model)
//---------------------------------------------------------------
//  build the workflow
  val cvModel =cv.fit(train)
//---------------------------------------------------------------
//  save the workflow
  delete(new java.io.File("/home/farid/Bureau/kaggHousPrice/model"))
  cvModel.write.overwrite().save("model/RF_model")

////---------------------------------------------------------------
//  Load the workflow back
val myRFmodel = CrossValidatorModel.load("model/RF_model")
  //---------------------------------------------------------------
//  evaluation the model
  val rf_evaluator = new RegressionEvaluator().setMetricName("rmse")
  println(rf_evaluator.evaluate(myRFmodel.transform(train)))
  //---------------------------------------------------------------
//  save predictions
  delete(new java.io.File("/home/farid/Bureau/kaggHousPrice/result_RF_reuse.csv"))
  df_ID_test.join_df2_by_index(myRFmodel.transform(test)
    .select("prediction"))
    .coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("result_RF_reuse.csv")

  spark.stop()
}
