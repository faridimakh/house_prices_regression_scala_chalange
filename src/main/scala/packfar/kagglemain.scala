package packfar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{FloatType, IntegerType}

object kagglemain extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")
  //import my data
  val df_brut_train: DataFrame = beginspark.importDF("Data/train.csv")
  val df_brut_test: DataFrame = beginspark.importDF("Data/test.csv") //to get data consistency between train and test

  val df_num_test_kmean = df_brut_test.select_cols_By_Type(IntegerType)
    //    .withColumn("SalePrice", lit(0))
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold")
    .conveting_multiple_columns_to_specific_DataType(FloatType)
    .impute_numeric_vars_with_mean_or_median()

  val df_num_train_kmean = df_brut_train.select_cols_By_Type(IntegerType)
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold")
    .conveting_multiple_columns_to_specific_DataType(FloatType)
    .select_cols_by_names(df_num_test_kmean.columns.toList :+ "SalePrice")
  //    .impute_numeric_vars_with_mean_or_median()

  
  var states = scala.collection.mutable.Map[Int, Double]()
  val nb_classes = List(2, 3, 5, 8, 20, 30, 40, 150, 200)
  for (i <- nb_classes) {
    val df_regularise_trn_tst = regularisation_training_compared_to_testing(df_num_train_kmean, df_num_test_kmean, i)
    df_regularise_trn_tst.head.show(2, truncate = false)
    df_regularise_trn_tst(1).show(2, truncate = false)

    val rf = new RandomForestRegressor().setNumTrees(10).setMaxDepth(5)
    val rf_mod = rf.fit(df_regularise_trn_tst.head)

    val predictions = rf_mod.transform(df_regularise_trn_tst(2))

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    states += (i -> rmse)

  }
  states.foreach(println)
  spark.close()
}