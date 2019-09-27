package packfar

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
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold")
    .conveting_multiple_columns_to_specific_DataType(FloatType)

  val df_cat_test_kmean = df_brut_test.select_Impute_And_Transform_categorical_to_numerical()
    .conveting_multiple_columns_to_specific_DataType(FloatType)
  val df1 = df_num_test_kmean.join_df2_by_index(df_cat_test_kmean)


  val df_num_train_kmean = df_brut_train.select_cols_By_Type(IntegerType)
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold")
    .conveting_multiple_columns_to_specific_DataType(FloatType)
    .select_cols_by_names(df_num_test_kmean.columns.toList :+ "SalePrice")
  val df_cat_train_kmean = df_brut_test.select_Impute_And_Transform_categorical_to_numerical()
    .conveting_multiple_columns_to_specific_DataType(FloatType)
  val df0 = df_num_train_kmean.join_df2_by_index(df_cat_train_kmean)

  val df_is_test = df_brut_test.select("Id")

//  var states = scala.collection.mutable.Map[Int, Double]()
//  val nb_classes = List(2, 3, 5, 8, 20, 30, 200)
//  for (i <- nb_classes) {
//    val df_regularise_trn_tst = regularisation_training_compared_to_testing(df0, df1, i)
//    df_regularise_trn_tst.head.show(2, truncate = false)
//    df_regularise_trn_tst(1).show(2, truncate = false)
//
//    val rf = new RandomForestRegressor().setNumTrees(10).setMaxDepth(5)
//    val rf_mod = rf.fit(df_regularise_trn_tst.head)
//
//    val predictions = rf_mod.transform(df_regularise_trn_tst(2))
//
//    val evaluator = new RegressionEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("rmse")
//    val rmse = evaluator.evaluate(predictions)
//    states += (i -> rmse)
//
//  }
//  states.foreach(println)


    val df_regularise_trn_tst = regularisation_training_compared_to_testing(df0, df1)

      val rf = new RandomForestRegressor().setNumTrees(1000).setMaxDepth(12)
      val rf_mod = rf.fit(df_regularise_trn_tst.head)

      val predictions = rf_mod.transform(df_regularise_trn_tst(1))

  val dfdf=predictions.join_df2_by_index(df_is_test)//.withColumnRenamed("prediction","SalePrice")select("Id","prediction")
  dfdf.write.format("csv").save("/home/farid/Bureau/kaggHousPrice/Data/sam.csv")

  spark.close()
}
