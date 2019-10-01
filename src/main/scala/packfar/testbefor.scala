package packfar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.FloatType

object testbefor extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")

  //import my data NUMERIC


  val df_brut_test: DataFrame = beginspark.importDF(data_path + "/test.csv").drop(spatial_colums_type: _*) //to get data consistency between train and test
  val df_brut_train: DataFrame = beginspark.importDF(data_path + "/train.csv").drop(spatial_colums_type: _*)
  //
  val df_ID_train = df_brut_train.select("Id")
  val df_ID_test = df_brut_test.select("Id")
  val df_cible = df_brut_train.select("SalePrice").cast_all_columns_to_numeric(FloatType)
  //
  //  //  extracrion des type de variable categorique
  val test_features = df_brut_test.select_Impute_And_Transform_categorical_to_numerical()
  test_features.show(2,truncate = false)
  val train_features = df_brut_test.select_cols_by_names(test_features.columns.toList).
    join_df2_by_index(df_cible)
  train_features.show()

}