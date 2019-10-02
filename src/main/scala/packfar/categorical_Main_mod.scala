package packfar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType

object categorical_Main_mod extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")

  //import my data NUMERIC


  val df_brut_test: DataFrame = beginspark.importDF(data_path + "/test.csv")
  //to get data consistency between train and test
  val df_brut_train: DataFrame = beginspark.importDF(data_path + "/train.csv")

  val df_ID_train = df_brut_train.select("Id")
  val df_cible_train = df_brut_train.select("SalePrice").cast_all_columns_to_numeric(DoubleType)

  var df_cat_train = df_brut_train
    .drop(spatial_colums_type ::: Numerical_columns_type:_*)
    .select_Impute_And_Transform_categorical_to_numerical()
    .join_df2_by_index(df_cible_train)

  val df_ID_test = df_brut_test.select("Id")
  var df_cat_test = df_brut_test
    .drop(spatial_colums_type ::: Numerical_columns_type : _*)
    .select_Impute_And_Transform_categorical_to_numerical()

  println(df_cat_train.columns.length)
  println(df_cat_test.columns.length)
  val lisdatanum = get_final_trainTest_Num()
  println(lisdatanum.head.columns.toSet.intersect(df_cat_train.columns.toSet))





//  val kmen=regularisation_training_compared_to_testing(df_cat_train,df_cat_test,nb_classe = 600)
//  kmen.head.show(10,false)
//  kmen(1).show(10,false)
//  println(kmen.head.count(),df_cat_train.count())
//  println(kmen(1).count(),df_cat_test.count())

  spark.close()
}
