package packfar

import org.apache.spark.sql.functions._

object kagglemain extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")
  //import my data
  val df_train = beginspark.importDF("/home/farid/Bureau/house-prices-advanced-regression-techniques/train.csv")
  val df_test = beginspark.importDF("/home/farid/Bureau/house-prices-advanced-regression-techniques/test.csv")
    .withColumn("SalePrice", lit(0))//to get data consistency between train and test

  val df_train_test = df_train.union(df_test)
  df_train_test.show(2)


  //val df_key=df_train.select_cols_by_names(List("Id"))
  //val df_numerics=df_train.select_cols_By_Type(IntegerType).conveting_multiple_columns_to_specific_DataType(DoubleType).drop("Id")
  //val df_categorial=df_train.select_Impute_And_Transform_categorical_to_numerical()
  //  val df_cible:DataFrame= df_train.select_cols_by_names(List("SalePrice"))
  //
  //  .impute_numeric_vars_with_mean_or_median()
  //  val dffinal=df_key.join_df2_by_index(df_categorial).join_df2_by_index(df_numerics)
  //df_train.select_cols_By_Type(IntegerType).show()


  //  val vect_ass = new VectorAssembler().setInputCols(df3.columns).setOutputCol("assemb")
  //    val dd2=vect_ass.transform(df3)
  //  dd2.show()
  //  val featureIndexer = new VectorIndexer().inputCol()


  //  val df=df_string_var.na.replace(df_string_var.columns,Map("NA"->"nr"))


  //  val df_classif2 = df_classif.select_cols_By_Type(IntegerType).drop("Id","SalePrice")
  //
  //
  //  val vect_ass = new VectorAssembler().setInputCols(df_classif2.columns).setOutputCol("assemb")
  //  val dd = vect_ass.transform(df_classif2)
  //  val fefe = new PCA().setInputCol("assemb").setOutputCol("sdfsd").setK(3).fit(dd).transform(dd)
  //  val kmean=new KMeans().setK(5).setFeaturesCol("assemb").setPredictionCol("mes_class").fit(dd).transform(dd)
  //
  ////  kmean.select("assemb","mes_class").show(500,truncate = false)
  //  kmean.select("mes_class").groupBy("mes_class").count().show(500,truncate = false)

  //  val df_classif3 = df.select_cols_By_Type(StringType)
  //df_classif3.withColumn("LotConfig2",trim(col("LotConfig")))
  ////  val stindex=new StringIndexer().setInputCol("LotConfig2").setOutputCol("hhb").fit(df_classif3).transform(df_classif3)
  ////  stindex.select("LotConfig2","hhb").show(false)
  //df_classif3.select("LotConfig","LotConfig").show()


  spark.close()
}
