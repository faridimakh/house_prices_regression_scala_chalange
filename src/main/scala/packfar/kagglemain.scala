package packfar
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object kagglemain extends App {
  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")
  //import my data
  val df_brut_train: DataFrame = beginspark.importDF("Data/train.csv")
  val df_brut_test: DataFrame = beginspark.importDF("Data/test.csv").withColumn("SalePrice", lit(0)) //to get data consistency between train and test
  //reunir le test est le train
  val df_brut_train_test = df_brut_train.union(df_brut_test)
  //import data by types

  val df_id = df_brut_train_test.select_cols_by_names(List("Id"))
  val df_cible = df_brut_train_test.select_cols_by_names(List("SalePrice"))
  val df_cat_var = df_brut_train_test.select_Impute_And_Transform_categorical_to_numerical()
  val df_num_var = df_brut_train_test.select_cols_By_Type(IntegerType)
    .drop("Id", "YearBuilt", "YearRemodAdd", "YrSold", "SalePrice")
    .conveting_multiple_columns_to_specific_DataType(FloatType)
    .impute_numeric_vars_with_mean_or_median()
  val df_date_var = df_brut_train_test.select_cols_by_names(List("YearBuilt", "YearRemodAdd", "YrSold")) //.na.fill(2019)

  //1)model over numerical variables banckmark :
  //----we need only df_num_var, df_cible
  val df_num_cible_train_test = df_num_var.join_df2_by_index(df_cible)
  val df_num_cible_train = df_num_cible_train_test.where(col("SalePrice") =!= 0)
  val df_num_cible_test = df_num_cible_train_test.where(col("SalePrice") === 0).drop("SalePrice")

//
  val assembler = new VectorAssembler().setInputCols(df_num_cible_test.columns).setOutputCol("features")
  val df_num_cible_train_assembled=assembler.transform(df_num_cible_train).select("SalePrice","features")
    .withColumnRenamed("SalePrice","label")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

 val  mod=lr.fit(df_num_cible_train_assembled)
  //val mopipline=new Pipeline().setStages(Array(assembler, lr))
  println(mod.coefficients)
mod.transform(df_num_cible_train_assembled).show()
  spark.close()
}
