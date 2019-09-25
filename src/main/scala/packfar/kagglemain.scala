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
  df_train_test.show()

  spark.close()
}
