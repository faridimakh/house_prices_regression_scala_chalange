package configs

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait step2 extends step1 {

  //load data-----------------------------------------------------------------------------------------------------------
  private lazy val train_brut: DataFrame = spark_csv_reader.load(data_path.concat("/").concat(train_csv))


    .withColumn("labeldata", lit("train"))

  private lazy val schema_test_brut: StructType = train_brut.drop(target, "labeldata").schema

  private lazy val test_brut: DataFrame = spark_csv_reader.schema(schema_test_brut).load(data_path.concat("/")
    .concat(test_csv))
    .withColumn(target, lit(0.0))

    .withColumn("labeldata", lit("test"))

  private lazy val test_union_train_na: Dataset[Row] = train_brut.union(test_brut).withColumn(label, col(target)
    .cast(DoubleType))
    .withColumn("LotFrontage"+"_x", col("LotFrontage").cast("integer"))
    .withColumn("MasVnrArea"+"_x", col("MasVnrArea").cast("integer"))
    .withColumn("GarageYrBlt"+"_x", col("GarageYrBlt").cast("integer"))
    .drop("LotFrontage", "MasVnrArea", "GarageYrBlt")
//    .drop(/*id,*/ "LotFrontage", "MasVnrArea", "GarageYrBlt" /*, "GarageArea", "BsmtFinSF1"*/)
    .drop(target)

  private lazy val string_cols: Array[String] = select_cols_By_Type(test_union_train_na, StringType).columns
  private lazy val train_test: DataFrame = test_union_train_na.na.replace(string_cols, na_replace_map)

  private lazy val tstlb: Dataset[Row] = train_test.filter(col("labeldata").===("test"))
  lazy val test: DataFrame = tstlb.drop("labeldata")

  private lazy val trnlb: Dataset[Row] = train_test.filter(col("labeldata").===("train"))
  lazy val train: DataFrame = trnlb.drop("labeldata")

  //--------------------------------------------------------------------------------------------------------------------
  lazy val feature_stringtype: Array[String] = select_cols_By_Type(train, StringType).columns

  private lazy val feature_stringtype_indexed: Array[String] = feature_stringtype.map(_.concat(prefix_string_type))
  private lazy val feature_integertype: Array[String] = select_cols_By_Type(train, IntegerType)
    .columns.filter(_.contains(target) == false).filter(_.contains("Id") == false)
  lazy val features_predictors_to_assemble: Array[String] = feature_integertype ++ feature_stringtype_indexed

}

