package configs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType}

trait step3 extends step2 {
  //--------------------------------------------------------------------------------------------------------------------
  private lazy val features_predictors_integertype: Array[String] = select_cols_By_Type(train, IntegerType)
    .columns.filter(_.contains(target) == false).filter(_.contains("Id") == false)
  //--------------------------------------------------------------------------------------------------------------------
  lazy val train: DataFrame = train_test.filter(col(target) > 0.0)
  lazy val test: DataFrame = train_test.filter(col(target) === 0.0)

  lazy val features_predictors_stringtype: Array[String] = select_cols_By_Type(train, StringType).columns
  lazy val features_predictors_to_assemble: Array[String] = features_predictors_integertype ++
    features_predictors_stringtype.map(_.concat(prefix_string_type))

}
