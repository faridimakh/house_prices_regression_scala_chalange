package configs

import org.apache.spark.sql.functions._

object mainclass extends step4 {
  def main(args: Array[String]): Unit = {
    val model = CrossValidator.fit(train)
    val test_predictions = model.bestModel.transform(test).select(id, prediction)
      .withColumn(prediction + "_x", round(col(prediction), 0).cast("integer"))
      .withColumnRenamed(prediction + "_x", target)
      .drop(prediction)
    save_df(test_predictions)
  }
}



