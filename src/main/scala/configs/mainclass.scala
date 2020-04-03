package configs

import org.apache.spark.sql.functions._

object mainclass extends step3 {
  def main(args: Array[String]): Unit = {
    val model = CrossValidator.fit(train)
    val test_predictions = model.bestModel.transform(test).select(id, prediction)
      .withColumn(prediction + "_x", round(col(prediction), 0).cast("integer"))
      .withColumnRenamed(prediction + "_x", target)
      .drop(prediction)
    save_df(test_predictions)

  }

}


/*
    val house_data = spark_csv_reader. /*schema(get_schema).*/ load(data_path.concat("/").concat(train_csv))

    val cols = Array[String]("SalePrice", "LotArea", "RoofStyle", "Heating", "1stFlrSF",
      "2ndFlrSF", "BedroomAbvGr", "KitchenAbvGr", "GarageCars", "TotRmsAbvGrd", "YearBuilt")

    val skinny_house_data = house_data.select(cols.map(n => col(n)): _*)

    val skinny_house_data1 = skinny_house_data.withColumn("TotalSF", col("1stFlrSF") +
      col("2ndFlrSF")).drop("1stFlrSF", "2ndFlrSF")
      .withColumn("SalePrice", col("SalePrice").cast("double"))

    //    skinny_house_data1.describe("SalePrice").show
    //
    val roofStyleIndxr = new StringIndexer().setInputCol("RoofStyle")
      .setOutputCol("RoofStyleIdx")
      .setHandleInvalid("skip")
    //
    val heatingIndxr = new StringIndexer().setInputCol("Heating")
      .setOutputCol("HeatingIdx")
      .setHandleInvalid("skip")

    val linearRegression = new LinearRegression().setLabelCol("SalePrice")
    val v = Array(
       "RoofStyle",
      "HeatingIdx", "BedroomAbvGr",
      "KitchenAbvGr", "GarageCars",
      "TotRmsAbvGrd", "YearBuilt",
      "TotalSF"
    )
    var prc=0.0
    var vv = Array("LotArea")
    for (i <- 1 until v.length) {
      val assembler = new VectorAssembler()
        .setInputCols(vv
        ).setOutputCol("features")
      //
      //    // setup the pipeline
      val pipeline = new Pipeline().setStages(Array(roofStyleIndxr, heatingIndxr, assembler, linearRegression))
      // split the data into training and test pair
      val Array(training, testing) = skinny_house_data1.randomSplit(Array(0.8, 0.2), seed = 123)
      //    //     train the pipeline
      val model = pipeline.fit(training)
      //    model.show()
      // perform prediction
      val predictions = model.transform(testing)
      val evaluator = new RegressionEvaluator().setLabelCol("SalePrice")
        .setPredictionCol("prediction")
        .setMetricName("mse")
      val rmse = evaluator.evaluate(predictions)
    println("-----------------------------------------------------------")
      println(rmse)
      println(prc-rmse)
      prc=rmse
      vv.foreach(x=>print(x+"  "))
      vv = vv ++ Array(v(i))
    println()
    println("-----------------------------------------------------------")
    }

*/
