package configs

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

trait step3 extends step2 {


  private lazy val string_indexers: Array[StringIndexer] = feature_stringtype.map(x =>
    new StringIndexer().setInputCol(x).setOutputCol(x + "_indexed") //.setHandleInvalid("skip")
      .setHandleInvalid("keep")
  )
  private lazy val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(features_predictors_to_assemble)
    .setOutputCol(predictors_assembled)
    .setHandleInvalid("keep")
  //keep

  lazy val rf: RandomForestRegressor = new RandomForestRegressor()
    .setFeaturesCol(predictors_assembled)
    .setLabelCol(label)
    .setNumTrees(2000)

  private val paramGrid = new ParamGridBuilder()
  paramGrid
    .addGrid(rf.maxDepth, Array(2, 3/*, 7, 9*/))
    .addGrid(rf.featureSubsetStrategy, Array("onethird", "sqrt", "log2"))
    .addGrid(rf.maxBins, Array(32, 41/*, 51, 61*/))

  private val monpipeline: Pipeline = new Pipeline().setStages(string_indexers ++ Array(assembler) ++ Array(rf))

  private val evaluator: RegressionEvaluator = new RegressionEvaluator()
    .setLabelCol(label)
    .setPredictionCol(prediction)
    .setMetricName(metric)

  val CrossValidator: CrossValidator = new CrossValidator()
    .setEstimator(monpipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid.build())
    .setNumFolds(3).setParallelism(2)


}
