import java.io._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Imputer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

package object packfar {
  val work_path = "/home/farid/Bureau/kaggHousPrice"
  val data_path = "/home/farid/Bureau/kaggHousPrice/Data"
  val spatial_colums_type=List("LotFrontage", "MasVnrArea","GarageYrBlt","YearBuilt", "YearRemodAdd", "YrSold")

  implicit class DataFrameEXtend(df: DataFrame) {
    def select_cols_By_Type(column_Type: DataType): DataFrame = {
      val cols = df.schema.toList
        .filter(x => x.dataType == column_Type)
        .map(c => col(c.name))
      df.select(cols: _*)
    }

    def select_cols_by_names(Colones: List[String]): DataFrame = {
      df.select(Colones.map(c => col(c)): _*)
    }

    implicit def select_cols_by_index(implicit ListofIndex: List[Int]): DataFrame = {
      df.select(ListofIndex map df.columns map col: _*)
    }

    def join_df2_by_key(df1: DataFrame, key_join: Seq[String], type_of_join: String = "inner"): DataFrame = {
      val possible_joins = Set("inner", "cross", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")
      // TODO: make sure that key_join exist sur in df and df1
      if (!possible_joins.contains(type_of_join)) {
        println("type_of_join parameter must be in : [" + possible_joins.reduce(_ + ", " + _) + "]")
        null
      }
      else {
        df.join(df1, key_join, type_of_join)
      }
    }

    def join_df2_by_index(df0: DataFrame): DataFrame = {
      val df1 = df.withColumn("Id2", monotonically_increasing_id)
      val df2 = df0.withColumn("Id2", monotonically_increasing_id)
      df1.join(df2, "Id2").drop("Id2")
    }

    def get_Columns_as_ScalaList(): Unit = {
      println(df.columns.toList.map(x => '"' + x + '"'))
    }

    def describe_type_var(Type_Data_Column: DataType = DoubleType): Unit = {
      if (Type_Data_Column == StringType) {
        val df1: DataFrame = df.select_cols_By_Type(Type_Data_Column)
        val categorical_var = df1.columns
        for (i <- categorical_var) {
          println("********Variable Catégorielles************")
          df.groupBy(i).count().show(truncate = false)
          println("------------------------------------")
        }

      }
      else {
        println("********Variable Numérique************")
        //      val df2: DataFrame = df.select_cols_By_Type(IntegerType)
        val numerical_var = df.columns
        for (i <- numerical_var) {
          df.select(i).summary().show(truncate = false)
          println("------------------------------------")
        }
      }
    }

    def select_categorical_var_and_impute_na(a: String = "NA", b: String = "NoValue"): DataFrame = {
      val df_imputed = df.select_cols_By_Type(StringType).na.replace(df.columns, Map(a -> b))
      df_imputed
    }

    def select_Impute_And_Transform_categorical_to_numerical(): DataFrame = {
      val df1 = df.select_categorical_var_and_impute_na()
      val feat = df1.columns
      val inds = feat.map { colName =>
        new StringIndexer()
          .setInputCol(colName)
          .setOutputCol(colName + "catI")
          .fit(df1)
      }
      val pipeline = new Pipeline().setStages(inds).fit(df1).transform(df1)
      pipeline.select_cols_by_names(pipeline.columns.filter(x => x.endsWith("catI")).toList).toDF(feat: _*)
    }

    def conveting_multiple_columns_to_specific_DataType(convert_to: DataType): DataFrame = {
      var df_var = df
      val df_col_names = df.columns
      df_col_names.foreach(x => df_var = df_var.withColumn(x + "Float", col(x).cast(convert_to)))
      val df_returned = df_var.select_cols_by_names(df_var.columns.filter(x => x.endsWith("Float")).toList).toDF(df_col_names: _*)
      df_returned
    }

    def cast_all_columns_to_numeric(convert_to: DataType, my_strategy: String = "mean"): DataFrame = {
      var df1 = df
      df1.columns.foreach(x => df1 = df1.withColumn(x + "NewColumn", col(x).cast(convert_to)))
      val df2 = df1.select(df1.columns.filter(x => x.endsWith("NewColumn")).map(x => col(x)): _*).toDF(df.columns: _*)

      val my_impute = new Imputer()
        .setStrategy(my_strategy)
        .setInputCols(df2.columns)
        .setOutputCols(df2.columns.map(x => x + "imputed"))

      val df3 = my_impute.fit(df2).transform(df2)
      val df4 = df3.select(df3.columns.filter(x => x.endsWith("imputed")).map(x => col(x)): _*).toDF(df.columns: _*)
      df4
    }

    def convert_df_integers_type_to_dates(): DataFrame = {
      var df0 = df
      val col_names = df.columns.toList
      col_names.foreach(x => df0 = df0.withColumn(x + "_dateformat", date_add(to_date(trim(col(x).cast("String")), "YYYY"), 5)))
      df0 = df0.select_cols_by_names(df0.columns.filter(x => x.endsWith("_dateformat")).toList).toDF(col_names: _*)
      df0
      // TODO: add new features over date types columnes after
      //      val df55= df_date_var.convert_dateframe_integers_type_to_dates()
      //        .withColumn("a",year(col("YearRemodAdd"))-year(col("YearBuilt")))
      //      df55.show()
    }

  }

  def regularisation_training_compared_to_testing(df_train: DataFrame, df_test: DataFrame, nb_classe: Int = 300, nb_itter: Int = 20): List[DataFrame] = {
    val to_predict_var = df_train.columns.toSet.diff(df_test.columns.toSet).head
    val assembler = new VectorAssembler().setInputCols(df_test.columns).setOutputCol("features")
    val df_train_assembled = assembler.transform(df_train).select("features", to_predict_var)
    val df_test_assembled = assembler.transform(df_test).select("features")

    val kmean_model = new KMeans().setK(nb_classe).setMaxIter(nb_itter).setSeed(123).fit(df_train_assembled)

    val set_class_test = kmean_model.transform(df_test_assembled).select("prediction").rdd.map(x => x(0)).collect().toList
      .distinct.toString.replace("List", "")

    val df_train__assembled_and_regularised = kmean_model.transform(df_train_assembled)
      .where("prediction in" + set_class_test + "")
      .select("features", to_predict_var).withColumnRenamed(to_predict_var, "label")
    List(df_train__assembled_and_regularised, df_test_assembled)
  }

  def get_final_trainTest_Num(): List[DataFrame] = {
    val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
    val spark = beginspark.get_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    val df_brut_test: DataFrame = beginspark.importDF(data_path + "/test.csv").drop(spatial_colums_type:_*) //to get data consistency between train and test
    val df_brut_train: DataFrame = beginspark.importDF(data_path + "/train.csv").drop(spatial_colums_type:_*)

    val df_ID_train = df_brut_train.select("Id")
    val df_ID_test = df_brut_test.select("Id")
    val df_cible = df_brut_train.select("SalePrice").cast_all_columns_to_numeric(FloatType)

    //  extracrion des type de variable numeric
    val test_features = df_brut_train.select_cols_By_Type(IntegerType).
      drop("Id","SalePrice").cast_all_columns_to_numeric(FloatType)

    val train_features = df_brut_test.select_cols_by_names(test_features.columns.toList).cast_all_columns_to_numeric(FloatType).
      join_df2_by_index(df_cible)
    val DATA_train_test_IDtrain_ID_test_CIBLE = List(train_features, test_features, df_ID_train, df_ID_test)
    DATA_train_test_IDtrain_ID_test_CIBLE
  }

  def delete_Directory(file: File) {
    if (file.isDirectory & file.exists)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete_Directory)
    file.delete
  }

  def take_test_prediction_with_existing_model(test: DataFrame, df_ID_test: DataFrame, where_is_Your_model: String = work_path, model_name: String = "RF_model") {
    val myRFmodel = CrossValidatorModel.load(where_is_Your_model + "/" + model_name)

    delete_Directory(new java.io.File(where_is_Your_model + "/" + "predictions_" + model_name))
    df_ID_test.join_df2_by_index(myRFmodel.transform(test)
      .select("prediction")).withColumnRenamed("prediction", "SalePrice")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(where_is_Your_model + "/" + "predictions_" + model_name)
  }

  def Buld_RF_model(RF_model_name: String = "RF_model", train: DataFrame, numTrees: Array[Int] = Array(5, 100) //Array(50,100,500,1000,3000)
                    , MaxBins: Array[Int] = Array(2, 3) //Array(2,3,8,12,15)
                    , maxDepth: Array[Int] = Array(1, 3) //Array(1,3,5,10,12,15,20,30)
                    , numFolds: Int = 10) {
    //supression du modél s'il exist
    delete_Directory(new java.io.File(work_path + "/" + RF_model_name))
    val model = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
    //---------------------------------------------------------------
    val paramGrid = new ParamGridBuilder()
      .addGrid(model.numTrees, numTrees)
      .addGrid(model.maxBins, MaxBins)
      .addGrid(model.maxDepth, maxDepth)
      .build()
    val cv = new CrossValidator()
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)
      .setEstimator(model)
    //---------------------------------------------------------------
    //  build the workflow
    val cvModel = cv.fit(train)
    //---------------------------------------------------------------
    //evaluation
    Array("mse", "rmse", "r2", "mae").foreach(x=>println(x+" :"+new RegressionEvaluator().setMetricName(x).evaluate(cvModel.transform(train))))
    //---------------------------------------------------------------
    //  save the workflow
    cvModel.write.overwrite().save(work_path + "/" + RF_model_name)
  }

  def Buld_RF_modelspe( train: DataFrame, numFolds: Int = 10) {

    val model = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label").setSeed(123).setNumTrees(1000)
    //---------------------------------------------------------------
    //  build the workflow
    val cvModel = model.fit(train)
    //---------------------------------------------------------------
    //evaluation
    Array("mse", "rmse", "r2", "mae").foreach(x=>println(x+" :"+new RegressionEvaluator().setMetricName(x).evaluate(cvModel.transform(train))))
    println("NumTrees:   "+cvModel.getNumTrees)
  }

}