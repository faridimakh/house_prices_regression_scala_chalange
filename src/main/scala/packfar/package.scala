import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{Imputer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

package object packfar {

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

    def get_Colone_as_ScalaList(): Unit = {
      println(df.columns.toList.map(x => '"' + x + '"'))
    }

    def describe_type_var(typedata_colone: DataType = DoubleType): Unit = {
      if (typedata_colone == StringType) {
        val df1: DataFrame = df.select_cols_By_Type(typedata_colone)
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
        val numericaal_var = df.columns
        for (i <- numericaal_var) {
          df.select(i).summary().show(truncate = false)
          println("------------------------------------")
        }
      }
    }

    def select_categorical_var_and_impute_na(): DataFrame = {
      val df_imputed = df.select_cols_By_Type(StringType).na.replace(df.columns, Map("NA" -> "novalue"))
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
      pipeline
        .select_cols_by_names(pipeline.columns.filter(x => x.endsWith("catI"))
          .toList).toDF(feat: _*)
    }

    def conveting_multiple_columns_to_specific_DataType(convert_to: DataType): DataFrame = {
      var df_var = df
      val df_col_names = df.columns
      df_col_names.foreach(x => df_var = df_var.withColumn(x + "Float", col(x).cast(convert_to)))
      val df_returned = df_var.select_cols_by_names(df_var.columns.filter(x => x.endsWith("Float")).toList).toDF(df_col_names: _*)
      df_returned
    }

    def impute_numeric_vars_with_mean_or_median(strategy_imputation_mean_or_median: String = "mean"): DataFrame = {
      if (!Set("mean", "median").contains(strategy_imputation_mean_or_median)) {
        println("le parametre d'imputation doit apartenir à {mean,median}!")
        null
      } else {
        val df_col_names = df.columns
        val df_intermediary = new Imputer().setInputCols(df_col_names)
          .setOutputCols(df_col_names.map(x => x + "IMPUTED")).setStrategy(strategy_imputation_mean_or_median).fit(df).transform(df)
        val df_returned = df_intermediary.select_cols_by_names(df_intermediary.columns.filter(x => x.endsWith("IMPUTED")).toList).toDF(df_col_names: _*)
        df_returned
      }
    }

    def convert_dateframe_integers_type_to_dates(): DataFrame = {
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
    def regularisation_training_compared_to_testing(df_train: DataFrame, df_test: DataFrame, nb_classe: Int = 100, nb_itter: Int = 20): List[DataFrame] = {
      val to_predict_var = df_train.columns.toSet.diff(df_test.columns.toSet).head
      val assembler = new VectorAssembler().setInputCols(df_test.columns).setOutputCol("features")
      val df_train_assembled = assembler.transform(df_train).select("features", to_predict_var)
      val df_test_assembled = assembler.transform(df_test).select("features")

      val kmean_model = new KMeans().setK(nb_classe).setMaxIter(nb_itter).fit(df_train_assembled)

      val set_class_test = kmean_model.transform(df_test_assembled).select("prediction").rdd.map(x => x(0)).collect().toList
        .distinct.toString.replace("List", "")

      val df_train__assembled_and_regularised = kmean_model.transform(df_train_assembled)
        .where("prediction in" + set_class_test + "")
        .select("features", to_predict_var)
      List(df_train__assembled_and_regularised, df_test_assembled)
    }


  }
  def regularisation_training_compared_to_testing(df_train: DataFrame, df_test: DataFrame, nb_classe: Int = 20, nb_itter: Int = 20): List[DataFrame] = {
    val to_predict_var = df_train.columns.toSet.diff(df_test.columns.toSet).head
    val assembler = new VectorAssembler().setInputCols(df_test.columns).setOutputCol("features")
    val df_train_assembled = assembler.transform(df_train).select("features", to_predict_var)
    val df_test_assembled = assembler.transform(df_test).select("features")

    val kmean_model = new KMeans().setK(nb_classe).setMaxIter(nb_itter).setSeed(123).fit(df_train_assembled)

    val set_class_test = kmean_model.transform(df_test_assembled).select("prediction").rdd.map(x => x(0)).collect().toList
      .distinct.toString.replace("List", "")

    val df_train__assembled_and_regularised = kmean_model.transform(df_train_assembled)
      .where("prediction in" + set_class_test + "")
      .select("features", to_predict_var).withColumnRenamed(to_predict_var,"label")
    List(df_train__assembled_and_regularised, df_test_assembled,df_train_assembled.withColumnRenamed(to_predict_var,"label"))
  }


  def regularisation_training_compared_to_testing_bis(df: DataFrame, to_predict_var:String,nb_classe: Int = 20, nb_itter: Int = 20): List[DataFrame] = {
    val df_train=df.randomSplit(Array(0.7,0.3))(0)
    val df_test=df.randomSplit(Array(0.7,0.3))(1).drop(to_predict_var)
    val df_testBIS=df.randomSplit(Array(0.7,0.3))(1)

    val assembler = new VectorAssembler().setInputCols(df_test.columns).setOutputCol("features")

    val df_train_assembled = assembler.transform(df_train).select("features", to_predict_var)
    val df_test_assembled = assembler.transform(df_test).select("features")
    val df_test_assembledBIS = assembler.transform(df_testBIS).select("features",to_predict_var)

    val kmean_model = new KMeans().setK(nb_classe).setMaxIter(nb_itter).setSeed(123).fit(df_train_assembled)

    val set_class_test = kmean_model.transform(df_test_assembled).select("prediction").rdd.map(x => x(0)).collect().toList
      .distinct.toString.replace("List", "")

    val df_train__assembled_and_regularised = kmean_model.transform(df_train_assembled)
      .where("prediction in" + set_class_test + "")
      .select("features", to_predict_var).withColumnRenamed(to_predict_var,"label")
    List(df_train__assembled_and_regularised, df_test_assembled,df_test_assembledBIS.withColumnRenamed(to_predict_var,"label"))
  }


}