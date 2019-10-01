package packfar

object Numeric_Main_mod extends App {

  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")

  //****************************************************************************************************************************************************************
  //import my data NUMERIC
  val lisdatanum = get_final_trainTest_Num()
  val train_features = lisdatanum.head
  val test_features = lisdatanum(1)
  val df_ID_train = lisdatanum(2)
  val df_ID_test = lisdatanum(3)
  //****************************************************************************************************************************************************************
  for (i<-List(10,600,800)){
  val list_datas_regularized = regularisation_training_compared_to_testing(train_features, test_features, nb_classe = i) //list of dataframes

  //ici le dataframe d'entrainement est regularisé avec le kmean en fonction du test:datas_regularized.head
  val train = list_datas_regularized.head

  //random forst model buld and tuning:
    train.cache()
    println(i+"---------: "+train.count())
    Buld_RF_modelspe(train = train)
    println("------------------------------")

//  take_test_prediction_with_existing_model(test = test, df_ID_test = df_ID_test)
  }
  spark.stop()
}
