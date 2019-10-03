package packfar

object testbefor extends App {

  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")
  //****************************************************************************************************************************************************************
  //import my data NUMERIC
  val datas_processed = merge_get_twice_num_cat()
  val train = datas_processed.head
  val test = datas_processed(1)
  val Id_train = datas_processed(2)
  val Id_test = datas_processed(3)

  //****************************************************************************************************************************************************************
  val train_test_regularized = regularisation_training_compared_to_testing(train, test, nb_classe = 900)
  //list of dataframes

}
  //  for (i<-List(500,600,700)){
  //  val list_datas_regularized = regularisation_training_compared_to_testing(train_features, test_features, nb_classe = i) //list of dataframes
  //
  //  //ici le dataframe d'entrainement est regularisé avec le kmean en fonction du test:datas_regularized.head
  //  val train = list_datas_regularized.head
  //
  //  //random forst model buld and tuning:
  //    train.cache()
  //    println(i+"---------: "+train.count())
  //    Buld_RF_modelspe(train = train)
  //    println("------------------------------")
  //
  ////  take_test_prediction_with_existing_model(test = test, df_ID_test = df_ID_test)
  //  }
