package packfar

object testbefor extends App {

  val beginspark: Begining_spark_Local_Import = new Begining_spark_Local_Import()
  val spark = beginspark.get_local_spark_session()
  spark.sparkContext.setLogLevel("WARN")

  //****************************************************************************************************************************************************************
  //import my data NUMERIC
  val lisdatanum = merge_get_twice_num_cat()
  val train_features = lisdatanum.head
  val test_features = lisdatanum(1)
  val df_ID_train = lisdatanum(2)
  val df_ID_test = lisdatanum(3)
  println(train_features.count(),test_features.count())
  train_features.show(5,false)
  test_features.show(5,false)
  //****************************************************************************************************************************************************************
//  val list_datas_regularized = regularisation_training_compared_to_testing(train_features, test_features, nb_classe = 600) //list of dataframes
//  val df_num_train = list_datas_regularized.head
//  val df_num_test = list_datas_regularized(1)
//  Buld_RF_model(train=df_num_train)
//  take_test_prediction_with_existing_model(df_num_test,df_ID_test=df_ID_test)

  spark.stop()
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
