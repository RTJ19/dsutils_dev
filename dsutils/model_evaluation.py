from pyspark.ml.evaluation        import (BinaryClassificationEvaluator,
                                       MulticlassClassificationEvaluator,
                                       RegressionEvaluator)
from dsutils_dev.dsutils.convert  import DataFrameConverter
from tqdm                         import tqdm_notebook as tqdm
from math                         import sqrt

class NormalizedRMSE ():
  """
  NMRSE = RMSE/max()-min()
  
  When to use RMSE and NRMSE?
  Use RMS if you are comparing different models on the same data. 
  Use NRMS or if comparing different data that exists on different scales.

  source:https://stats.stackexchange.com/questions/349354/normalized-root-mean-square-nrms-vs-root-mean-square-rms
  """
  def __init__(self,predictionCol="prediction", labelCol="label"):

    self.predictionCol = predictionCol
    self.labelCol = labelCol
    self.rmse = RegressionEvaluator(predictionCol=self.predictionCol, labelCol=self.labelCol,metricName="rmse")

  def evaluate(self,dataset):
    """
    :param dataset: a dataset that contains labels/observations and
               predictions
    :return: metric
    """
    max_value = dataset.agg({self.predictionCol: "max"}).collect()[0][0]
    min_value = dataset.agg({self.predictionCol: "min"}).collect()[0][0]

    print(self.rmse.evaluate(dataset))

    return (sqrt(self.rmse.evaluate(dataset)))/(max_value-min_value)


class EvaluationClass(object):
  def __init__(self,prediction_type = 'classification',prediction_col = 'prediction',label_col='label'):
    self.prediction_type = prediction_type
    
    if self.prediction_type == "classification":
      self.accuracy = MulticlassClassificationEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="accuracy")
      self.f1 = MulticlassClassificationEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="f1")
      self.precision = MulticlassClassificationEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="weightedPrecision")
      self.recall = MulticlassClassificationEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="weightedRecall")
      self.roc = BinaryClassificationEvaluator(rawPredictionCol=prediction_col,labelCol=label_col,metricName="areaUnderROC")
      self.precision_recall = BinaryClassificationEvaluator(rawPredictionCol=prediction_col,labelCol=label_col,metricName="areaUnderPR")  
      self.default_sorting_column = "accuracy"
    
    elif self.prediction_type == "regression":
      self.rmse = RegressionEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="rmse")
      self.mse = RegressionEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="mse")
      self.r2 = RegressionEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="r2")
      self.mae = RegressionEvaluator(predictionCol=prediction_col, labelCol=label_col,metricName="mae")
      self.nrmse = NormalizedRMSE(predictionCol=prediction_col, labelCol=label_col)
      self.default_sorting_column = "rmse"

    else:
      raise ValueError("Enter prediction_type as classification or regression")

  def get_predictions(self,df,model,split_ratio,seed,\
                             train_data ,test_data ):
    """
    If dataFrame is given, then it splits the dataframe into train and test
    Else it takes in train and test data

    Returns predictions based on the test data and model provided

    """
    if df != False:
      train_data, test_data = df.randomSplit(split_ratio,seed=seed)

    model_fit = model.fit(train_data)
    return model_fit.transform(test_data) 

  def model_evaluation(self,model_list,df,train_data = None,test_data = None,\
                       split_ratio=[0.7,0.3],seed = 42, as_pandas=False,\
                       sorting_column =  None):
    """
    Gives a pyspark dataframe of evaluation metrics for each model in the 
    model list for the given dataset
    """
    if self.prediction_type == "classification":
      evaluation_df = pd.DataFrame(columns=("model","accuracy","f1score",\
                                            "precision","recall",\
                                            "areaUnderROC","areaUnderPR"))
      
      for counter,(name, model) in tqdm(enumerate(model_list),total=len(model_list)):
          print ("Modeling with %s"%name)
          # prediction
          results = self.get_predictions(df,model,split_ratio,seed,\
                                        train_data = train_data,test_data = test_data)
        
          evaluation_df.loc[counter,"model"]= name
          evaluation_df.loc[counter,"accuracy"] = self.accuracy.evaluate(results)*100
          evaluation_df.loc[counter,"f1score"]= self.f1.evaluate(results)
          evaluation_df.loc[counter,"precision"]= self.precision.evaluate(results)
          evaluation_df.loc[counter,"recall"]= self.recall.evaluate(results)
          evaluation_df.loc[counter,"areaUnderROC"] = self.roc.evaluate(results)
          evaluation_df.loc[counter,"areaUnderPR"] = self.roc.evaluate(results) 

      if sorting_column ==  None:
        sorting_column = self.default_sorting_column
        
      evaluation_df = evaluation_df.sort_values(by= sorting_column, ascending=False)
    
    elif self.prediction_type == "regression":
      evaluation_df = pd.DataFrame(columns=("model","rmse","mse",\
                                            "r2","mae"))
    
      for counter,(name, model) in tqdm(enumerate(model_list),total=len(model_list)):
        print ("Modeling with %s"%name)
        # prediction
        results = self.get_predictions(df,model,split_ratio,seed,\
                                      train_data = train_data,test_data = test_data)
        
        evaluation_df.loc[counter,"model"]= name
        evaluation_df.loc[counter,"rmse"]= self.rmse.evaluate(results)
        evaluation_df.loc[counter,"mse"]= self.mse.evaluate(results)
        evaluation_df.loc[counter,"mae"]= self.mae.evaluate(results)
        evaluation_df.loc[counter,"r2"]= self.r2.evaluate(results)
        evaluation_df.loc[counter,"Normalized RMSE"]= self.nrmse.evaluate(results)
        
      if sorting_column ==  None:
        sorting_column = self.default_sorting_column
        
      evaluation_df = evaluation_df.sort_values(by= sorting_column, ascending=True)

    if as_pandas == False:
      evaluation_spark_df = DataFrameConverter().get_spark_df(evaluation_df )
      return evaluation_spark_df
    
    else:
      return evaluation_df
    
