import matplotlib.pyplot       as plt
import pyspark.sql.functions   as F

from pyspark.sql               import Window
from pyspark_dist_explore      import hist
from tqdm                      import tqdm_notebook as tqdm

def get_eda_plots(df,hspace=0.5, wspace=0.5,numerical_figsize=(15,15),\
            categorical_figsize=(15,25),bins=25):
  """
  
  The function takes in a pyspark dataframe and gives subplots of numerical 
  labels and categorical labels. 
  For numerical labels it will give histogram of the numerical values for 
  each label.
  For categorical labels it will give percentages of each of the category in 
  each for each label
  """

  
  numerical_labels = [item[0] for item in df.dtypes if not item[1].startswith('string')]
  # print (numerical_labels)

  if (len(numerical_labels) % 2) == 0:
    numerical_labels2=numerical_labels

  else:
    numerical_labels2=numerical_labels
    numerical_labels2.append(numerical_labels[-1])
    
    print("Numerical columns has Odd number of features\n hence last subplot will be repeated")

  fig = plt.figure(figsize=numerical_figsize)
  fig.subplots_adjust(hspace=hspace, wspace=wspace)
  print ("Plotting numerical columns...")
  for column,i in tqdm(zip(numerical_labels2,range(1, len(numerical_labels2)+1)),total = len(numerical_labels2)):
    
    ax = fig.add_subplot(round((len(numerical_labels2)/2)+0.5), 2, i)
    hist(ax, x=df.select(column), bins=bins)
    ax.set_title(column)
    ax.legend()
  
  categorical_labels = [item[0] for item in df.dtypes if item[1].startswith('string')]
  # print (categorical_labels)
  
  if (len(categorical_labels) % 2) == 0:
    categorical_labels2=categorical_labels
  else:
    categorical_labels2=categorical_labels
    categorical_labels2.append(categorical_labels[-1])
    
    print("Categorical labels has Odd number of features\n hence last subplot will be repeated")

  fig = plt.figure(figsize=(categorical_figsize))
  fig.subplots_adjust(hspace=hspace, wspace=wspace)
  # plt.xticks(rotation=45)

  print ("Plotting categorical columns...")
  for column,i in tqdm(zip(categorical_labels2,range(1, len(categorical_labels2)+1))total = len(categorical_labels2)):
    
    window = Window.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
    tab = df.select([column]).\
      groupBy(column).\
      agg(F.count(column).alias('num'),
          ).\
      withColumn('total',F.sum(F.col('num')).over(window)).\
      withColumn('percent',F.col('num')*100/F.col('total')).\
      drop(F.col('total'))

    categories = [(row[column]) for row in tab.collect()]
    category_percentage = [(row.percent) for row in tab.collect()]

    ax = fig.add_subplot(round((len(categorical_labels2)/2)+0.5), 2, i)
    ax.bar(categories, category_percentage, label="percentage")
    plt.xticks(rotation=45)
    ax.set_title(column)
    ax.legend()
