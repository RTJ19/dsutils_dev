from google.colab import drive

def mount_drive(folder_name):
  # This will prompt for authorization.
  drive.mount('/content/drive')
  file_path = "/content/drive/My Drive/"+ folder_name +"/"
  return file_path

def get_spark_environment(extended=True):
  !apt-get update
  !apt-get install openjdk-8-jdk-headless -qq > /dev/null
  !wget -q https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz
  !tar xf spark-2.2.0-bin-hadoop2.7.tgz
  !pip install -q findspark
  !pip install pyspark
  if extended == True:
    !pip install pyspark_dist_explore
    !pip install scikit-plot
    
  # setting the environment
  import os
  os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
  os.environ["SPARK_HOME"] = "/content/spark-2.2.0-bin-hadoop2.7"
  import findspark
  findspark.init()
