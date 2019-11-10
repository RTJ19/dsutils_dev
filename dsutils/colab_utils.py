from google.colab import drive
import os

def mount_drive(folder_name):
  # This will prompt for authorization.
  drive.mount('/content/drive')
  file_path = "/content/drive/My Drive/"+ folder_name +"/"
  return file_path

def get_spark_environment(extended=True):
  os.system('apt-get update')
  os.system('apt-get install openjdk-8-jdk-headless -qq > /dev/null')
  os.system('wget -q https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz')
  os.system('tar xf spark-2.2.0-bin-hadoop2.7.tgz')
  os.system('pip install -q findspark')
  os.system('pip install pyspark')
  if extended == True:
    os.system('pip install pyspark_dist_explore')
    os.system('pip install scikit-plot')
    
  # setting the environment
  #import os
  os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
  os.environ["SPARK_HOME"] = "/content/spark-2.2.0-bin-hadoop2.7"
  import findspark
  findspark.init()
