from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from CCSLink import Spark_Session as SS
from graphframes import *
import os

def cluster_number(df, id_1, id_2):
  """ 
  Take dataframe of matches with two id columns,
  create vertex dataframe (ids) with col name id,
  create edges dataframe from input df with col names 
  src and dst required to create graph. Split graph
  into connected components to create clusters.
  
  Parameters
  ----------
  df: PySpark DataFrame
      DataFrame to add new column 'Cluster_Number' to.
  id_1: string
      ID number column of first DataFrame.
  id_2: string
      ID number column of second DataFrame.

  Raises
  ------
  TypeError
      if variables 'id_1' or 'id_2' are not strings.
        
  """
  # Check variable types
  if not ((isinstance(df.schema[id_1].dataType, StringType)) and (isinstance(df.schema[id_2].dataType, StringType))):
      raise TypeError('ID variables must be strings')
  
  # Set up spark settings
  username = os.getenv("HADOOP_USER_NAME")
  checkpoint_path = f"/user/{username}/checkpoints"
  SS.SPARK().sparkContext.setCheckpointDir(checkpoint_path)
  
  # Stack all unique IDs from both datasets into one column called 'id'
  ids = df.select(id_1).union(df.select(id_2))
  ids = ids.select(id_1).distinct().withColumnRenamed(id_1, 'id')

  # Rename matched data columns ready for clustering
  matches = df.select(id_1, id_2)
  matches = matches.withColumnRenamed(id_1, 'src')
  matches = matches.withColumnRenamed(id_2, 'dst')

  # Create graph & get connected components / clusters
  graph = GraphFrame(ids, matches)
  cluster = graph.connectedComponents()
  
  # Update cluster numbers (1,2,3,4,... instead of 1,10,100,1000,...)
  lookup = cluster.select('component').dropDuplicates(['component']).withColumn('Cluster_Number',F.rank().over(Window.orderBy("component"))).sort('component')
  cluster = cluster.join(lookup, on = 'component', how = 'left').withColumnRenamed('id', id_1)
  
  # Join new cluster number onto matched pairs
  df = df.join(cluster, on = id_1, how = 'left').sort('Cluster_Number').drop('component')
  
  return df
