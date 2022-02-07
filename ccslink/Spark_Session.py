# Packages
import os
import shutil
from pyspark.sql import SparkSession


def SPARK():
    '''
    This function takes no arguments and sets default sparkSession parameters. These can be
    further configured in running scripts by specifying extra SparkSession.conf settings.

    Within the sparkSession initiated by this function, the package is zipped and sent to
    the executors, necessary when calling UDFs as the executors running these will not
    have the CCSLink package in their environment otherwise.
    
    Parameters
    ----------
    big_sparksession: boolean
      if empty or None a default-sized sparksession is created, else a highly-resourced session
      is created.
  
    Returns
    -------
    sparkSession

    '''
    # get graphframes jar path to configure session with (could be manually entered but this will update with newer vesions)
    graphframes_path = "/home/cdsw/.local/lib/python3.6/site-packages/graphframes-wrapper"
  
    for file in os.listdir(graphframes_path):
      if file.endswith(".jar"):
          # Get the latest jar file
          jar_path = os.path.join(graphframes_path, file)
            
    # Create Spark session
    sparkSession = SparkSession\
                    .builder\
                    .config('spark.executor.memory', '50g')\
                    .config('spark.yarn.executor.memoryOverhead', '15g')\
                    .config('spark.executor.cores', 5)\
                    .config('spark.ui.showConsoleProgress', 'false')\
                    .config('spark.python.worker.memory', '5g')\
                    .config("spark.jars", jar_path)\
                    .config('spark.dynamicAllocation.maxExecutors', 4)\
                    .config('spark.dynamicAllocation.enabled', 'true')\
                    .config('spark.shuffle.service.enabled', 'true')\
                    .config('spark.sql.crossJoin.enabled', 'true')\
                    .config("spark.sql.repl.eagerEval.enabled", 'true')\
                    .appName("CCSlink_" + str(os.environ["HADOOP_USER_NAME"]))\
                    .enableHiveSupport()\
                    .getOrCreate()

                    
    return sparkSession