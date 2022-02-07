# ----------------------- #
# -------- SETUP -------- #
# ----------------------- #

# Import PySpark, Parameters, File Paths, Functions & Packages
import pyspark
from CCSLink import Parameters
from CCSLink.Parameters import FILE_PATH
from CCSLink import Person_Functions as PF
from CCSLink import Household_Functions as HF
from CCSLink import Cluster_Function as CF
exec(open("/home/cdsw/collaborative_method_matching/CCSLink/Packages.py").read()) 

# ------------------------- #
# --------- DATA ---------- #
# ------------------------- # 

# -- The aim of this script is to collect together all matching decisions (match or non-match) already made in the CMS across all loads -- #

# Empty dataset to append all decisions made in CMS
cms_decisions = sparkSession.createDataFrame([], StructType([StructField("id_ccs", StringType(), True),
                                                             StructField("id_cen", StringType(), True)]))

# File paths of all person decisions made during clerical matching (could include matches made in the HH journey too)
paths = []

# Run if paths list not empty
if len(paths) > 0:

  # Combine into single set of decisions
  for path in paths:
    
    # Read in decisions
    df = sparkSession.read.option("delimiter", ";").csv(path, header = True)\
                           .selectExpr('CCS_Resident_ID as id_ccs', 'census_Resident_ID as id_cen', 'match_status')\
                           .filter(col('match_status').isNull() == False).drop('match_status')
      
    # Combine 
    cms_decisions = cms_decisions.unionByName(df).dropDuplicates(['id_cen', 'id_ccs'])

# Save
cms_decisions.write.mode('overwrite').parquet(FILE_PATH('Stage_5_CMS_Decisions'))

sparkSession.stop()
