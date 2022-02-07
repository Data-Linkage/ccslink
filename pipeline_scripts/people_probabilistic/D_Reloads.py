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

# File paths of all person decisions made in the CMS (could include matches made in the HH journey too)
paths = ['/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/16/16/residents_match/*.csv',         # All CRZ matches after resolving conflicts
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/16/17/residents_match/*.csv',         # Extra CRZ matches after resident name fix
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/08/27/30/householdsppl_match/ab/*.csv',  # Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/09/02/37/householdsppl_match/cd/*.csv',  # Households CD (1) person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/09/02/38/householdsppl_match/cd/*.csv',  # Households CD (2) person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/10/27/113/residents_match/*.csv',        # V16: All CRZ matches after resolving conflicts
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/02/123/householdsppl_match/ab/*.csv', # V16: Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/08/125/householdsppl_match/cd/*.csv', # V16: Households CD person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/17/134/residents_match/*.csv',        # V20: All CRZ matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/22/139/householdsppl_match/ab/*.csv', # V20: Households AB person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/11/23/140/householdsppl_match/cd/*.csv', # V20: Households CD person matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/12/08/155/residents_match/*.csv',        # V20: Presearch matches
         '/data/dap/c21_processing_zone/c21_cmatch_hdfs_h/file/cms_outputs/2021/12/09/159/householdsppl_match/cd/*.csv'] # V20: Households EF person matches

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
